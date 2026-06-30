import {
  DEFAULT_RETRY,
  RetryOptions,
  withRetry,
} from './retry';
import {
  InMemoryScannerStore,
  IScannerStore,
  SwapState,
} from './store';
import {
  Block,
  FundsLockedEvent,
  IBlockchainNode,
  INotifier,
  LoanNotification,
  LoanRepaidEvent,
  LoanRequestedEvent,
  ProtocolEvent,
  RawLog,
  SwapNotification,
  SwapRequestedEvent,
  TerminalSwapEvent,
} from './types';

// Contract address the scanner listens to
const VAULT_SWAP_CONTRACT = '0xVaultSwap';

/**
 * Raised when a reorg is detected (block `n`'s `parentHash` does not match the
 * hash we stored for `n-1`) AND it cannot be safely auto-recovered. The scanner
 * first tries to rewind to the common ancestor and re-scan; it only throws this
 * when recovery is unsafe:
 *   - the common ancestor lies outside the retained recovery window (the reorg is
 *     deeper than `maxReorgDepth`), or
 *   - a notification was already emitted for a block past the ancestor, so a
 *     rewind would orphan an emission we cannot un-notify.
 * In both cases the scanner halts without advancing, leaving state consistent for
 * operator intervention rather than guessing. See NOTES.md (reorg levels).
 */
export class ReorgDetectedError extends Error {
  constructor(
    readonly blockNumber: number,
    readonly expectedParentHash: string,
    readonly actualParentHash: string,
  ) {
    super(
      `Reorg detected at block ${blockNumber}: parentHash "${actualParentHash}" does ` +
        `not build on the processed predecessor "${expectedParentHash}". Halting with ` +
        `the checkpoint at ${blockNumber - 1}; nothing emitted from the orphaned chain.`,
    );
    this.name = 'ReorgDetectedError';
  }
}

// ── Logger ──────────────────────────────────────────────────────────────────
// Logger seam so tests stay quiet and a real logger can be injected. Defaults
// to console.

export interface ILogger {
  info(message: string): void;
  warn(message: string): void;
  error(message: string): void;
}

const consoleLogger: ILogger = {
  info: (m) => console.log(m),
  warn: (m) => console.warn(m),
  error: (m) => console.error(m),
};

// ── Scanner options ───────────────────────────────────────────────────────────

export interface ScannerOptions {
  /**
   * Durable state. Pass the SAME instance to a second scanner to simulate a
   * restart. Defaults to a fresh in-memory store.
   */
  store?: IScannerStore;

  /**
   * Confirmation depth: only process blocks at least this many below the tip
   * (safeTip = latest - confirmations). Recent blocks are the ones that get
   * re-orged, so waiting for them to sink deeper avoids acting on events that
   * may be rolled back.
   *
   * Default 0 treats the tip as final, which keeps tests simple. Production sets
   * a chain-appropriate value (~12 on Ethereum, or the chain's finalized
   * checkpoint where available). NOTES.md covers why this alone is not a
   * complete reorg strategy.
   */
  confirmations?: number;

  /** Retry policy for node RPC calls. */
  retry?: RetryOptions;

  /**
   * Deliver queued notifications at the end of start() (convenience for a
   * single-process run). Default true. Set false to run delivery as a fully
   * separate loop: start() only scans + enqueues, and the caller drives
   * drainOutbox() on its own cadence.
   */
  autoDrain?: boolean;

  /**
   * Failed delivery attempts after which an outbox entry is moved to the
   * dead-letter queue, so one undeliverable notification cannot block the rest of
   * the queue forever. Default 5.
   */
  maxDeliveryAttempts?: number;

  /**
   * How many blocks deep a reorg can be and still be auto-recovered. The default
   * in-memory store keeps this many recent block hashes; a reorg whose common
   * ancestor falls outside the window can't be located, so the scanner halts
   * rather than guess. Only used when the scanner builds its own store. Default 100.
   */
  maxReorgDepth?: number;

  logger?: ILogger;
}

// ── Event parsing ─────────────────────────────────────────────────────────────

function parseLog(log: RawLog, blockNumber: number, logger: ILogger): ProtocolEvent | null {
  if (log.address !== VAULT_SWAP_CONTRACT) return null;

  const txHash = String(log.args['txHash'] ?? '');

  switch (log.event) {
    case 'SwapRequested':
      return {
        type: 'SwapRequested',
        swapId: String(log.args['swapId']),
        user: String(log.args['user']),
        tokenIn: String(log.args['tokenIn']),
        tokenOut: String(log.args['tokenOut']),
        amountIn: String(log.args['amountIn']),
        blockNumber,
        txHash,
      };
    case 'FundsLocked':
      return {
        type: 'FundsLocked',
        swapId: String(log.args['swapId']),
        vault: String(log.args['vault']),
        amountIn: String(log.args['amountIn']),
        blockNumber,
        txHash,
      };
    case 'SwapSettled': {
      const swapId = String(log.args['swapId']);
      const status = log.args['status'];
      // Do NOT coerce an unrecognised status to 'filled'. A bogus status that
      // silently became a *successful fill* is the dangerous direction in a
      // financial system: better to drop the malformed settle (the swap stays
      // pending and the warning is visible) than to emit a false settlement.
      if (status !== 'filled' && status !== 'expired') {
        logger.warn(
          `SwapSettled for ${swapId} at block ${blockNumber} has unrecognised ` +
            `status "${String(status)}"; skipping this log.`,
        );
        return null;
      }
      return {
        type: 'SwapSettled',
        swapId,
        amountOut: String(log.args['amountOut']),
        status,
        blockNumber,
        txHash,
      };
    }
    case 'FundsLockFailed':
      return {
        type: 'FundsLockFailed',
        swapId: String(log.args['swapId']),
        vault: String(log.args['vault'] ?? ''),
        reason: String(log.args['reason'] ?? ''),
        blockNumber,
        txHash,
      };
    case 'SwapCancelled': {
      const swapId = String(log.args['swapId']);
      const rawBy = log.args['by'];
      // Attribution-only field: a cancel with an unrecognised `by` is still a
      // real cancellation, so we keep it but record 'unknown' rather than
      // mis-attributing it to the user.
      let by: 'user' | 'protocol' | 'unknown';
      if (rawBy === 'user' || rawBy === 'protocol') {
        by = rawBy;
      } else {
        logger.warn(
          `SwapCancelled for ${swapId} at block ${blockNumber} has unrecognised ` +
            `'by' value "${String(rawBy)}"; recording as 'unknown'.`,
        );
        by = 'unknown';
      }
      return {
        type: 'SwapCancelled',
        swapId,
        by,
        reason: String(log.args['reason'] ?? ''),
        blockNumber,
        txHash,
      };
    }
    case 'LoanRequested': {
      const loanId = String(log.args['loanId']);
      const dueBlock = Number(log.args['dueBlock']);
      // dueBlock drives the default deadline; a non-numeric value would silently
      // mis-time every default for this loan, so reject the malformed log.
      if (!Number.isInteger(dueBlock)) {
        logger.warn(
          `LoanRequested for ${loanId} at block ${blockNumber} has a non-integer ` +
            `dueBlock "${String(log.args['dueBlock'])}"; skipping this log.`,
        );
        return null;
      }
      return {
        type: 'LoanRequested',
        loanId,
        borrower: String(log.args['borrower']),
        amount: String(log.args['amount']),
        dueBlock,
        blockNumber,
        txHash,
      };
    }
    case 'LoanRepaid':
      return {
        type: 'LoanRepaid',
        loanId: String(log.args['loanId']),
        borrower: String(log.args['borrower']),
        amountRepaid: String(log.args['amountRepaid']),
        blockNumber,
        txHash,
      };
    default:
      return null;
  }
}

// ── Scanner ───────────────────────────────────────────────────────────────────

/**
 * Scans the chain, correlates multi-block operations by id, and emits a
 * notification when one reaches a final state. It now handles two operation
 * types: VaultSwap swaps and loan repayments.
 *
 * Design note — why both operation types live in one class. Per the brief
 * ("extend the scanner"), the loan operation is added inline rather than as a
 * separate component. The block loop, checkpoint, confirmation depth, retry and
 * delivery are genuinely shared and operation-agnostic, so I kept a single
 * scanner driving a single getBlock stream. At scale I would factor the
 * operation-specific correlation out into one processor per operation type,
 * registered behind a small interface that the generic core dispatches to:
 *
 *     handleEvent(event)            // correlate this operation's own logs
 *     onBlockProcessed(blockNumber) // per-block hook — e.g. the loan default sweep
 *
 * Adding a third operation type would then be "register another processor" with
 * no change to the core, and the store would split along the same seam
 * (ISwapStore / ILoanStore). The class keeps the VaultSwapScanner name for
 * continuity with Parts 1–3; ProtocolScanner would be the honest name once the
 * processors are extracted. Kept inline here to match the assignment's scope.
 */
export class VaultSwapScanner {
  private readonly node: IBlockchainNode;
  private readonly notifier: INotifier;
  private readonly startBlock: number;

  private readonly store: IScannerStore;
  private readonly confirmations: number;
  private readonly retry: RetryOptions;
  private readonly autoDrain: boolean;
  private readonly maxDeliveryAttempts: number;
  private readonly log: ILogger;

  /**
   * @param startBlock First block to scan, INCLUSIVE — on a fresh run the scan
   *   begins exactly here (e.g. startBlock 500 first reads block 500, not 501).
   *   A swap whose events predate it is therefore outside the window. Defaults to
   *   1; values <= 1 begin at block 1 (genesis carries no protocol events). A
   *   persisted checkpoint overrides it.
   */
  constructor(node: IBlockchainNode, notifier: INotifier, startBlock = 1, options: ScannerOptions = {}) {
    this.node = node;
    this.notifier = notifier;
    this.startBlock = startBlock;

    this.store = options.store ?? new InMemoryScannerStore(options.maxReorgDepth);
    this.confirmations = options.confirmations ?? 0;
    this.retry = options.retry ?? DEFAULT_RETRY;
    this.autoDrain = options.autoDrain ?? true;
    this.maxDeliveryAttempts = options.maxDeliveryAttempts ?? 5;
    this.log = options.logger ?? consoleLogger;
  }

  /**
   * One-shot catch-up: process every not-yet-seen block from the resume point
   * up to the current safe tip, then resolve.
   *
   * In production a scheduler (cron / interval / long-poll) invokes start()
   * repeatedly to follow the chain as new blocks arrive; each call picks up
   * exactly where the last left off via the persisted checkpoint. Letting the
   * caller drive the loop keeps the method deterministic and easy to test, and
   * lets the polling cadence be tuned independently.
   */
  async start(): Promise<void> {
    // Resume point: a persisted checkpoint (restart) wins over the constructor
    // startBlock. `lastProcessed` is the last block we have fully handled, so
    // scanning begins at lastProcessed + 1. startBlock is INCLUSIVE — the first
    // block to scan — so the seed is startBlock - 1, making the loop start exactly
    // at startBlock. The seed floors at 0 (block 1 is the lowest scannable block;
    // genesis carries no protocol events), so startBlock <= 1 begins at block 1.
    const persisted = await this.store.getCheckpoint();
    let lastProcessed = persisted ?? Math.max(this.startBlock - 1, 0);
    if (persisted === null) {
      // Seed the checkpoint so a crash before the first block still resumes from
      // startBlock rather than block 0.
      await this.store.setCheckpoint(lastProcessed);
    }

    const latest = await withRetry(() => this.node.getLatestBlockNumber(), this.retry);
    const safeTip = latest - this.confirmations;

    this.log.info(
      `Scanner starting: resume@${lastProcessed + 1} latest=${latest} ` +
        `confirmations=${this.confirmations} safeTip=${safeTip}`,
    );

    if (safeTip <= lastProcessed) {
      this.log.info('Nothing to do: already caught up to the safe tip.');
    } else {
      for (let n = lastProcessed + 1; n <= safeTip; n++) {
        const block = await withRetry(() => this.node.getBlock(n), {
          ...this.retry,
          onRetry: (err, attempt) =>
            this.log.warn(`getBlock(${n}) failed (attempt ${attempt}): ${describe(err)}`),
        });

        // Reorg guard: block n must build on the block we processed as n-1.
        // Confirmation depth makes shallow reorgs invisible (we only process
        // blocks that are already `confirmations` deep); this catches the
        // dangerous residue — a reorg deeper than that, or a node serving an
        // inconsistent chain. The hash is null for the very first block (no
        // fetched predecessor to compare), so the check is skipped there.
        const expectedParent = await this.store.getLastBlockHash();
        if (expectedParent !== null && block.parentHash !== expectedParent) {
          // Try to recover: rewind to the common ancestor and re-scan the
          // canonical chain. recoverOrThrow() rewinds the store and returns the
          // ancestor, or throws ReorgDetectedError when recovery isn't safe (reorg
          // deeper than the window, or an already-emitted notification orphaned).
          // Resume the loop from the ancestor; n++ moves to ancestor+1.
          const ancestor = await this.recoverOrThrow(n, block, expectedParent);
          n = ancestor;
          lastProcessed = ancestor;
          continue;
        }

        await this.processBlock(block);

        // Advance the checkpoint only AFTER the whole block is processed, so a
        // crash mid-block re-processes that block (safe: notifications are
        // deduped by the notified-set) rather than skipping its tail of events.
        // The block's hash is committed with the checkpoint so the next run's
        // reorg guard has a consistent predecessor to compare against.
        await this.store.setCheckpoint(n, block.hash);
        lastProcessed = n;
      }

      this.log.info(`Scanner caught up to block ${lastProcessed}.`);
    }

    // Delivery is off the critical path: the block loop above only enqueues to the
    // outbox, so a slow or down notifier never holds up scanning or the
    // checkpoint. The drain runs after the loop (even on a no-new-blocks pass, to
    // retry anything still queued) and is non-fatal — a failing delivery is
    // retried or dead-lettered, never thrown back into the scan. Set
    // autoDrain:false to run delivery as a fully separate loop instead.
    if (this.autoDrain) await this.drainOutbox();
  }

  // ── Reorg recovery ─────────────────────────────────────────────────────────

  /**
   * Recover from a detected reorg, or throw if recovery isn't safe.
   *
   * Walks back to the last block whose stored hash still matches the canonical
   * chain — the common ancestor — then either rewinds to it and re-scans, or
   * escalates to a halt:
   *   - no ancestor within the recovery window → the reorg is deeper than we can
   *     verify; halt.
   *   - a notification was already emitted for a block past the ancestor → that
   *     emission is orphaned and we cannot un-notify it; halt.
   *   - otherwise → rewind the store to the ancestor (dropping the orphaned
   *     pending tail) and return it, so the loop re-scans the canonical chain.
   *     Nothing was emitted from the orphaned range, so there is nothing to retract.
   */
  private async recoverOrThrow(n: number, block: Block, expectedParent: string): Promise<number> {
    const ancestor = await this.findCommonAncestor(n, block);
    if (ancestor === null) {
      this.log.error(
        `Reorg at block ${n} (parentHash ${block.parentHash} != ${expectedParent}) is deeper ` +
          `than the recovery window; no common ancestor found. Halting.`,
      );
      throw new ReorgDetectedError(n, expectedParent, block.parentHash);
    }
    if (await this.store.hasNotificationAfter(ancestor)) {
      this.log.error(
        `Reorg at block ${n} orphaned a block after ${ancestor} for which a notification was ` +
          `already emitted; cannot un-notify. Halting.`,
      );
      throw new ReorgDetectedError(n, expectedParent, block.parentHash);
    }
    await this.store.rewindTo(ancestor);
    this.log.warn(
      `Reorg recovered: rewound checkpoint to common ancestor ${ancestor} and re-scanning the ` +
        `canonical chain forward (orphaned blocks ${ancestor + 1}..${n - 1}).`,
    );
    return ancestor;
  }

  /**
   * Find the highest block whose stored hash still matches the canonical chain.
   * `block` is the canonical block n, so `block.parentHash` is the canonical hash
   * of n-1. We compare canonical hashes against the stored window walking back;
   * the first match is the common ancestor. Returns null if the divergence runs
   * past the window (unverifiable → caller halts).
   */
  private async findCommonAncestor(n: number, block: Block): Promise<number | null> {
    let canonicalChildParent = block.parentHash; // canonical hash of the block below the cursor
    for (let k = n - 1; k >= 1; k--) {
      const stored = await this.store.getBlockHash(k);
      if (stored === null) return null; // walked past the retained window
      if (stored === canonicalChildParent) return k; // stored == canonical: common ancestor
      // Block k is orphaned; fetch the canonical block at height k for its parent.
      const canonicalK = await withRetry(() => this.node.getBlock(k), this.retry);
      canonicalChildParent = canonicalK.parentHash;
    }
    return null;
  }

  // ── Per-block processing ──────────────────────────────────────────────────

  private async processBlock(block: Block): Promise<void> {
    const events = block.logs
      .map((rawLog) => parseLog(rawLog, block.number, this.log))
      .filter((event): event is ProtocolEvent => event !== null);

    // Two passes within the block. A operation's events are causally ordered
    // ACROSS blocks (a swap cannot settle before it locks; a loan cannot be
    // repaid before it is requested), but the order of logs WITHIN a single block
    // is not guaranteed to respect that. Recording openers (SwapRequested /
    // FundsLocked / LoanRequested) before handling closers makes correlation
    // independent of intra-block log ordering: a closer that shares a block with
    // its own prerequisite still sees it, instead of failing to correlate and
    // deleting the operation's anchor — which would lose it permanently.
    for (const event of events) {
      if (event.type === 'SwapRequested' || event.type === 'FundsLocked') {
        await this.recordPartial(event);
      } else if (event.type === 'LoanRequested') {
        await this.recordLoan(event);
      }
    }
    for (const event of events) {
      if (
        event.type === 'SwapSettled' ||
        event.type === 'FundsLockFailed' ||
        event.type === 'SwapCancelled'
      ) {
        await this.handleTerminal(event);
      } else if (event.type === 'LoanRepaid') {
        await this.handleLoanRepaid(event);
      }
    }

    // A loan default is the absence of a repayment by the deadline, so it has no
    // log to react to. After this block's events are applied, sweep for loans
    // whose dueBlock has now been reached and are still outstanding. Running this
    // every block (even logless ones) is what lets a default fire on time.
    await this.sweepLoanDefaults(block.number);
  }

  private async recordPartial(event: SwapRequestedEvent | FundsLockedEvent): Promise<void> {
    // If we've already emitted for this swap, any further event for it is a
    // duplicate / out-of-order straggler (e.g. a reorg replay) — ignore it.
    if (await this.store.isNotified(event.swapId)) return;

    const state: SwapState = (await this.store.getPending(event.swapId)) ?? {};
    if (event.type === 'SwapRequested') {
      state.requested = event;
    } else {
      state.fundsLocked = event;
    }
    await this.store.putPending(event.swapId, state);
  }

  /**
   * Handle any terminal event (SwapSettled / FundsLockFailed / SwapCancelled).
   * The first terminal event seen for a swap finalises it; the swap is then
   * sealed by the notified-set, so any later terminal (e.g. a settle racing a
   * cancel) or straggler partial is ignored.
   */
  private async handleTerminal(event: TerminalSwapEvent): Promise<void> {
    // Dedupe: never emit twice for the same swap, even across restarts.
    if (await this.store.isNotified(event.swapId)) {
      this.log.warn(`Duplicate terminal event (${event.type}) for ${event.swapId} ignored; swap already finalised.`);
      return;
    }

    const state = (await this.store.getPending(event.swapId)) ?? {};
    const notification = this.correlate(event, state);

    if (!notification) {
      // Could not be correlated (reason already logged in correlate()). Drop any
      // partial state so it does not linger as a never-completing swap.
      await this.store.deletePending(event.swapId);
      return;
    }

    await this.finalizeSwap(notification);
    this.log.info(`Finalised ${event.swapId} (${notification.outcome}); queued for delivery.`);
  }

  /**
   * Seal a swap and enqueue its notification for delivery. Enqueue → mark → clean
   * up are all local, reliable writes — no notifier call on the critical path. In
   * production these commit in one transaction with the checkpoint, so advancing
   * the checkpoint and durably queuing the notification are the same atomic fact
   * (exactly-once effect). Here the small window makes it at-least-once: the
   * notified-set seals against re-enqueue, and the consumer dedupes on swapId.
   */
  private async finalizeSwap(notification: SwapNotification): Promise<void> {
    await this.store.enqueueOutbox({
      key: `swap:${notification.swapId}`,
      kind: 'swap',
      notification,
      attempts: 0,
    });
    // Tag with the terminal block so reorg recovery knows whether this emission
    // would be orphaned by a rewind past it.
    await this.store.markNotified(notification.swapId, notification.terminal.blockNumber);
    await this.store.deletePending(notification.swapId);
  }

  /**
   * Build the notification for a terminal event, or null if it can't be
   * correlated and should be skipped.
   *
   * Every terminal needs the SwapRequested as its correlation anchor; without it
   * we usually started the scan mid-swap (start at block 500, requested at 490)
   * and can't produce a useful notification. Backfill is the production fix; see
   * NOTES.md. SwapSettled additionally requires FundsLocked, so a filled/expired
   * notification always carries its full three-step provenance — a missing lock
   * there signals a real gap. lock_failed / cancelled need only the request,
   * because a missing lock is the expected shape for those outcomes.
   */
  private correlate(event: TerminalSwapEvent, state: SwapState): SwapNotification | null {
    if (!state.requested) {
      this.log.warn(
        `${event.type} for ${event.swapId} at block ${event.blockNumber} has no prior ` +
          `SwapRequested (likely started mid-swap); skipping notification.`,
      );
      return null;
    }

    switch (event.type) {
      case 'SwapSettled':
        if (!state.fundsLocked) {
          this.log.warn(
            `SwapSettled for ${event.swapId} at block ${event.blockNumber} cannot be ` +
              `correlated (missing FundsLocked); skipping notification.`,
          );
          return null;
        }
        return {
          swapId: event.swapId,
          outcome: event.status,
          requested: state.requested,
          fundsLocked: state.fundsLocked,
          settled: event,
          terminal: event,
        };

      case 'FundsLockFailed':
        return {
          swapId: event.swapId,
          outcome: 'lock_failed',
          requested: state.requested,
          terminal: event,
          reason: event.reason,
        };

      case 'SwapCancelled':
        return {
          swapId: event.swapId,
          outcome: 'cancelled',
          requested: state.requested,
          // Present only if the funds were locked before the cancel landed.
          ...(state.fundsLocked ? { fundsLocked: state.fundsLocked } : {}),
          terminal: event,
          reason: event.reason,
        };

      default: {
        // Exhaustiveness guard: if a new TerminalSwapEvent variant is added to
        // the union but not handled here, this fails to compile. At runtime it
        // also refuses to silently drop the event.
        const unhandled: never = event;
        throw new Error(`Unhandled terminal event type: ${JSON.stringify(unhandled)}`);
      }
    }
  }

  // ── Loans (Part 4) ────────────────────────────────────────────────────────

  private async recordLoan(event: LoanRequestedEvent): Promise<void> {
    // Already finalised (repaid/defaulted)? A further request log is a duplicate
    // or straggler — ignore it.
    if (await this.store.isLoanNotified(event.loanId)) return;

    // First request wins. A re-request for a still-pending loan should be an
    // identical replay; if it carries a DIFFERENT dueBlock it is a protocol
    // anomaly (the deadline would silently move and re-time the default), so keep
    // the original and warn rather than overwrite.
    const existing = await this.store.getPendingLoan(event.loanId);
    if (existing) {
      if (existing.dueBlock !== event.dueBlock) {
        this.log.warn(
          `LoanRequested for ${event.loanId} at block ${event.blockNumber} conflicts ` +
            `with the recorded dueBlock ${existing.dueBlock} (new ${event.dueBlock}); ` +
            `keeping the original.`,
        );
      }
      return;
    }

    await this.store.putPendingLoan(event);
  }

  /**
   * A repayment finalises a loan as `repaid` only if it lands strictly before the
   * deadline. A repayment in or after `dueBlock` is too late: the loan has
   * defaulted (or will, in this same block's sweep), so it is left for the
   * default path rather than emitted as repaid.
   */
  private async handleLoanRepaid(event: LoanRepaidEvent): Promise<void> {
    if (await this.store.isLoanNotified(event.loanId)) {
      this.log.warn(`LoanRepaid for ${event.loanId} ignored; loan already finalised.`);
      return;
    }

    const requested = await this.store.getPendingLoan(event.loanId);
    if (!requested) {
      // No anchor: started mid-loan (request before our scan window), or a
      // repayment for a loan we never saw requested. Can't build a notification.
      this.log.warn(
        `LoanRepaid for ${event.loanId} at block ${event.blockNumber} has no prior ` +
          `LoanRequested (likely started mid-loan); skipping notification.`,
      );
      return;
    }

    if (event.blockNumber >= requested.dueBlock) {
      // On or after the deadline → not a valid repayment. Leave it pending so the
      // per-block default sweep finalises it as defaulted.
      this.log.warn(
        `LoanRepaid for ${event.loanId} at block ${event.blockNumber} is at/after ` +
          `dueBlock ${requested.dueBlock}; too late, treating as default.`,
      );
      return;
    }

    const notification: LoanNotification = {
      loanId: event.loanId,
      outcome: 'repaid',
      requested,
      repaid: event,
    };
    await this.finalizeLoan(notification, event.blockNumber);
    this.log.info(`Finalised loan ${event.loanId} (repaid); queued for delivery.`);
  }

  /**
   * Finalise a `defaulted` notification for every outstanding loan whose deadline
   * has been reached at `blockNumber`. Defaults are deduped by the notified-loan
   * set, so re-running this after a crash/replay is safe. Each default is enqueued
   * to the outbox, not delivered here, so an undeliverable default never stalls
   * the sweep, the block, or anything behind it.
   */
  private async sweepLoanDefaults(blockNumber: number): Promise<void> {
    const due = await this.store.loansDueBy(blockNumber);
    for (const loan of due) {
      if (await this.store.isLoanNotified(loan.loanId)) continue;
      const notification: LoanNotification = {
        loanId: loan.loanId,
        outcome: 'defaulted',
        requested: loan,
      };
      await this.finalizeLoan(notification, blockNumber);
      this.log.info(
        `Finalised loan ${loan.loanId} (defaulted; dueBlock ${loan.dueBlock} ` +
          `reached); queued for delivery.`,
      );
    }
  }

  /**
   * Loan counterpart of finalizeSwap: enqueue → mark → clean up, all local.
   * `finalizeBlock` is the block the loan was finalised at (the repayment block,
   * or the sweep block for a default), tagged on the notified record so reorg
   * recovery knows whether a rewind would orphan this emission.
   */
  private async finalizeLoan(notification: LoanNotification, finalizeBlock: number): Promise<void> {
    await this.store.enqueueOutbox({
      key: `loan:${notification.loanId}`,
      kind: 'loan',
      notification,
      attempts: 0,
    });
    await this.store.markLoanNotified(notification.loanId, finalizeBlock);
    await this.store.deletePendingLoan(notification.loanId);
  }

  // ── Outbox delivery (off the critical path) ─────────────────────────────────

  /**
   * Deliver queued notifications. Public so a scheduler can run delivery as its
   * own loop, independent of scanning. Each entry is attempted once per call;
   * repeated calls are the retry mechanism. A delivery failure is caught (never
   * thrown back to the caller), counted, and — once it has failed
   * `maxDeliveryAttempts` times — moved to the dead-letter queue so one
   * undeliverable notification cannot block the rest of the queue.
   */
  async drainOutbox(): Promise<void> {
    const pending = await this.store.peekOutbox();
    for (const entry of pending) {
      try {
        if (entry.kind === 'swap') {
          await this.notifier.notify(entry.notification as SwapNotification);
        } else {
          await this.notifier.notifyLoan(entry.notification as LoanNotification);
        }
        await this.store.markDelivered(entry.key);
        this.log.info(`Delivered ${entry.key}.`);
      } catch (err) {
        const attempts = entry.attempts + 1;
        await this.store.recordOutboxAttempt(entry.key);
        this.log.warn(
          `Delivery of ${entry.key} failed (attempt ${attempts}/` +
            `${this.maxDeliveryAttempts}): ${describe(err)}`,
        );
        if (attempts >= this.maxDeliveryAttempts) {
          await this.store.moveToDeadLetter(entry.key);
          this.log.error(`Dead-lettered ${entry.key} after ${attempts} failed attempts.`);
        }
      }
    }
  }
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
