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
  RawLog,
  SwapNotification,
  SwapRequestedEvent,
  SwapSettledEvent,
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
   * How many blocks deep a reorg can be and still be auto-recovered. The default
   * in-memory store keeps this many recent block hashes; a reorg whose common
   * ancestor falls outside the window can't be located, so the scanner halts
   * rather than guess. Only used when the scanner builds its own store. Default 100.
   */
  maxReorgDepth?: number;

  logger?: ILogger;
}

// ── Event parsing ─────────────────────────────────────────────────────────────

function parseLog(log: RawLog, blockNumber: number): SwapRequestedEvent | FundsLockedEvent | SwapSettledEvent | null {
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
    case 'SwapSettled':
      return {
        type: 'SwapSettled',
        swapId: String(log.args['swapId']),
        amountOut: String(log.args['amountOut']),
        status: log.args['status'] === 'expired' ? 'expired' : 'filled',
        blockNumber,
        txHash,
      };
    default:
      return null;
  }
}

// ── Scanner ───────────────────────────────────────────────────────────────────

export class VaultSwapScanner {
  private readonly node: IBlockchainNode;
  private readonly notifier: INotifier;
  private readonly startBlock: number;

  private readonly store: IScannerStore;
  private readonly confirmations: number;
  private readonly retry: RetryOptions;
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
      return;
    }

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
        // Try to recover: rewind to the common ancestor and re-scan the canonical
        // chain. recoverOrThrow() rewinds the store and returns the ancestor, or
        // throws ReorgDetectedError when recovery isn't safe (reorg deeper than the
        // window, or an already-emitted notification was orphaned). Resume the loop
        // from the ancestor; n++ moves to ancestor+1.
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
    for (const rawLog of block.logs) {
      const event = parseLog(rawLog, block.number);
      if (!event) continue;

      switch (event.type) {
        case 'SwapRequested':
        case 'FundsLocked':
          await this.recordPartial(event);
          break;
        case 'SwapSettled':
          await this.handleSettled(event);
          break;
      }
    }
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

  private async handleSettled(event: SwapSettledEvent): Promise<void> {
    // Dedupe: never emit twice for the same swap, even across restarts.
    if (await this.store.isNotified(event.swapId)) {
      this.log.warn(`Duplicate SwapSettled for ${event.swapId} ignored.`);
      return;
    }

    const state = await this.store.getPending(event.swapId);

    if (!state?.requested || !state?.fundsLocked) {
      // Incomplete correlation, usually because the scan started mid-swap
      // (start at block 500, swap requested at 490). The earlier events were
      // never seen, so a complete SwapNotification can't be built (its
      // requested/fundsLocked fields are required). Log and drop rather than
      // emit a partial notification. Backfilling the earlier blocks is the
      // production fix; see NOTES.md.
      const missing = [
        state?.requested ? null : 'SwapRequested',
        state?.fundsLocked ? null : 'FundsLocked',
      ].filter(Boolean);
      this.log.warn(
        `SwapSettled for ${event.swapId} at block ${event.blockNumber} cannot be ` +
          `correlated (missing ${missing.join(', ')}); skipping notification.`,
      );
      await this.store.deletePending(event.swapId);
      return;
    }

    const notification: SwapNotification = {
      swapId: event.swapId,
      outcome: event.status,
      requested: state.requested,
      fundsLocked: state.fundsLocked,
      settled: event,
    };

    // At-least-once: notify first, then mark + clean up. A crash between
    // notify() and markNotified() re-emits this one notification on restart,
    // preferable to at-most-once (losing it). Exactly-once in production comes
    // from an idempotent notifier keyed on swapId, or a transactional outbox so
    // notify + markNotified commit atomically.
    await withRetry(() => this.notifier.notify(notification), {
      ...this.retry,
      onRetry: (err, attempt) =>
        this.log.warn(`notify(${event.swapId}) failed (attempt ${attempt}): ${describe(err)}`),
    });

    // Tag with the settle block so reorg recovery knows whether this emission
    // would be orphaned by a rewind past it.
    await this.store.markNotified(event.swapId, event.blockNumber);
    await this.store.deletePending(event.swapId);

    this.log.info(`Emitted notification for ${event.swapId} (${event.status}).`);
  }
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
