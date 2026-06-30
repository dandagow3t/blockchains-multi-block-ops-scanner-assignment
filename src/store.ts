import {
  FundsLockedEvent,
  LoanNotification,
  LoanRequestedEvent,
  SwapNotification,
  SwapRequestedEvent,
} from './types';

/**
 * In-flight state for a swap that has not yet settled.
 *
 * A swap is pending once one of its events has been seen but not the terminal
 * `SwapSettled`. The full events are stored (not just flags) because the final
 * `SwapNotification` carries them.
 */
export interface SwapState {
  requested?: SwapRequestedEvent;
  fundsLocked?: FundsLockedEvent;
}

/**
 * A finalised notification awaiting delivery — the transactional-outbox record.
 *
 * Finalising an operation enqueues one of these (a local, reliable write) instead
 * of calling the notifier on the critical path; a separate drain delivers them.
 * `key` is `${kind}:${id}`, the idempotency key, so re-enqueueing the same
 * operation is a no-op rather than a duplicate.
 */
export interface OutboxEntry {
  key: string;
  kind: 'swap' | 'loan';
  notification: SwapNotification | LoanNotification;
  /** Failed delivery attempts so far; drives the dead-letter threshold. */
  attempts: number;
}

/**
 * Durable scanner state.
 *
 * Restart-safety (no duplicate notifications after a restart) rests entirely on
 * what this persists:
 *
 *   - checkpoint    last fully-processed block; on restart, scanning resumes at
 *                   checkpoint+1, so settled blocks are never re-scanned.
 *   - pending swaps partially-correlated swaps; must survive a restart, or a
 *                   swap requested+locked before the restart and settled after
 *                   it could never complete.
 *   - notified set  swapIds already notified; a second dedupe layer that holds
 *                   even if a block is replayed.
 *
 * Methods are async so the call-sites stay unchanged when the in-memory store
 * is replaced by a database.
 *
 * In production this is a transactional store (e.g. Postgres): checkpoint +
 * pending + notified update in one transaction per block, so the checkpoint
 * never advances without the notifications it implies being durable. `notified`
 * becomes a unique index, or is replaced by an idempotency key on the notifier
 * (transactional outbox) for exactly-once delivery instead of the at-least-once
 * this in-memory version provides. See NOTES.md.
 */
export interface IScannerStore {
  /** Last fully-processed block number, or null if the scanner has never run. */
  getCheckpoint(): Promise<number | null>;
  /**
   * Record the checkpoint and, atomically, the hash of that block. The hash is
   * the predecessor a reorg check compares the next block's `parentHash` against;
   * committing it with the checkpoint keeps the two from ever skewing. Omit the
   * hash only when seeding a checkpoint for a block we never fetched (startBlock).
   */
  setCheckpoint(blockNumber: number, hash?: string): Promise<void>;
  /** Hash of the checkpoint block, or null if unknown (fresh / seeded start). */
  getLastBlockHash(): Promise<string | null>;
  /** Hash of a specific processed block if still in the recovery window, else null. */
  getBlockHash(blockNumber: number): Promise<string | null>;
  /**
   * Roll all state back to `ancestor` after a reorg: rewind the checkpoint, drop
   * the orphaned tail of the hash window, and discard pending state derived from
   * orphaned blocks (block > ancestor) so re-scanning rebuilds it from the
   * canonical chain.
   */
  rewindTo(ancestor: number): Promise<void>;
  /**
   * Whether any notification was already emitted for an operation finalised AFTER
   * `blockNumber`. A reorg below such a block orphaned an emission we cannot
   * un-notify, so recovery escalates to a halt rather than rewriting history.
   */
  hasNotificationAfter(blockNumber: number): Promise<boolean>;

  // ── Swaps ──
  getPending(swapId: string): Promise<SwapState | undefined>;
  putPending(swapId: string, state: SwapState): Promise<void>;
  deletePending(swapId: string): Promise<void>;

  isNotified(swapId: string): Promise<boolean>;
  /** Records the swap as notified, tagged with the block it was finalised at. */
  markNotified(swapId: string, blockNumber: number): Promise<void>;

  // ── Loans (Part 4) ──
  // A loan's only pending state is its request, which carries the deadline; once
  // repaid or defaulted it is removed and recorded in the notified set.
  getPendingLoan(loanId: string): Promise<LoanRequestedEvent | undefined>;
  putPendingLoan(loan: LoanRequestedEvent): Promise<void>;
  deletePendingLoan(loanId: string): Promise<void>;

  /**
   * Outstanding loans whose dueBlock has been reached (dueBlock <= blockNumber).
   * These are the loans that default at `blockNumber` unless already repaid
   * (repaid loans are removed from pending, so they never appear here).
   */
  loansDueBy(blockNumber: number): Promise<LoanRequestedEvent[]>;

  isLoanNotified(loanId: string): Promise<boolean>;
  /** Records the loan as notified, tagged with the block it was finalised at. */
  markLoanNotified(loanId: string, blockNumber: number): Promise<void>;

  // ── Outbox (delivery off the critical path) ──
  // In production these rows commit in the SAME transaction as the checkpoint and
  // the notified/pending updates, so "the checkpoint advanced" and "a delivery is
  // durably queued" are the same atomic fact. A separate worker drains them.
  enqueueOutbox(entry: OutboxEntry): Promise<void>;
  /** Undelivered entries in FIFO order. */
  peekOutbox(): Promise<OutboxEntry[]>;
  recordOutboxAttempt(key: string): Promise<void>;
  markDelivered(key: string): Promise<void>;
  moveToDeadLetter(key: string): Promise<void>;
  getDeadLetters(): Promise<OutboxEntry[]>;
}

/**
 * In-memory implementation, used here and in tests.
 *
 * To simulate a restart, construct a new `VaultSwapScanner` with the same store
 * instance: the checkpoint, pending swaps and notified set carry over as they
 * would across a process restart.
 */
export class InMemoryScannerStore implements IScannerStore {
  private checkpoint: number | null = null;
  /**
   * Recent block hashes keyed by block number — a bounded window (the last
   * `maxReorgDepth` processed blocks). The reorg guard compares the next block's
   * parentHash against these, and recovery walks back through them to locate the
   * common ancestor. Bounded so memory stays flat; a reorg deeper than the window
   * can't be located and is escalated to a halt.
   */
  private readonly blockHashes = new Map<number, string>();
  private readonly pending = new Map<string, SwapState>();
  /** swapId -> block it was finalised at (drives the recovery un-notify guard). */
  private readonly notified = new Map<string, number>();

  /** @param maxReorgDepth how many recent block hashes to retain for recovery. */
  constructor(private readonly maxReorgDepth = 100) {}

  async getCheckpoint(): Promise<number | null> {
    return this.checkpoint;
  }

  async setCheckpoint(blockNumber: number, hash?: string): Promise<void> {
    this.checkpoint = blockNumber;
    // Record the hash in the window (the seed path passes none — a block we never
    // fetched — so the first real block skips the reorg check), then prune past
    // the recovery window so memory stays bounded.
    if (hash !== undefined) {
      this.blockHashes.set(blockNumber, hash);
      const floor = blockNumber - this.maxReorgDepth;
      for (const k of this.blockHashes.keys()) if (k <= floor) this.blockHashes.delete(k);
    }
  }

  async getLastBlockHash(): Promise<string | null> {
    return this.checkpoint === null ? null : this.blockHashes.get(this.checkpoint) ?? null;
  }

  async getBlockHash(blockNumber: number): Promise<string | null> {
    return this.blockHashes.get(blockNumber) ?? null;
  }

  async rewindTo(ancestor: number): Promise<void> {
    this.checkpoint = ancestor;
    for (const k of this.blockHashes.keys()) if (k > ancestor) this.blockHashes.delete(k);
    // Drop pending swap fields recorded from now-orphaned blocks; re-scanning the
    // canonical chain re-records whatever it actually contains. A swap left with
    // no fields is removed entirely.
    for (const [swapId, state] of this.pending) {
      if (state.requested && state.requested.blockNumber > ancestor) state.requested = undefined;
      if (state.fundsLocked && state.fundsLocked.blockNumber > ancestor) state.fundsLocked = undefined;
      if (!state.requested && !state.fundsLocked) this.pending.delete(swapId);
    }
    // Loans recorded from orphaned blocks are dropped the same way (Part 4).
    for (const [loanId, loan] of this.pendingLoans) {
      if (loan.blockNumber > ancestor) this.pendingLoans.delete(loanId);
    }
  }

  async hasNotificationAfter(blockNumber: number): Promise<boolean> {
    for (const b of this.notified.values()) if (b > blockNumber) return true;
    for (const b of this.notifiedLoans.values()) if (b > blockNumber) return true;
    return false;
  }

  async getPending(swapId: string): Promise<SwapState | undefined> {
    const state = this.pending.get(swapId);
    // Return a shallow copy so callers can't mutate stored state by reference.
    return state ? { ...state } : undefined;
  }

  async putPending(swapId: string, state: SwapState): Promise<void> {
    this.pending.set(swapId, { ...state });
  }

  async deletePending(swapId: string): Promise<void> {
    this.pending.delete(swapId);
  }

  async isNotified(swapId: string): Promise<boolean> {
    return this.notified.has(swapId);
  }

  async markNotified(swapId: string, blockNumber: number): Promise<void> {
    this.notified.set(swapId, blockNumber);
  }

  // ── Loans (Part 4) ──

  private readonly pendingLoans = new Map<string, LoanRequestedEvent>();
  /** loanId -> block it was finalised at (drives the recovery un-notify guard). */
  private readonly notifiedLoans = new Map<string, number>();

  async getPendingLoan(loanId: string): Promise<LoanRequestedEvent | undefined> {
    return this.pendingLoans.get(loanId);
  }

  async putPendingLoan(loan: LoanRequestedEvent): Promise<void> {
    this.pendingLoans.set(loan.loanId, loan);
  }

  async deletePendingLoan(loanId: string): Promise<void> {
    this.pendingLoans.delete(loanId);
  }

  async loansDueBy(blockNumber: number): Promise<LoanRequestedEvent[]> {
    // Linear scan of outstanding loans. Fine for an in-memory store; in
    // production this is a single indexed query (`WHERE due_block <= $1 AND
    // status = 'pending'`) or a min-heap keyed on dueBlock, so the per-block cost
    // is the number of loans that actually come due, not all outstanding ones.
    const due: LoanRequestedEvent[] = [];
    for (const loan of this.pendingLoans.values()) {
      if (loan.dueBlock <= blockNumber) due.push(loan);
    }
    return due;
  }

  async isLoanNotified(loanId: string): Promise<boolean> {
    return this.notifiedLoans.has(loanId);
  }

  async markLoanNotified(loanId: string, blockNumber: number): Promise<void> {
    this.notifiedLoans.set(loanId, blockNumber);
  }

  // ── Outbox (Part 4 follow-up: delivery off the critical path) ──

  private readonly outbox = new Map<string, OutboxEntry>();
  private readonly deadLetters: OutboxEntry[] = [];

  async enqueueOutbox(entry: OutboxEntry): Promise<void> {
    // Idempotent by key: re-finalising the same operation overwrites rather than
    // duplicating. Map preserves the original insertion position, so FIFO order
    // is stable across an overwrite.
    this.outbox.set(entry.key, { ...entry });
  }

  async peekOutbox(): Promise<OutboxEntry[]> {
    return Array.from(this.outbox.values()).map((e) => ({ ...e }));
  }

  async recordOutboxAttempt(key: string): Promise<void> {
    const entry = this.outbox.get(key);
    if (entry) entry.attempts += 1;
  }

  async markDelivered(key: string): Promise<void> {
    this.outbox.delete(key);
  }

  async moveToDeadLetter(key: string): Promise<void> {
    const entry = this.outbox.get(key);
    if (entry) {
      this.outbox.delete(key);
      this.deadLetters.push(entry);
    }
  }

  async getDeadLetters(): Promise<OutboxEntry[]> {
    return this.deadLetters.map((e) => ({ ...e }));
  }
}
