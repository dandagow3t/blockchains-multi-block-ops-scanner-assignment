import { FundsLockedEvent, SwapRequestedEvent } from './types';

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

  getPending(swapId: string): Promise<SwapState | undefined>;
  putPending(swapId: string, state: SwapState): Promise<void>;
  deletePending(swapId: string): Promise<void>;

  isNotified(swapId: string): Promise<boolean>;
  /** Records the swap as notified, tagged with the block it was finalised at. */
  markNotified(swapId: string, blockNumber: number): Promise<void>;
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
  }

  async hasNotificationAfter(blockNumber: number): Promise<boolean> {
    for (const b of this.notified.values()) if (b > blockNumber) return true;
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
}
