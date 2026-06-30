// ── Blockchain primitives - DO NOT TOUCH --────────────────────────────────────

export interface RawLog {
  address: string;   // contract address
  event: string;     // event name
  args: Record<string, string | number>;
}

export interface Block {
  number: number;
  hash: string;
  parentHash: string;
  logs: RawLog[];
}

// ── VaultSwap protocol events ─────────────────────────────────────────────────

export interface SwapRequestedEvent {
  type: 'SwapRequested';
  swapId: string;
  user: string;
  tokenIn: string;
  tokenOut: string;
  amountIn: string;
  blockNumber: number;
  txHash: string;
}

export interface FundsLockedEvent {
  type: 'FundsLocked';
  swapId: string;
  vault: string;
  amountIn: string;
  blockNumber: number;
  txHash: string;
}

export interface SwapSettledEvent {
  type: 'SwapSettled';
  swapId: string;
  amountOut: string;
  status: 'filled' | 'expired';
  blockNumber: number;
  txHash: string;
}

// ── Part 3: terminal events that end a swap early ────────────────────────────

/**
 * The vault could not lock the funds (e.g. insufficient liquidity). Terminal:
 * the swap ends here, having only been requested.
 */
export interface FundsLockFailedEvent {
  type: 'FundsLockFailed';
  swapId: string;
  vault: string;
  reason: string;
  blockNumber: number;
  txHash: string;
}

/**
 * The user or the protocol cancelled the swap after it was requested but before
 * it settled. Terminal. May arrive before or after FundsLocked.
 */
export interface SwapCancelledEvent {
  type: 'SwapCancelled';
  swapId: string;
  /**
   * Who cancelled. 'unknown' is used when the on-chain log carries a `by` value
   * the scanner does not recognise — the cancel is still real, but the scanner
   * records the attribution honestly rather than guessing 'user'.
   */
  by: 'user' | 'protocol' | 'unknown';
  reason: string;
  blockNumber: number;
  txHash: string;
}

export type SwapEvent =
  | SwapRequestedEvent
  | FundsLockedEvent
  | SwapSettledEvent
  | FundsLockFailedEvent
  | SwapCancelledEvent;

/** Events that put a swap into a final state and trigger a notification. */
export type TerminalSwapEvent = SwapSettledEvent | FundsLockFailedEvent | SwapCancelledEvent;

// ── Notification ──────────────────────────────────────────────────────────────

export type SwapOutcome = 'filled' | 'expired' | 'lock_failed' | 'cancelled';

/**
 * Emitted once when a swap reaches a final state.
 *
 * `requested` is always present — it is the correlation anchor, so a swap with
 * no observed request is never notified. `fundsLocked` and `settled` are
 * optional because a swap can now end early:
 *
 *   - filled / expired → reached SwapSettled; `fundsLocked` and `settled` set.
 *   - lock_failed      → vault failed to lock; neither `fundsLocked` nor
 *                        `settled` set; `reason` explains why.
 *   - cancelled        → aborted before settling; `fundsLocked` set only if the
 *                        funds were locked before the cancel; `reason` set.
 *
 * `terminal` is the on-chain event that ended the swap, always present. It is the
 * single place a consumer can read the ending's `blockNumber` / `txHash` (and, by
 * narrowing on its `type`, `by` for a cancel or `vault` for a lock failure)
 * regardless of outcome. For filled / expired it is the same object as `settled`.
 */
export interface SwapNotification {
  swapId: string;
  outcome: SwapOutcome;
  requested: SwapRequestedEvent;
  fundsLocked?: FundsLockedEvent;
  settled?: SwapSettledEvent;
  terminal: TerminalSwapEvent;
  /** Why a non-filled swap ended (lock_failed / cancelled). Absent for filled. */
  reason?: string;
}

// ── Node interface ────────────────────────────────────────────────────────────

export interface IBlockchainNode {
  getLatestBlockNumber(): Promise<number>;
  getBlock(blockNumber: number): Promise<Block>;
}

// ── Notifier interface ────────────────────────────────────────────────────────

export interface INotifier {
  notify(notification: SwapNotification): Promise<void>;
}
