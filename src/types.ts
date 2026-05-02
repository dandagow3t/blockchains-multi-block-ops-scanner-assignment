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

export type SwapEvent = SwapRequestedEvent | FundsLockedEvent | SwapSettledEvent;

// ── Notification ──────────────────────────────────────────────────────────────

export type SwapOutcome = 'filled' | 'expired';

export interface SwapNotification {
  swapId: string;
  outcome: SwapOutcome;
  requested: SwapRequestedEvent;
  fundsLocked: FundsLockedEvent;
  settled: SwapSettledEvent;
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
