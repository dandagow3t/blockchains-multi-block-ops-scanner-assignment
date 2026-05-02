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

// ── In-progress swap state ────────────────────────────────────────────────────

interface SwapState {
  requested?: SwapRequestedEvent;
  fundsLocked?: FundsLockedEvent;
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
  private node: IBlockchainNode;
  private notifier: INotifier;

  private lastProcessedBlock: number;
  private pendingSwaps: Map<string, SwapState> = new Map();

  constructor(node: IBlockchainNode, notifier: INotifier, startBlock: number = 0) {
    this.node = node;
    this.notifier = notifier;
    this.lastProcessedBlock = startBlock;
  }

  async start(): Promise<void> {
    console.log(`Scanner starting from block ${this.lastProcessedBlock}`);

    throw new Error("Implement me . . . !");
  }
}
