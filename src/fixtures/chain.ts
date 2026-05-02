import { Block } from '../types';

/**
 * A sample happy-path chain: swap-101 completes across 3 blocks.
 * The scanner should emit one SwapNotification with outcome "filled".
 */
export const HAPPY_PATH_CHAIN: Block[] = [
  {
    number: 1000,
    hash: '0xblock1000',
    parentHash: '0xblock999',
    logs: [
      {
        address: '0xVaultSwap',
        event: 'SwapRequested',
        args: {
          txHash: '0xtx_a',
          swapId: 'swap-101',
          user: '0xAlice',
          tokenIn: 'LTC',
          tokenOut: 'BTC',
          amountIn: '10000000',
        },
      },
    ],
  },
  {
    number: 1001,
    hash: '0xblock1001',
    parentHash: '0xblock1000',
    logs: [
      {
        address: '0xVaultSwap',
        event: 'FundsLocked',
        args: {
          txHash: '0xtx_b',
          swapId: 'swap-101',
          vault: '0xVault1',
          amountIn: '10000000',
        },
      },
    ],
  },
  {
    number: 1002,
    hash: '0xblock1002',
    parentHash: '0xblock1001',
    logs: [
      {
        address: '0xVaultSwap',
        event: 'SwapSettled',
        args: {
          txHash: '0xtx_c',
          swapId: 'swap-101',
          amountOut: '9950000',
          status: 'filled',
        },
      },
    ],
  },
];
