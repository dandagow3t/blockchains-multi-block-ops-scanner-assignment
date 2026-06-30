import { SimulatedNode } from '../src/node';
import { RetryOptions } from '../src/retry';
import {
  ILogger,
  ReorgDetectedError,
  ScannerOptions,
  VaultSwapScanner,
} from '../src/scanner';
import { InMemoryScannerStore } from '../src/store';
import {
  Block,
  FundsLockFailedEvent,
  IBlockchainNode,
  INotifier,
  LoanNotification,
  RawLog,
  SwapCancelledEvent,
  SwapNotification,
} from '../src/types';

// ── Test helpers ──────────────────────────────────────────────────────────────

const CONTRACT = '0xVaultSwap';

/**
 * Retry policy for tests: no backoff delay and no jitter, so retry paths are
 * exercised deterministically and instantly (no real timers).
 */
const FAST_RETRY: RetryOptions = {
  maxRetries: 5,
  baseDelayMs: 0,
  maxDelayMs: 0,
  jitter: 'none',
};

/** Swallow scanner logging so test output stays readable. */
const silentLogger: ILogger = { info: () => {}, warn: () => {}, error: () => {} };

/** Logger that records messages, for the few tests that assert on log output. */
class RecordingLogger implements ILogger {
  readonly infos: string[] = [];
  readonly warns: string[] = [];
  readonly errors: string[] = [];
  info(m: string): void {
    this.infos.push(m);
  }
  warn(m: string): void {
    this.warns.push(m);
  }
  error(m: string): void {
    this.errors.push(m);
  }
}

class CapturingNotifier implements INotifier {
  public notifications: SwapNotification[] = [];
  public loanNotifications: LoanNotification[] = [];
  async notify(n: SwapNotification): Promise<void> {
    this.notifications.push(n);
  }
  async notifyLoan(n: LoanNotification): Promise<void> {
    this.loanNotifications.push(n);
  }
}

/**
 * Notifier that fails its first `failuresLeft` calls, then succeeds. Used to
 * model a flaky downstream sink. The same instance carried across two scanner
 * runs simulates "failed during this run, recovered on restart". The failure
 * budget is shared across swap and loan deliveries.
 */
class FlakyNotifier implements INotifier {
  public notifications: SwapNotification[] = [];
  public loanNotifications: LoanNotification[] = [];
  constructor(private failuresLeft: number) {}
  async notify(n: SwapNotification): Promise<void> {
    this.maybeFail();
    this.notifications.push(n);
  }
  async notifyLoan(n: LoanNotification): Promise<void> {
    this.maybeFail();
    this.loanNotifications.push(n);
  }
  private maybeFail(): void {
    if (this.failuresLeft > 0) {
      this.failuresLeft--;
      throw new Error('notify failed (downstream unavailable)');
    }
  }
}

/** Always fails delivery for the given swapIds; delivers everything else. */
class PoisonNotifier implements INotifier {
  public notifications: SwapNotification[] = [];
  public loanNotifications: LoanNotification[] = [];
  constructor(private readonly poisonSwapIds: Set<string>) {}
  async notify(n: SwapNotification): Promise<void> {
    if (this.poisonSwapIds.has(n.swapId)) throw new Error(`poison: ${n.swapId}`);
    this.notifications.push(n);
  }
  async notifyLoan(n: LoanNotification): Promise<void> {
    this.loanNotifications.push(n);
  }
}

/**
 * Decorates any IBlockchainNode to add the things a real node has and the
 * SimulatedNode does not: a controllable tip (so confirmation depth and chain
 * growth can be simulated), transient getBlock failures, and call recording.
 *
 * It delegates to the wrapped node for actual block data, so the chain is still
 * built with the provided SimulatedNode.
 */
class UnreliableNode implements IBlockchainNode {
  readonly getBlockCalls: number[] = [];
  private failuresLeft = new Map<number, number>();
  private latestOverride: number | null = null;

  constructor(private readonly inner: IBlockchainNode) {}

  /** Pin the reported tip (e.g. to model the chain having grown). */
  withLatest(n: number): this {
    this.latestOverride = n;
    return this;
  }

  /** Make getBlock(n) throw the next `times` times it is called. */
  failBlock(n: number, times: number): this {
    this.failuresLeft.set(n, times);
    return this;
  }

  async getLatestBlockNumber(): Promise<number> {
    return this.latestOverride ?? this.inner.getLatestBlockNumber();
  }

  async getBlock(n: number): Promise<Block> {
    this.getBlockCalls.push(n);
    const left = this.failuresLeft.get(n) ?? 0;
    if (left > 0) {
      this.failuresLeft.set(n, left - 1);
      throw new Error(`transient RPC error on block ${n}`);
    }
    return this.inner.getBlock(n);
  }
}

// ── Chain builders ──────────────────────────────────────────────────────────

let txSeq = 0;
function nextTx(): string {
  return `0xtx${txSeq++}`;
}

function block(number: number, logs: RawLog[] = [], hash = `0xblock${number}`): Block {
  return {
    number,
    hash,
    parentHash: `0xblock${number - 1}`,
    logs,
  };
}

function requestedLog(swapId: string, over: Record<string, string> = {}): RawLog {
  return {
    address: CONTRACT,
    event: 'SwapRequested',
    args: {
      txHash: nextTx(),
      swapId,
      user: '0xAlice',
      tokenIn: 'LTC',
      tokenOut: 'BTC',
      amountIn: '10000000',
      ...over,
    },
  };
}

function lockedLog(swapId: string, over: Record<string, string> = {}): RawLog {
  return {
    address: CONTRACT,
    event: 'FundsLocked',
    args: {
      txHash: nextTx(),
      swapId,
      vault: '0xVault1',
      amountIn: '10000000',
      ...over,
    },
  };
}

function settledLog(
  swapId: string,
  status: 'filled' | 'expired' = 'filled',
  over: Record<string, string> = {},
): RawLog {
  return {
    address: CONTRACT,
    event: 'SwapSettled',
    args: {
      txHash: nextTx(),
      swapId,
      amountOut: '9950000',
      status,
      ...over,
    },
  };
}

function lockFailedLog(
  swapId: string,
  reason = 'insufficient_liquidity',
  over: Record<string, string> = {},
): RawLog {
  return {
    address: CONTRACT,
    event: 'FundsLockFailed',
    args: { txHash: nextTx(), swapId, vault: '0xVault1', reason, ...over },
  };
}

function cancelledLog(
  swapId: string,
  by: 'user' | 'protocol' = 'user',
  reason = 'user_request',
  over: Record<string, string> = {},
): RawLog {
  return {
    address: CONTRACT,
    event: 'SwapCancelled',
    args: { txHash: nextTx(), swapId, by, reason, ...over },
  };
}

function loanRequestedLog(
  loanId: string,
  dueBlock: number,
  over: Record<string, string | number> = {},
): RawLog {
  return {
    address: CONTRACT,
    event: 'LoanRequested',
    args: { txHash: nextTx(), loanId, borrower: '0xBob', amount: '5000000', dueBlock, ...over },
  };
}

function loanRepaidLog(
  loanId: string,
  over: Record<string, string | number> = {},
): RawLog {
  return {
    address: CONTRACT,
    event: 'LoanRepaid',
    args: { txHash: nextTx(), loanId, borrower: '0xBob', amountRepaid: '5000000', ...over },
  };
}

/** A self-contained 3-block swap starting at `from`: requested, locked, settled. */
function happyPath(swapId: string, from: number, status: 'filled' | 'expired' = 'filled'): Block[] {
  return [
    block(from, [requestedLog(swapId)]),
    block(from + 1, [lockedLog(swapId)]),
    block(from + 2, [settledLog(swapId, status)]),
  ];
}

function makeScanner(
  node: IBlockchainNode,
  notifier: INotifier,
  opts: { startBlock?: number } & ScannerOptions = {},
): VaultSwapScanner {
  // startBlock is inclusive and defaults to 1 (chains in these tests begin at
  // block 1), so the default scans the whole chain just as before.
  const { startBlock = 1, ...scannerOpts } = opts;
  return new VaultSwapScanner(node, notifier, startBlock, {
    logger: silentLogger,
    retry: FAST_RETRY,
    ...scannerOpts,
  });
}

// ── Tests ─────────────────────────────────────────────────────────────────────

describe('correlation & the happy path', () => {
  test('emits one notification, fully correlated, when a swap completes across three blocks', async () => {
    const notifier = new CapturingNotifier();
    const node = new SimulatedNode(happyPath('swap-1', 1));

    await makeScanner(node, notifier).start();

    expect(notifier.notifications).toHaveLength(1);
    const n = notifier.notifications[0];
    expect(n).toMatchObject({
      swapId: 'swap-1',
      outcome: 'filled',
      requested: { type: 'SwapRequested', swapId: 'swap-1', user: '0xAlice', blockNumber: 1 },
      fundsLocked: { type: 'FundsLocked', swapId: 'swap-1', vault: '0xVault1', blockNumber: 2 },
      settled: { type: 'SwapSettled', swapId: 'swap-1', amountOut: '9950000', blockNumber: 3 },
    });
  });

  test('carries through the "expired" outcome', async () => {
    const notifier = new CapturingNotifier();
    const node = new SimulatedNode(happyPath('swap-exp', 1, 'expired'));

    await makeScanner(node, notifier).start();

    expect(notifier.notifications).toHaveLength(1);
    expect(notifier.notifications[0].outcome).toBe('expired');
  });

  test('correlates multiple interleaved swaps independently', async () => {
    const notifier = new CapturingNotifier();
    // Two swaps whose events are interleaved across blocks; settle in B-then-A order.
    const node = new SimulatedNode([
      block(1, [requestedLog('A')]),
      block(2, [requestedLog('B')]),
      block(3, [lockedLog('A')]),
      block(4, [lockedLog('B')]),
      block(5, [settledLog('B', 'filled')]),
      block(6, [settledLog('A', 'expired')]),
    ]);

    await makeScanner(node, notifier).start();

    expect(notifier.notifications.map((n) => [n.swapId, n.outcome])).toEqual([
      ['B', 'filled'],
      ['A', 'expired'],
    ]);
  });

  test('correlates events that arrive out of order (FundsLocked before SwapRequested)', async () => {
    const notifier = new CapturingNotifier();
    const node = new SimulatedNode([
      block(1, [lockedLog('swap-ooo')]),
      block(2, [requestedLog('swap-ooo')]),
      block(3, [settledLog('swap-ooo')]),
    ]);

    await makeScanner(node, notifier).start();

    expect(notifier.notifications).toHaveLength(1);
    expect(notifier.notifications[0]).toMatchObject({
      swapId: 'swap-ooo',
      fundsLocked: { blockNumber: 1 },
      requested: { blockNumber: 2 },
    });
  });

  test('handles all three events landing in a single block', async () => {
    const notifier = new CapturingNotifier();
    const node = new SimulatedNode([
      block(1, [requestedLog('swap-1block'), lockedLog('swap-1block'), settledLog('swap-1block')]),
    ]);

    await makeScanner(node, notifier).start();

    expect(notifier.notifications).toHaveLength(1);
    expect(notifier.notifications[0].swapId).toBe('swap-1block');
  });

  test('ignores logs from other contracts and unknown event types', async () => {
    const notifier = new CapturingNotifier();
    const node = new SimulatedNode([
      block(1, [
        { address: '0xSomeOtherContract', event: 'SwapRequested', args: { swapId: 'noise', user: 'x', tokenIn: 'A', tokenOut: 'B', amountIn: '1', txHash: '0xz' } },
        { address: CONTRACT, event: 'SomethingUnrelated', args: { swapId: 'noise', txHash: '0xy' } },
        requestedLog('swap-real'),
      ]),
      block(2, [lockedLog('swap-real')]),
      block(3, [settledLog('swap-real')]),
    ]);

    await makeScanner(node, notifier).start();

    expect(notifier.notifications).toHaveLength(1);
    expect(notifier.notifications[0].swapId).toBe('swap-real');
  });
});

describe('incomplete operations', () => {
  test('does not notify for a swap that is requested and locked but never settles', async () => {
    const notifier = new CapturingNotifier();
    const node = new SimulatedNode([
      block(1, [requestedLog('dangling')]),
      block(2, [lockedLog('dangling')]),
      block(3, []),
    ]);

    await makeScanner(node, notifier).start();

    expect(notifier.notifications).toHaveLength(0);
  });

  test('drops an uncorrelated SwapSettled (prior events never seen) without notifying', async () => {
    // Models starting the scan mid-swap: the request/lock happened before our
    // window, so a complete notification cannot be built. It must be dropped,
    // not emitted as a partial.
    const notifier = new CapturingNotifier();
    const logger = new RecordingLogger();
    const node = new SimulatedNode([block(1, [settledLog('orphan')])]);

    await makeScanner(node, notifier, { logger }).start();

    expect(notifier.notifications).toHaveLength(0);
    expect(logger.warns.some((m) => m.includes('orphan') && m.includes('skipping'))).toBe(true);
  });

  test('startBlock is inclusive: a swap that began before it is seen only at its settle, then dropped', async () => {
    const notifier = new CapturingNotifier();
    const logger = new RecordingLogger();
    // Swap began at block 1 (request) + 2 (lock); the scanner starts INCLUSIVELY
    // at block 3, so it scans exactly block 3 — the settle — and nothing earlier.
    // The classic "start at 500, requested at 490": the settle arrives with no
    // anchor and must be dropped, not emitted as a partial.
    const node = new SimulatedNode(happyPath('early-swap', 1));

    await makeScanner(node, notifier, { startBlock: 3, logger }).start();

    // The settle at block 3 was reached (proving startBlock 3 is inclusive) but
    // dropped for lack of its SwapRequested anchor.
    expect(notifier.notifications).toHaveLength(0);
    expect(
      logger.warns.some((m) => m.includes('early-swap') && m.includes('skipping')),
    ).toBe(true);
  });

  test('startBlock is inclusive: a swap whose request lands exactly at startBlock is fully correlated', async () => {
    const notifier = new CapturingNotifier();
    // Request at block 5 == startBlock. Under the old exclusive semantics the scan
    // would have begun at 6 and missed this request; inclusive must catch it.
    const node = new SimulatedNode([
      block(5, [requestedLog('at-start')]),
      block(6, [lockedLog('at-start')]),
      block(7, [settledLog('at-start', 'filled')]),
    ]);

    await makeScanner(node, notifier, { startBlock: 5 }).start();

    expect(notifier.notifications).toHaveLength(1);
    expect(notifier.notifications[0]).toMatchObject({ swapId: 'at-start', outcome: 'filled' });
  });
});

describe('restart safety & deduplication', () => {
  test('does not re-emit notifications when restarted after a full catch-up', async () => {
    const store = new InMemoryScannerStore();
    const node = new SimulatedNode(happyPath('swap-1', 1));

    const first = new CapturingNotifier();
    await makeScanner(node, first, { store }).start();
    expect(first.notifications).toHaveLength(1);

    // "Restart": a brand-new scanner over the same durable store.
    const second = new CapturingNotifier();
    await makeScanner(node, second, { store }).start();
    expect(second.notifications).toHaveLength(0);
  });

  test('resumes from the checkpoint and does not re-fetch already-processed blocks', async () => {
    const store = new InMemoryScannerStore();
    // Two swaps: A in blocks 1-3, B in blocks 4-6.
    const node = new UnreliableNode(
      new SimulatedNode([...happyPath('A', 1), ...happyPath('B', 4)]),
    );

    // First pass: chain only reaches block 3, so only swap A is final.
    node.withLatest(3);
    const first = new CapturingNotifier();
    await makeScanner(node, first, { store }).start();
    expect(first.notifications.map((n) => n.swapId)).toEqual(['A']);

    // Chain grows to include swap B; restart and catch up.
    node.getBlockCalls.length = 0;
    node.withLatest(6);
    const second = new CapturingNotifier();
    await makeScanner(node, second, { store }).start();

    expect(second.notifications.map((n) => n.swapId)).toEqual(['B']);
    // Only the new blocks were fetched; blocks 1-3 were never re-read.
    expect(node.getBlockCalls).toEqual([4, 5, 6]);
  });

  test('ignores a duplicate SwapSettled within a single run', async () => {
    const notifier = new CapturingNotifier();
    const node = new SimulatedNode([
      block(1, [requestedLog('dup')]),
      block(2, [lockedLog('dup')]),
      block(3, [settledLog('dup', 'filled')]),
      block(4, [settledLog('dup', 'filled')]), // replayed settle
    ]);

    await makeScanner(node, notifier).start();

    expect(notifier.notifications).toHaveLength(1);
  });

  test('is idempotent to duplicated partial events (replay of the same log)', async () => {
    const notifier = new CapturingNotifier();
    const node = new SimulatedNode([
      block(1, [requestedLog('rep'), requestedLog('rep')]), // same partial twice
      block(2, [lockedLog('rep')]),
      block(3, [settledLog('rep')]),
    ]);

    await makeScanner(node, notifier).start();

    expect(notifier.notifications).toHaveLength(1);
    expect(notifier.notifications[0].swapId).toBe('rep');
  });
});

describe('confirmation depth (finality)', () => {
  test('does not process blocks still inside the confirmation window', async () => {
    const notifier = new CapturingNotifier();
    // Chain has the full swap in blocks 1-3, tip = 3, but 2 confirmations
    // required => safeTip = 1, so only the request is seen and nothing settles.
    const node = new UnreliableNode(new SimulatedNode(happyPath('swap-1', 1))).withLatest(3);

    await makeScanner(node, notifier, { confirmations: 2 }).start();

    expect(notifier.notifications).toHaveLength(0);
  });

  test('processes a swap once its settle block sinks below the confirmation depth', async () => {
    const store = new InMemoryScannerStore();
    const node = new UnreliableNode(new SimulatedNode(happyPath('swap-1', 1)));

    // First pass: tip = 3, confirmations = 2 => safeTip = 1. Not yet settled.
    node.withLatest(3);
    const first = new CapturingNotifier();
    await makeScanner(node, first, { store, confirmations: 2 }).start();
    expect(first.notifications).toHaveLength(0);

    // Chain advances to tip = 5 => safeTip = 3, so the settle is now final.
    node.withLatest(5);
    const second = new CapturingNotifier();
    await makeScanner(node, second, { store, confirmations: 2 }).start();
    expect(second.notifications).toHaveLength(1);
    expect(second.notifications[0].swapId).toBe('swap-1');
    // Blocks 4 and 5 do not exist and must never be fetched (safeTip is 3).
    expect(node.getBlockCalls).not.toContain(4);
    expect(node.getBlockCalls).not.toContain(5);
  });
});

describe('chain reorganisation', () => {
  // Part 1 addresses reorgs through confirmation depth (Level 1), not by
  // detecting and rolling back orphaned blocks (Level 2 — see NOTES.md). These
  // two tests pin that boundary: confirmation depth keeps us from acting on
  // blocks a shallow reorg can still replace, while a reorg of an
  // already-processed block is a documented, deliberate gap.
  //
  // The scanner does not read block.hash / parentHash; the differing hashes
  // below just make the fork explicit. That the scanner ignores them is exactly
  // why full Level-2 detection would be needed to handle a deep reorg.

  test('confirmation depth keeps a settle that a reorg later orphans from ever being emitted', async () => {
    const store = new InMemoryScannerStore();

    // So-far chain: the swap settles "filled" at block 3, but the tip is only 3
    // and we require 2 confirmations => safeTip = 1, so we never act on it.
    const preReorg = new UnreliableNode(
      new SimulatedNode([
        block(1, [requestedLog('swap-r')]),
        block(2, [lockedLog('swap-r')]),
        block(3, [settledLog('swap-r', 'filled')], '0xblock3-orphan'),
      ]),
    ).withLatest(3);

    const before = new CapturingNotifier();
    await makeScanner(preReorg, before, { store, confirmations: 2 }).start();
    expect(before.notifications).toHaveLength(0); // settle still inside the window

    // Reorg: block 3 is replaced. On the canonical chain the swap instead
    // settles "expired" at block 4; the orphaned "filled" settle is gone.
    const afterReorg = new UnreliableNode(
      new SimulatedNode([
        block(1, [requestedLog('swap-r')]),
        block(2, [lockedLog('swap-r')]),
        block(3, [], '0xblock3-canonical'), // same height, different block, no settle
        // block 4 builds on the canonical block 3, so parentHash is set to match
        // (the reorg guard now verifies this linkage).
        { number: 4, hash: '0xblock4', parentHash: '0xblock3-canonical', logs: [settledLog('swap-r', 'expired')] },
        block(5, []),
        block(6, []),
      ]),
    ).withLatest(6); // safeTip = 4, so the canonical settle is now final

    const after = new CapturingNotifier();
    await makeScanner(afterReorg, after, { store, confirmations: 2 }).start();

    // Exactly one notification, carrying the CANONICAL outcome. The orphaned
    // "filled" settle was never emitted, precisely because we waited for depth.
    expect(after.notifications).toHaveLength(1);
    expect(after.notifications[0]).toMatchObject({ swapId: 'swap-r', outcome: 'expired' });
  });

  test('recovers from a reorg by rewinding to the common ancestor and re-scanning the canonical chain', async () => {
    const store = new InMemoryScannerStore();
    const logger = new RecordingLogger();

    // Run 1: a clean 2-block prefix. The swap is requested + locked (PENDING, not
    // yet settled — so nothing has been emitted). Checkpoint reaches block 2.
    const run1 = new SimulatedNode([
      block(1, [requestedLog('swap-reorg')]),
      block(2, [lockedLog('swap-reorg')]),
    ]);
    await makeScanner(run1, new CapturingNotifier(), { store }).start();
    expect(await store.getCheckpoint()).toBe(2);

    // Run 2: block 2 was orphaned and replaced ('0xblock2-B'); block 3 settles the
    // swap on the NEW block 2. Resuming at block 3, its parentHash ('0xblock2-B')
    // no longer matches the block-2 hash we stored ('0xblock2'). Because nothing
    // was emitted from the orphaned range, the scanner rewinds to the common
    // ancestor (block 1), discards the orphaned lock, and re-scans forward —
    // emitting exactly one notification carrying the CANONICAL outcome.
    const notifier = new CapturingNotifier();
    const run2 = new SimulatedNode([
      block(1, [requestedLog('swap-reorg')]),
      { number: 2, hash: '0xblock2-B', parentHash: '0xblock1', logs: [lockedLog('swap-reorg')] },
      { number: 3, hash: '0xblock3', parentHash: '0xblock2-B', logs: [settledLog('swap-reorg', 'filled')] },
    ]);

    await makeScanner(run2, notifier, { store, logger }).start();

    expect(notifier.notifications).toHaveLength(1);
    expect(notifier.notifications[0]).toMatchObject({ swapId: 'swap-reorg', outcome: 'filled' });
    expect(await store.getCheckpoint()).toBe(3); // re-scanned forward to the canonical tip
    expect(logger.warns.some((m) => m.includes('Reorg recovered') && m.includes('ancestor 1'))).toBe(true);
  });

  test('halts (cannot recover) when the reorg orphans an already-emitted notification', async () => {
    const store = new InMemoryScannerStore();

    // Run 1 (confirmations 0): the swap SETTLES at block 3 and we emit 'filled'.
    const run1 = new CapturingNotifier();
    const original = new SimulatedNode(happyPath('swap-x', 1)); // req 1, lock 2, settle 3
    await makeScanner(original, run1, { store }).start();
    expect(run1.notifications.map((n) => n.outcome)).toEqual(['filled']);
    expect(await store.getCheckpoint()).toBe(3);

    // Run 2: a deep reorg orphans block 3 (the settle) — block 3 is replaced with
    // an empty '0xblock3-B' and a new block 4 builds on it. The common ancestor is
    // block 2, but we ALREADY emitted 'filled' from the now-orphaned block 3.
    // That emission can't be un-notified, so recovery must escalate to a halt
    // rather than silently rewrite history.
    const run2 = new CapturingNotifier();
    const reorged = new SimulatedNode([
      block(1, [requestedLog('swap-x')]),
      block(2, [lockedLog('swap-x')]),
      { number: 3, hash: '0xblock3-B', parentHash: '0xblock2', logs: [] }, // settle orphaned away
      { number: 4, hash: '0xblock4', parentHash: '0xblock3-B', logs: [] },
    ]);

    await expect(makeScanner(reorged, run2, { store }).start()).rejects.toThrow(ReorgDetectedError);
    expect(run2.notifications).toHaveLength(0); // no rewrite
    expect(await store.getCheckpoint()).toBe(3); // not rewound; left for operator intervention
  });

  test('halts when the reorg is deeper than the recovery window (no common ancestor found)', async () => {
    // maxReorgDepth 1 keeps only the last block's hash, so a 2-deep reorg cannot
    // be traced back to a common ancestor and must halt rather than guess.
    const store = new InMemoryScannerStore(1);

    const run1 = new SimulatedNode([block(1), block(2), block(3)]);
    await makeScanner(run1, new CapturingNotifier(), { store }).start();
    expect(await store.getCheckpoint()).toBe(3);

    // Block 3 is orphaned ('0xblock3-B') and block 4 builds on it. Walking back,
    // block 2's hash was already pruned from the 1-deep window, so no ancestor can
    // be confirmed.
    const run2 = new SimulatedNode([
      block(1),
      block(2),
      { number: 3, hash: '0xblock3-B', parentHash: '0xblock2', logs: [] },
      { number: 4, hash: '0xblock4', parentHash: '0xblock3-B', logs: [] },
    ]);

    await expect(makeScanner(run2, new CapturingNotifier(), { store }).start()).rejects.toThrow(
      ReorgDetectedError,
    );
    expect(await store.getCheckpoint()).toBe(3); // not advanced, not rewound
  });

  test('recovery drops an orphaned loan request so it never spuriously defaults', async () => {
    const store = new InMemoryScannerStore();
    const logger = new RecordingLogger();

    // Run 1: a loan is requested at block 2 (due at block 5) — pending, not yet
    // notified. Checkpoint reaches 2.
    const run1 = new SimulatedNode([block(1), block(2, [loanRequestedLog('loan-orph', 5)])]);
    await makeScanner(run1, new CapturingNotifier(), { store }).start();
    expect(await store.getCheckpoint()).toBe(2);

    // Run 2: block 2 is orphaned ('0xblock2-B') WITHOUT the loan request; the
    // canonical chain extends empty past the old due block. Recovery rewinds to
    // block 1 and drops the orphaned pending loan, so no 'defaulted' is ever
    // emitted for a loan that does not exist on the canonical chain.
    const notifier = new CapturingNotifier();
    const run2 = new SimulatedNode([
      block(1),
      { number: 2, hash: '0xblock2-B', parentHash: '0xblock1', logs: [] },
      { number: 3, hash: '0xblock3', parentHash: '0xblock2-B', logs: [] },
      block(4, [], '0xblock4'),
      block(5, [], '0xblock5'),
      block(6, [], '0xblock6'),
    ]);

    await makeScanner(run2, notifier, { store, logger }).start();

    expect(notifier.loanNotifications).toHaveLength(0); // orphaned loan never defaults
    expect(logger.warns.some((m) => m.includes('Reorg recovered'))).toBe(true);
    expect(await store.getCheckpoint()).toBe(6);
  });

  test('does NOT roll back a settle already processed before the reorg (Level-1 limitation)', async () => {
    const store = new InMemoryScannerStore();

    // confirmations: 0 — treat the tip as final. The swap settles "filled" at
    // block 3 and we emit immediately.
    const original = new SimulatedNode([
      block(1, [requestedLog('swap-x')]),
      block(2, [lockedLog('swap-x')]),
      block(3, [settledLog('swap-x', 'filled')], '0xblock3-A'),
    ]);

    const first = new CapturingNotifier();
    await makeScanner(original, first, { store, confirmations: 0 }).start();
    expect(first.notifications.map((n) => n.outcome)).toEqual(['filled']);

    // The chain reorgs: block 3 is replaced by a fork where the swap settled
    // "expired". The checkpoint is already at 3, so the scanner never re-reads
    // it: the earlier "filled" notification stands and the canonical "expired"
    // is never observed. Detecting and compensating for this is Level-2 reorg
    // handling, intentionally out of scope for Part 1 (see NOTES.md).
    const reorged = new SimulatedNode([
      block(1, [requestedLog('swap-x')]),
      block(2, [lockedLog('swap-x')]),
      block(3, [settledLog('swap-x', 'expired')], '0xblock3-B'),
    ]);

    const second = new CapturingNotifier();
    await makeScanner(reorged, second, { store, confirmations: 0 }).start();

    expect(second.notifications).toHaveLength(0); // no rollback, no re-emit
  });
});

describe('node reliability', () => {
  test('retries transient getBlock failures and still processes the block', async () => {
    const notifier = new CapturingNotifier();
    const node = new UnreliableNode(new SimulatedNode(happyPath('swap-1', 1)));
    node.failBlock(2, 2); // block 2 fails twice, then succeeds

    await makeScanner(node, notifier, { retry: { ...FAST_RETRY, maxRetries: 3 } }).start();

    expect(notifier.notifications).toHaveLength(1);
  });

  test('propagates a permanent node failure and does not advance the checkpoint past it', async () => {
    const store = new InMemoryScannerStore();
    const notifier = new CapturingNotifier();
    const node = new UnreliableNode(new SimulatedNode(happyPath('swap-1', 1)));
    node.failBlock(2, 99); // never recovers within the retry budget

    const scanner = makeScanner(node, notifier, { store, retry: { ...FAST_RETRY, maxRetries: 2 } });
    await expect(scanner.start()).rejects.toThrow(/block 2/);

    // Block 1 committed; block 2 failed before its checkpoint write.
    expect(await store.getCheckpoint()).toBe(1);
    expect(notifier.notifications).toHaveLength(0);
  });

  test('recovers after a node outage on restart, processing the rest of the chain exactly once', async () => {
    const store = new InMemoryScannerStore();
    const node = new UnreliableNode(new SimulatedNode(happyPath('swap-1', 1)));
    node.failBlock(2, 99);

    const failing = makeScanner(node, new CapturingNotifier(), {
      store,
      retry: { ...FAST_RETRY, maxRetries: 2 },
    });
    await expect(failing.start()).rejects.toThrow();

    // Node recovers; restart picks up from block 2 and finishes the swap.
    node.failBlock(2, 0);
    const recovered = new CapturingNotifier();
    await makeScanner(node, recovered, { store }).start();

    expect(recovered.notifications).toHaveLength(1);
    expect(recovered.notifications[0].swapId).toBe('swap-1');
  });

  // Notification-delivery reliability lives in its own suite below; the node's
  // reliability (getBlock / getLatestBlockNumber) is what this group covers.
});

describe('idle behaviour', () => {
  test('start() is a no-op when already caught up to the safe tip', async () => {
    const store = new InMemoryScannerStore();
    const node = new UnreliableNode(new SimulatedNode(happyPath('swap-1', 1)));

    await makeScanner(node, new CapturingNotifier(), { store }).start();
    node.getBlockCalls.length = 0;

    const notifier = new CapturingNotifier();
    await makeScanner(node, notifier, { store }).start();

    expect(notifier.notifications).toHaveLength(0);
    expect(node.getBlockCalls).toHaveLength(0); // no blocks fetched
  });

  test('an empty chain produces no notifications and does not throw', async () => {
    const notifier = new CapturingNotifier();
    const node = new SimulatedNode([]);

    await makeScanner(node, notifier).start();

    expect(notifier.notifications).toHaveLength(0);
  });
});

describe('early termination (Part 3)', () => {
  test('emits a lock_failed notification when the vault fails to lock funds', async () => {
    const notifier = new CapturingNotifier();
    const node = new SimulatedNode([
      block(1, [requestedLog('swap-lf')]),
      block(2, [lockFailedLog('swap-lf', 'insufficient_liquidity')]),
    ]);

    await makeScanner(node, notifier).start();

    expect(notifier.notifications).toHaveLength(1);
    const n = notifier.notifications[0];
    expect(n.outcome).toBe('lock_failed');
    expect(n.reason).toBe('insufficient_liquidity');
    expect(n.requested.swapId).toBe('swap-lf');
    expect(n.fundsLocked).toBeUndefined(); // funds never locked
    expect(n.settled).toBeUndefined(); // never settled
  });

  test('emits a cancelled notification when a swap is cancelled before funds are locked', async () => {
    const notifier = new CapturingNotifier();
    const node = new SimulatedNode([
      block(1, [requestedLog('swap-c1')]),
      block(2, [cancelledLog('swap-c1', 'user', 'user_request')]),
    ]);

    await makeScanner(node, notifier).start();

    expect(notifier.notifications).toHaveLength(1);
    expect(notifier.notifications[0]).toMatchObject({
      swapId: 'swap-c1',
      outcome: 'cancelled',
      reason: 'user_request',
    });
    expect(notifier.notifications[0].fundsLocked).toBeUndefined();
  });

  test('includes fundsLocked when a swap is cancelled after the funds were locked', async () => {
    const notifier = new CapturingNotifier();
    const node = new SimulatedNode([
      block(1, [requestedLog('swap-c2')]),
      block(2, [lockedLog('swap-c2')]),
      block(3, [cancelledLog('swap-c2', 'protocol', 'timeout')]),
    ]);

    await makeScanner(node, notifier).start();

    expect(notifier.notifications).toHaveLength(1);
    const n = notifier.notifications[0];
    expect(n.outcome).toBe('cancelled');
    expect(n.fundsLocked).toMatchObject({ swapId: 'swap-c2', blockNumber: 2 });
  });

  test('the first terminal event seals the swap (a settle racing a cancel does not double-notify)', async () => {
    const notifier = new CapturingNotifier();
    const node = new SimulatedNode([
      block(1, [requestedLog('swap-race')]),
      block(2, [lockedLog('swap-race')]),
      block(3, [cancelledLog('swap-race', 'user', 'user_request')]),
      block(4, [settledLog('swap-race', 'filled')]), // late settle after the cancel
    ]);

    await makeScanner(node, notifier).start();

    expect(notifier.notifications).toHaveLength(1);
    expect(notifier.notifications[0].outcome).toBe('cancelled'); // first terminal wins
  });

  test('a settle still requires FundsLocked: a settle with no observed lock is dropped', async () => {
    const notifier = new CapturingNotifier();
    const logger = new RecordingLogger();
    const node = new SimulatedNode([
      block(1, [requestedLog('swap-nolock')]),
      block(2, [settledLog('swap-nolock', 'filled')]), // requested, but never locked
    ]);

    await makeScanner(node, notifier, { logger }).start();

    expect(notifier.notifications).toHaveLength(0);
    expect(logger.warns.some((m) => m.includes('swap-nolock') && m.includes('FundsLocked'))).toBe(true);
  });

  test('drops an early-termination event that has no observed request (started mid-swap)', async () => {
    const notifier = new CapturingNotifier();
    const node = new SimulatedNode([block(1, [lockFailedLog('swap-orphan')])]);

    await makeScanner(node, notifier).start();

    expect(notifier.notifications).toHaveLength(0);
  });

  test('does not re-emit an early-termination notification after restart', async () => {
    const store = new InMemoryScannerStore();
    const node = new SimulatedNode([
      block(1, [requestedLog('swap-dedupe')]),
      block(2, [cancelledLog('swap-dedupe')]),
    ]);

    await makeScanner(node, new CapturingNotifier(), { store }).start();

    const second = new CapturingNotifier();
    await makeScanner(node, second, { store }).start();
    expect(second.notifications).toHaveLength(0);
  });
});

describe('notification completeness & input validation (Part 3)', () => {
  // A — the terminating event is surfaced on every outcome, not only on settled,
  // so a consumer can always locate the on-chain ending and read its fields.
  test('lock_failed notification carries the terminating event (block, tx, vault)', async () => {
    const notifier = new CapturingNotifier();
    const node = new SimulatedNode([
      block(1, [requestedLog('swap-A1')]),
      block(2, [lockFailedLog('swap-A1', 'insufficient_liquidity')]),
    ]);

    await makeScanner(node, notifier).start();

    const n = notifier.notifications[0];
    expect(n.terminal.type).toBe('FundsLockFailed');
    expect(n.terminal.blockNumber).toBe(2);
    expect(n.terminal.txHash).toBeTruthy();
    expect((n.terminal as FundsLockFailedEvent).vault).toBe('0xVault1');
    expect((n.terminal as FundsLockFailedEvent).reason).toBe('insufficient_liquidity');
  });

  test('cancelled notification preserves who cancelled via the terminating event', async () => {
    const notifier = new CapturingNotifier();
    const node = new SimulatedNode([
      block(1, [requestedLog('swap-A2')]),
      block(2, [cancelledLog('swap-A2', 'protocol', 'risk_hold')]),
    ]);

    await makeScanner(node, notifier).start();

    const n = notifier.notifications[0];
    expect(n.terminal.type).toBe('SwapCancelled');
    expect((n.terminal as SwapCancelledEvent).by).toBe('protocol');
    expect(n.terminal.blockNumber).toBe(2);
    expect(n.terminal.txHash).toBeTruthy();
  });

  test('for a filled swap, terminal is the same object as settled', async () => {
    const notifier = new CapturingNotifier();
    const node = new SimulatedNode(happyPath('swap-A3', 1));

    await makeScanner(node, notifier).start();

    const n = notifier.notifications[0];
    expect(n.terminal.type).toBe('SwapSettled');
    expect(n.terminal).toBe(n.settled); // same reference
  });

  // B — unsafe enum coercion removed.
  test('an unrecognised settle status is dropped, never coerced to "filled"', async () => {
    const notifier = new CapturingNotifier();
    const logger = new RecordingLogger();
    const node = new SimulatedNode([
      block(1, [requestedLog('swap-B1')]),
      block(2, [lockedLog('swap-B1')]),
      block(3, [settledLog('swap-B1', 'filled', { status: 'bogus' })]), // garbage status
    ]);

    await makeScanner(node, notifier, { logger }).start();

    expect(notifier.notifications).toHaveLength(0); // NOT a false 'filled'
    expect(
      logger.warns.some((m) => m.includes('swap-B1') && m.includes('unrecognised status')),
    ).toBe(true);
  });

  test('an unrecognised cancel "by" is recorded as "unknown", not mis-attributed to the user', async () => {
    const notifier = new CapturingNotifier();
    const logger = new RecordingLogger();
    const node = new SimulatedNode([
      block(1, [requestedLog('swap-B2')]),
      block(2, [cancelledLog('swap-B2', 'user', 'odd', { by: 'bot' })]), // garbage 'by'
    ]);

    await makeScanner(node, notifier, { logger }).start();

    expect(notifier.notifications).toHaveLength(1);
    expect((notifier.notifications[0].terminal as SwapCancelledEvent).by).toBe('unknown');
    expect(logger.warns.some((m) => m.includes('swap-B2') && m.includes('unknown'))).toBe(true);
  });
});

describe('intra-block log ordering', () => {
  // C — correlation must not depend on the order of logs within a block. A
  // terminal positioned before its own prerequisite in the same block must still
  // correlate, rather than dropping the swap and erasing its anchor.

  test('a SwapSettled placed before its FundsLocked in the same block still correlates', async () => {
    const notifier = new CapturingNotifier();
    const node = new SimulatedNode([
      block(1, [requestedLog('swap-ord')]),
      // settle log sits BEFORE the lock log in the same block
      block(2, [settledLog('swap-ord', 'filled'), lockedLog('swap-ord')]),
    ]);

    await makeScanner(node, notifier).start();

    expect(notifier.notifications).toHaveLength(1);
    expect(notifier.notifications[0]).toMatchObject({
      swapId: 'swap-ord',
      outcome: 'filled',
      fundsLocked: { blockNumber: 2 },
      settled: { blockNumber: 2 },
    });
  });

  test('all three events in one block, terminal-first, still correlate', async () => {
    const notifier = new CapturingNotifier();
    const node = new SimulatedNode([
      // fully reversed order within the block: settled, locked, requested
      block(1, [
        settledLog('swap-rev', 'filled'),
        lockedLog('swap-rev'),
        requestedLog('swap-rev'),
      ]),
    ]);

    await makeScanner(node, notifier).start();

    expect(notifier.notifications).toHaveLength(1);
    expect(notifier.notifications[0].swapId).toBe('swap-rev');
  });

  test('a cancel before its lock in the same block still captures the lock', async () => {
    const notifier = new CapturingNotifier();
    const node = new SimulatedNode([
      block(1, [requestedLog('swap-cl')]),
      // cancel log before the lock log; the lock is recorded first, so it is
      // included in the cancellation notification.
      block(2, [cancelledLog('swap-cl', 'user', 'user_request'), lockedLog('swap-cl')]),
    ]);

    await makeScanner(node, notifier).start();

    expect(notifier.notifications).toHaveLength(1);
    const n = notifier.notifications[0];
    expect(n.outcome).toBe('cancelled');
    expect(n.fundsLocked).toMatchObject({ blockNumber: 2 });
  });
});

describe('loan operations (Part 4)', () => {
  test('emits a repaid notification when a loan is repaid before its dueBlock', async () => {
    const notifier = new CapturingNotifier();
    const node = new SimulatedNode([
      block(1, [loanRequestedLog('loan-1', 5)]),
      block(2, []),
      block(3, [loanRepaidLog('loan-1')]),
    ]);

    await makeScanner(node, notifier).start();

    expect(notifier.notifications).toHaveLength(0); // no swap notifications
    expect(notifier.loanNotifications).toHaveLength(1);
    expect(notifier.loanNotifications[0]).toMatchObject({
      loanId: 'loan-1',
      outcome: 'repaid',
      requested: { type: 'LoanRequested', dueBlock: 5, blockNumber: 1 },
      repaid: { type: 'LoanRepaid', blockNumber: 3, amountRepaid: '5000000' },
    });
  });

  test('emits a defaulted notification when dueBlock is reached with no repayment', async () => {
    const notifier = new CapturingNotifier();
    const node = new SimulatedNode([
      block(1, [loanRequestedLog('loan-d', 4)]),
      block(2, []),
      block(3, []),
      block(4, []), // dueBlock reached, still unpaid
    ]);

    await makeScanner(node, notifier).start();

    expect(notifier.loanNotifications).toHaveLength(1);
    const n = notifier.loanNotifications[0];
    expect(n.outcome).toBe('defaulted');
    expect(n.repaid).toBeUndefined(); // a default has no terminating event
    expect(n.requested.dueBlock).toBe(4);
  });

  test('a repayment landing AT dueBlock is too late, and the loan defaults', async () => {
    const notifier = new CapturingNotifier();
    const node = new SimulatedNode([
      block(1, [loanRequestedLog('loan-late', 3)]),
      block(2, []),
      block(3, [loanRepaidLog('loan-late')]), // repaid exactly at dueBlock — not "before"
    ]);

    await makeScanner(node, notifier).start();

    expect(notifier.loanNotifications).toHaveLength(1);
    expect(notifier.loanNotifications[0].outcome).toBe('defaulted');
  });

  test('a default does not fire until its dueBlock is confirmation-deep', async () => {
    const store = new InMemoryScannerStore();
    const node = new UnreliableNode(
      new SimulatedNode([block(1, [loanRequestedLog('loan-c', 3)]), block(2, []), block(3, [])]),
    );

    // tip = 3, confirmations = 2 => safeTip = 1: dueBlock 3 is not yet final.
    node.withLatest(3);
    const first = new CapturingNotifier();
    await makeScanner(node, first, { store, confirmations: 2 }).start();
    expect(first.loanNotifications).toHaveLength(0);

    // Chain advances; safeTip reaches 3, so the default is now safe to emit.
    node.withLatest(5);
    const second = new CapturingNotifier();
    await makeScanner(node, second, { store, confirmations: 2 }).start();
    expect(second.loanNotifications.map((n) => n.outcome)).toEqual(['defaulted']);
  });

  test('a pending loan survives a restart and defaults when its dueBlock later arrives', async () => {
    const store = new InMemoryScannerStore();
    const node = new UnreliableNode(
      new SimulatedNode([block(1, [loanRequestedLog('loan-r', 3)]), block(2, []), block(3, [])]),
    );

    // First run only reaches block 2: the loan is recorded but not yet due.
    node.withLatest(2);
    const first = new CapturingNotifier();
    await makeScanner(node, first, { store }).start();
    expect(first.loanNotifications).toHaveLength(0);

    // Restart over the same store; the chain now reaches the dueBlock.
    node.withLatest(3);
    const second = new CapturingNotifier();
    await makeScanner(node, second, { store }).start();
    expect(second.loanNotifications.map((n) => n.outcome)).toEqual(['defaulted']);
  });

  test('a default is emitted once and not re-emitted as the chain grows', async () => {
    const store = new InMemoryScannerStore();
    const node = new UnreliableNode(
      new SimulatedNode([block(1, [loanRequestedLog('loan-1x', 2)]), block(2, []), block(3, [])]),
    );

    node.withLatest(2);
    const first = new CapturingNotifier();
    await makeScanner(node, first, { store }).start();
    expect(first.loanNotifications).toHaveLength(1); // defaulted at block 2

    node.withLatest(3);
    const second = new CapturingNotifier();
    await makeScanner(node, second, { store }).start();
    expect(second.loanNotifications).toHaveLength(0); // not re-emitted
  });

  test('drops a LoanRepaid with no observed LoanRequested (started mid-loan)', async () => {
    const notifier = new CapturingNotifier();
    const node = new SimulatedNode([block(1, [loanRepaidLog('loan-orphan')])]);

    await makeScanner(node, notifier).start();

    expect(notifier.loanNotifications).toHaveLength(0);
  });

  test('correlates two loans independently — one repaid, one defaulted', async () => {
    const notifier = new CapturingNotifier();
    const node = new SimulatedNode([
      block(1, [loanRequestedLog('loan-A', 5), loanRequestedLog('loan-B', 3)]),
      block(2, [loanRepaidLog('loan-A')]), // A repaid before its due block 5
      block(3, []), // B's due block 3 reached, unpaid
    ]);

    await makeScanner(node, notifier).start();

    const byId = Object.fromEntries(notifier.loanNotifications.map((n) => [n.loanId, n.outcome]));
    expect(byId).toEqual({ 'loan-A': 'repaid', 'loan-B': 'defaulted' });
  });

  test('correlates a LoanRepaid that precedes its LoanRequested in the same block', async () => {
    const notifier = new CapturingNotifier();
    const node = new SimulatedNode([
      block(1, [loanRepaidLog('loan-ord'), loanRequestedLog('loan-ord', 5)]),
    ]);

    await makeScanner(node, notifier).start();

    expect(notifier.loanNotifications).toHaveLength(1);
    expect(notifier.loanNotifications[0].outcome).toBe('repaid');
  });

  test('a conflicting re-request keeps the original dueBlock (first request wins)', async () => {
    const notifier = new CapturingNotifier();
    const logger = new RecordingLogger();
    const node = new SimulatedNode([
      block(1, [loanRequestedLog('loan-conf', 3)]), // original deadline: block 3
      block(2, [loanRequestedLog('loan-conf', 10)]), // conflicting re-request: block 10
      block(3, []),
      block(4, []),
    ]);

    await makeScanner(node, notifier, { logger }).start();

    // The default must fire at the ORIGINAL dueBlock 3, not the moved-out 10.
    expect(notifier.loanNotifications).toHaveLength(1);
    expect(notifier.loanNotifications[0].outcome).toBe('defaulted');
    expect(notifier.loanNotifications[0].requested.dueBlock).toBe(3);
    expect(logger.warns.some((m) => m.includes('loan-conf') && m.includes('conflicts'))).toBe(true);
  });
});

describe('outbox delivery (off the critical path)', () => {
  test('a notifier outage does not block scanning: the work is checkpointed and queued', async () => {
    const store = new InMemoryScannerStore();
    const node = new SimulatedNode(happyPath('swap-1', 1));
    // Sink is down: every delivery attempt throws.
    const down = new FlakyNotifier(Number.MAX_SAFE_INTEGER);

    // autoDrain:false → start() only scans + enqueues; it must not throw.
    const scanner = makeScanner(node, down, { store, autoDrain: false });
    await scanner.start();

    // Scanning finished and the checkpoint advanced despite the sink being down.
    expect(await store.getCheckpoint()).toBe(3);
    expect(down.notifications).toHaveLength(0);
    // The notification is durably queued in the outbox.
    expect((await store.peekOutbox()).map((e) => e.key)).toEqual(['swap:swap-1']);

    // Sink recovers; a drain (here a fresh scanner over the same store, i.e. a
    // separate delivery worker) delivers it and clears the outbox.
    const up = new CapturingNotifier();
    await makeScanner(node, up, { store }).drainOutbox();
    expect(up.notifications.map((n) => n.swapId)).toEqual(['swap-1']);
    expect(await store.peekOutbox()).toHaveLength(0);
  });

  test('a transient delivery failure is retried on a later drain, not lost', async () => {
    const store = new InMemoryScannerStore();
    const node = new SimulatedNode(happyPath('swap-1', 1));
    const notifier = new FlakyNotifier(1); // first delivery fails, then succeeds
    const scanner = makeScanner(node, notifier, { store, autoDrain: false });

    await scanner.start();
    await scanner.drainOutbox(); // attempt 1 fails
    expect(notifier.notifications).toHaveLength(0);
    expect(await store.peekOutbox()).toHaveLength(1); // still queued

    await scanner.drainOutbox(); // attempt 2 succeeds
    expect(notifier.notifications).toHaveLength(1);
    expect(await store.peekOutbox()).toHaveLength(0);
  });

  test('a permanently undeliverable notification is dead-lettered and does not block others', async () => {
    const store = new InMemoryScannerStore();
    const node = new SimulatedNode([...happyPath('swap-good', 1), ...happyPath('swap-poison', 4)]);
    const notifier = new PoisonNotifier(new Set(['swap-poison']));

    const scanner = makeScanner(node, notifier, {
      store,
      autoDrain: false,
      maxDeliveryAttempts: 3,
    });
    await scanner.start();

    // Drain three times: the good one delivers immediately; the poison one fails
    // each pass and is dead-lettered on the third.
    await scanner.drainOutbox();
    await scanner.drainOutbox();
    await scanner.drainOutbox();

    expect(notifier.notifications.map((n) => n.swapId)).toEqual(['swap-good']); // delivered once
    expect(await store.peekOutbox()).toHaveLength(0); // nothing left blocking the queue
    expect((await store.getDeadLetters()).map((e) => e.key)).toEqual(['swap:swap-poison']);
  });

  test('auto-drain (default) delivers without an explicit drain call', async () => {
    const node = new SimulatedNode(happyPath('swap-1', 1));
    const notifier = new CapturingNotifier();

    await makeScanner(node, notifier).start(); // autoDrain defaults to true

    expect(notifier.notifications.map((n) => n.swapId)).toEqual(['swap-1']);
  });

  test('loan notifications flow through the same outbox', async () => {
    const store = new InMemoryScannerStore();
    const node = new SimulatedNode([block(1, [loanRequestedLog('loan-1', 5)]), block(2, [loanRepaidLog('loan-1')]), block(3, [])]);
    const down = new FlakyNotifier(Number.MAX_SAFE_INTEGER);

    await makeScanner(node, down, { store, autoDrain: false }).start();
    expect((await store.peekOutbox()).map((e) => e.key)).toEqual(['loan:loan-1']);

    const up = new CapturingNotifier();
    await makeScanner(node, up, { store }).drainOutbox();
    expect(up.loanNotifications.map((n) => n.outcome)).toEqual(['repaid']);
    expect(await store.peekOutbox()).toHaveLength(0);
  });
});
