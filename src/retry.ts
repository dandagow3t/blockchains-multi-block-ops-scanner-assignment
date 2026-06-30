/**
 * Retry with exponential backoff and jitter.
 *
 * Node RPC calls fail transiently: timeouts, 5xx, rate limits, or a block the
 * node reports as latest but has not yet indexed. These warrant a bounded retry
 * rather than crashing or skipping the block and dropping its events.
 *
 * Jitter randomises each wait so multiple workers (sharded / multi-instance)
 * don't retry in lockstep and hammer a recovering node at the same instants.
 *
 * The original error is re-thrown once `maxRetries` is exhausted; errors are
 * never swallowed.
 */

export type JitterMode = 'full' | 'equal' | 'none';

export interface RetryOptions {
  /** Number of additional attempts after the first one. */
  maxRetries: number;
  /** Base delay in ms; raw backoff is baseDelayMs * 2^attempt before jitter. */
  baseDelayMs: number;
  /** Cap on the backoff (before jitter is applied). */
  maxDelayMs?: number;
  /**
   * Jitter applied to each wait. Defaults to 'full'.
   *   full  — wait in [0, backoff]                 (best spread)
   *   equal — wait in [backoff/2, backoff]         (spread, keeps a floor)
   *   none  — wait = backoff                        (deterministic, for tests)
   */
  jitter?: JitterMode;
  /** Hook for observability / tests. */
  onRetry?: (error: unknown, attempt: number) => void;
}

export const DEFAULT_RETRY: RetryOptions = {
  maxRetries: 5,
  baseDelayMs: 100,
  maxDelayMs: 5_000,
  jitter: 'full',
};

const sleep = (ms: number): Promise<void> =>
  ms <= 0 ? Promise.resolve() : new Promise((resolve) => setTimeout(resolve, ms));

function applyJitter(backoff: number, mode: JitterMode): number {
  switch (mode) {
    case 'none':
      return backoff;
    case 'equal':
      return backoff / 2 + Math.random() * (backoff / 2);
    case 'full':
    default:
      return Math.random() * backoff;
  }
}

export async function withRetry<T>(fn: () => Promise<T>, options: RetryOptions): Promise<T> {
  const { maxRetries, baseDelayMs, maxDelayMs = Infinity, jitter = 'full', onRetry } = options;

  let lastError: unknown;
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      if (attempt === maxRetries) break;
      onRetry?.(error, attempt + 1);
      const backoff = Math.min(baseDelayMs * 2 ** attempt, maxDelayMs);
      await sleep(applyJitter(backoff, jitter));
    }
  }
  throw lastError;
}
