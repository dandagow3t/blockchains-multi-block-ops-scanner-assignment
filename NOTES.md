# Part 1 — Implementation Notes

Scope: `VaultSwapScanner.start()` scans the chain, correlates the three
VaultSwap events by `swapId`, and emits one `SwapNotification` per swap when
`SwapSettled` arrives, without emitting duplicates after a restart.

## Architecture

```
start()                      one-shot catch-up: process checkpoint+1 .. safeTip
  └─ processBlock()          parse logs, dispatch by event type
       ├─ recordPartial()    store SwapRequested / FundsLocked as pending state
       └─ handleSettled()    correlate, notify, dedupe, clean up
```

Three pieces sit behind interfaces so the production replacement is a swap-in,
not a rewrite:

| Concern            | Here                       | Production                                                        |
| ------------------ | -------------------------- | ---------------------------------------------------------------- |
| Durable state      | `InMemoryScannerStore`     | Transactional DB (e.g. Postgres) — see below                     |
| Node flakiness     | `withRetry` (backoff)      | Same, plus circuit-breaker / multiple RPC providers / failover   |
| Notification sink  | `INotifier`                | Durable broker + transactional outbox — see below                |

## Key decisions

**One-shot catch-up, not an internal loop.** `start()` processes up to the
current safe tip and returns; a scheduler calls it repeatedly to follow the
chain. This keeps the method deterministic and easy to test, and decouples
polling cadence from scanner logic.

**State store persists everything needed for restart-safety.** Checkpoint (last
fully-processed block), pending partial swaps, and the notified-`swapId` set.

- Checkpoint means restarts resume at `checkpoint+1`; settled blocks are never
  re-scanned.
- Pending swaps must persist, or a swap requested+locked before a restart and
  settled after it could never complete.
- The notified set is a second dedupe layer that holds even if a block is
  replayed (crash recovery, reorg).

A restart is simulated in tests by constructing a new `VaultSwapScanner` with the
same store instance.

**Checkpoint advances only after a block is fully processed.** A crash mid-block
re-processes that block on restart instead of skipping the rest of its logs;
re-processing is safe because notifications are deduped by the notified set.

**Delivery is at-least-once.** `notify()` runs first, then `markNotified()`. A
crash in between re-emits one notification on restart. Losing a settlement
notification is worse than a rare duplicate. Exactly-once comes from an
idempotent notifier keyed on `swapId`, or a transactional outbox so the
notification record and the checkpoint commit in one transaction.

> Known liveness gap (Part 1). `notify()` runs synchronously on the critical
> path and `withRetry` re-throws on exhaustion, so a permanently failing
> ("poison") notification aborts the whole catch-up and stalls head progress for
> every swap behind it. The outbox fixes this too: commit only the notification
> record on the critical path (a local, reliable write) and drain it from a
> separate worker with its own retry and a dead-letter queue, so one
> undeliverable notification never blocks block processing.

**Incomplete swaps are dropped with a warning, not emitted.** A `SwapSettled`
with no stored `SwapRequested` + `FundsLocked` can't produce a complete
`SwapNotification` (those fields are required). The usual cause is starting the
scan mid-swap (start at block 500, swap requested at 490). The production fix is
backfill: on first start, scan from a known protocol-deployment block or accept a
configured backfill range. Part 1 logs and skips.

## Production technology choices

Each simplified component sits behind an interface (`IScannerStore`,
`INotifier`), so these are drop-in replacements. The reasoning carries over to
the managed equivalent on any cloud.

### Durable state → relational/transactional DB (PostgreSQL)

Here: `InMemoryScannerStore` (`Map` + `Set` behind `IScannerStore`).

Why a transactional relational store, and Postgres specifically:

1. Atomic multi-key commit is the core requirement. Crash-consistency needs
   `checkpoint` + `pending` deltas + `notified` (+ the notification outbox)
   written as one unit; otherwise notifications are lost or duplicated. One ACID
   transaction expresses "advance the checkpoint only if the work it implies is
   durable."
2. The access patterns are relational: load pending by `swapId`, batch-load a
   window with `WHERE swapId IN (...)`, expire stale pending swaps. SQL indexes
   these directly; KV stores make them awkward.
3. One engine covers two other needs: `pg_advisory_lock` provides the
   single-writer lease, and the same DB hosts the transactional outbox so the
   notification commits atomically with the state.
4. Read-after-write consistency and row locking fit a single ordered writer, with
   mature backup / PITR / replica tooling.

Alternatives:

- Redis — a cache, not a source of truth: durability is best-effort (RDB/AOF
  windows) and atomic updates across several keys need Lua. Useful later to cache
  hot pending state.
- DynamoDB / KV — strong scale and ops, but capped transactions (25 items) and a
  model that fights range scans and ad-hoc `IN` loads. The right call only after
  sharding past a single Postgres.
- SQLite / embedded (RocksDB) — fine on one node, but no network access means no
  HA and no second worker.

When this changes: a single Postgres has a write-throughput ceiling, which is the
trigger for the horizontal-sharding step below. Sharding by `swapId` scales
Postgres linearly first.

### Notification sink → durable broker (Kafka, or SQS FIFO on AWS) + outbox

Here: `INotifier` (capturing/console implementation).

Why a broker rather than a direct webhook:

1. Liveness decoupling. A direct webhook ties scanner progress to consumer
   uptime (the poison-message gap above). A broker buffers, retries, and
   dead-letters on its own.
2. Per-swap ordering and dedupe. Partition/group by `swapId` to keep a swap's
   events ordered while different swaps run in parallel, with `swapId` as the
   idempotency key for exactly-once effect on top of at-least-once delivery.
3. Replay. A retained log lets consumers rebuild without re-scanning the chain.

Which broker is situational:

- Kafka — a partitioned, retained, replayable log at high throughput with
  multiple consumer groups; partition key `swapId`. Cost: operational weight.
- SQS FIFO + SNS — managed on AWS; FIFO gives per-`MessageGroupId` (`swapId`)
  ordering and content-based dedup out of the box. Cost: lock-in, lower
  throughput ceiling, shorter dedup window.
- RabbitMQ — only for complex routing; overkill here.

With the outbox in the same Postgres, this upgrades delivery from at-least-once
to exactly-once effect.

### Single-writer coordination → partition-by-shard, or Postgres advisory lock

Here: nothing — the in-memory store is single-process.

Two instances against shared state would double-process. Options, ranked:

1. Partition the work so no lock is needed (preferred): each instance owns a
   disjoint `swapId` (or block-range) shard, so there is no shared mutable state
   to lock, and it scales out. A fencing token stops a paused-then-resumed zombie
   instance writing stale data.
2. `pg_advisory_lock` for leader election with a warm standby: it lives in the DB
   already in use, auto-releases when the session dies, and is transaction-aware.
3. etcd / ZooKeeper lease — purpose-built with fencing, but a new dependency;
   worth it only if one is already running.
4. Redis Redlock — avoided for correctness-critical mutual exclusion: unsafe
   under network partitions and clock skew. Fine for best-effort locks only.

Backstop regardless of locking: a `UNIQUE` constraint on `notified(swapId)` turns
a double-notification into a database error rather than a silent duplicate.

## Chain finality & reorgs — three levels

Reorg safety is a spectrum. This is where the implementation sits and what the
next levels are.

**Level 0 — no finality (`confirmations = 0`, the tests' default).** Treat the
tip as final. Fine for a deterministic simulated chain, unsafe on a live one
because the most recent blocks are the ones that get re-orged.

**Level 1 — confirmation depth (implemented, configurable).** `safeTip = latest -
confirmations`; only blocks that have sunk `confirmations` deep are processed, so
transient tip churn never produces a notification that has to be retracted. Set
per-chain (~12 on Ethereum, or the chain's `finalized` tag). Cheap, and removes
most reorg exposure. It does not detect a reorg, only lowers the odds of being
caught by one: a reorg deeper than `confirmations` still slips through, and a swap
whose events span the boundary can be mis-correlated.

**Level 1.5 — reorg *detection* (implemented).** The store keeps a bounded window
of recent block hashes, and before processing block `n` the scanner checks
`block.parentHash === storedHash(n-1)`. A mismatch means the chain reorged below
our safe tip (deeper than `confirmations`) or the node served an inconsistent
history — it never correlates events from an orphaned fork or emits a
notification built on a chain that no longer exists. Confirmation depth keeps
shallow reorgs from ever reaching processed blocks, so this fires only on the
dangerous, must-not-be-silent cases.

**Level 2 — bounded reorg *recovery* (implemented).** On detection the scanner
does not just halt: it walks back through the stored hash window to the **common
ancestor** (the last block whose stored hash still matches the canonical chain),
then either recovers or escalates:

- **Recover** — rewind the checkpoint to the ancestor, discard the orphaned tail
  of pending state (fields recorded from blocks `> ancestor`), and re-scan the
  canonical chain forward. Correct *because nothing was emitted from the orphaned
  range*, so there is nothing to retract. Self-healing.
- **Halt** (`ReorgDetectedError`) in the two cases recovery can't handle safely:
  the common ancestor lies outside the retained window (reorg deeper than
  `maxReorgDepth` — can't verify), or a notification was *already emitted* for a
  block past the ancestor (an orphaned emission we cannot un-notify). Halting
  leaves state consistent for operator intervention rather than guessing.

This deliberately stops short of **un-notify**: retracting an already-delivered
notification needs retractable notifications or a compensating event downstream,
which is a protocol/consumer contract beyond the scanner. Confirmation depth is
what keeps the halt case rare — a notification is only emitted once its block is
`confirmations` deep, so an emission is orphaned only by a reorg deeper than that.
`maxReorgDepth` (default 100) bounds the hash-window memory and sets how deep a
reorg can be and still auto-recover.

## Throughput: block time 10s → 400ms

At a 400ms interval the scanner must, on average, finish a block's work in under
400ms, or lag (`tip − checkpoint`) grows without bound and it falls permanently
behind.

### A throughput target, not a per-block deadline

A hard per-block deadline is the wrong model. The requirement is sustained
throughput ≥ 2.5 blocks/sec. As long as the average clears that, an occasional
slow block is absorbed by the backlog and made up on faster ones. The real budget
is `400ms × parallelism`, so pipelining is what makes a 400ms interval
comfortable.

Lag is the health signal: track `tip − checkpoint` as a gauge and alert on it. If
it trends up, throughput is below the production rate and the fix is scaling out
(last point), not micro-optimisation. An SLO such as "p99 lag < 5 blocks (2s)"
makes this concrete.

### Where the time goes

Per-block cost is almost entirely I/O round-trips, not CPU:

1. `getBlock(n)` — one network RTT per block (tens to hundreds of ms); a single
   sequential RTT can exceed the budget. The dominant cost.
2. Store I/O — in production each checkpoint / pending / notified write is a DB
   RTT; the per-event `getPending` + `isNotified` is an N+1 pattern.
3. Notifier I/O — a remote call per settlement.
4. Parsing — negligible.

The rest is about removing or amortising round-trips, in rough order of impact.

1. **Fetch by log-range, not block-by-block.** Replace N `getBlock` calls with one
   server-side-filtered range query (the `eth_getLogs(fromBlock, toBlock,
   address, topics)` equivalent): one RTT returns only VaultSwap events across a
   window and transfers far less data. The biggest single win. Interface gap:
   `IBlockchainNode` only exposes `getBlock`; production would add a
   `getLogs(range, filter)` method.

2. **Pipeline fetch ahead of processing.** Keep a bounded window of in-flight
   fetches (`n … n+K`) running concurrently but commit in order, so correlation
   and the checkpoint stay monotonic. With 200ms fetches and 10 in flight,
   effective throughput is ~20ms/block. This is what makes the `400ms ×
   parallelism` budget real; the per-block correlation logic is unchanged.

3. **Batch state writes.** Process a window, then write the new checkpoint +
   pending deltas + notified in one transaction. Fewer DB RTTs; a crash replays
   up to one batch (safe — deduped), so batch size tunes the latency-vs-replay
   trade.

4. **Remove the per-event N+1.** Collect a window's `swapId`s and load them in one
   query (`WHERE swapId IN (...)`), work in memory, bulk-write back: two
   round-trips per window instead of two-plus per event. The in-memory store
   already behaves this way.

5. **Take the notifier off the critical path.** Write notifications to a durable
   outbox in the same transaction as the state commit and drain them from a
   separate worker. Per-block latency then depends on a local DB write, not an
   external service, and delivery becomes exactly-once.

6. **Subscribe instead of poll.** Interval-polling `getLatestBlockNumber` adds
   overhead and discovery latency. A push subscription (`newHeads` / log
   subscription over websocket) delivers blocks as they are produced.

   The caveat sets a design rule: a websocket is best-effort and can silently
   miss blocks (drops, reconnects, node buffer overflow, events during a brief
   disconnect), so correctness must not depend on it. Correctness comes instead
   from the contiguous integer checkpoint: block `N+1` is processed only after
   `N`, and every pass handles the whole range `checkpoint+1 … tip`, so skipping a
   block number is impossible regardless of what the socket delivers; a dropped
   notification only delays when the tip is learned.

   That separates two concerns: skipping a block number (the checkpoint prevents
   it) and being briefly behind the true tip (a latency bound set by the polling
   interval, not a correctness bug). The polling/catch-up loop is the correctness
   floor; the subscription is a latency accelerator on top. The test for using a
   subscription at all: remove it and correctness should still hold (latency just
   rises). Here it does. A design that produces gaps without the socket — one that
   processes "the block the event named" rather than "everything up to the tip" —
   makes the socket load-bearing for correctness and should not use it. Checkpoint
   for correctness, socket for latency.

7. **Faster blocks make confirmation depth cheaper.** At 400ms, 12 confirmations
   is 4.8s of latency. Processing slightly behind the tip also adds slack, which
   makes pipelining safer, so a faster chain argues for a healthy `confirmations`
   value rather than against it.

8. **Scale horizontally when a single writer can't keep up.** Shard by `swapId`
   (hash-partition) across N instances, each owning a disjoint range; the range
   fetch is shared or each shard filters its own. This needs the
   single-writer-per-partition / lease design above plus DB-level dedupe to stay
   correct across rebalances.

The per-block path here is a few synchronous awaits, which is fine for a simulated
chain. Surviving 400ms blocks changes the shape to: stream ranges → pipelined
fetch → in-memory correlation over a window → batched transactional commit →
async outbox delivery, with lag as the signal for when to scale.

## Edge cases handled

- Duplicate events (same event twice, e.g. replay): pending writes are idempotent
  (overwrite); a second `SwapSettled` is ignored via the notified set.
- Out-of-order partials within range (`FundsLocked` before `SwapRequested`):
  whichever arrives first is stored; correlation only needs both present by settle
  time.
- Transient node errors: retried with exponential backoff; exhaustion re-throws
  so the run fails loudly instead of dropping a block.
- Already caught up: `start()` is a no-op when `safeTip <= checkpoint`.

## Edge cases not handled in Part 1

- Full reorg rollback (Level 2 above).
- Backfilling events emitted before the configured start block.
- Failed / cancelled / expired-without-all-steps swaps — Part 3, which may extend
  `types.ts`.
- Concurrent scanner instances: the in-memory store is single-process; two would
  double-process. Production uses a single-writer lease / advisory lock or
  partitioned ranges, plus DB-level dedupe.

## Known gaps (review follow-ups)

- `startBlock` is exclusive (scanning begins at `startBlock + 1`), so the named
  block is skipped — inclusive semantics or a rename would be clearer.
- No reaping/TTL for `pending` or `notified`, and stuck swaps surface only as a
  `warn` — production needs a pending-age metric / stale sweep.
- A `SwapSettled` missing `FundsLocked` (or with a bad `status`) is dropped with a
  warn — a lost real settlement deserves a louder signal (alert / dead-letter).
- Reorg handling auto-recovers (Level 2, bounded): a `parentHash` break rewinds to
  the common ancestor and re-scans. It still halts (`ReorgDetectedError`) when the
  reorg is deeper than `maxReorgDepth` or orphaned an already-emitted notification
  (un-notify is out of scope — needs a retractable-notification contract).
- Most tests run `confirmations: 0` (Level 0), which is the unsafe-on-a-live-chain
  default; the confirmation-boundary correlation case isn't pinned by a test.
- `txHash` is read from `log.args`; against a real node it's receipt metadata, so
  `terminal.txHash` would be empty without an adapter change.
