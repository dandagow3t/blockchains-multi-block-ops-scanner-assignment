# Part 3 — Hardening the VaultSwap Protocol

The original protocol modelled only the happy path: a swap was *requested*,
funds were *locked*, and it *settled* (`filled` or `expired`). A real swap on a
live chain can also end without completing all three steps. This part adds the
on-chain vocabulary for those endings and updates the scanner to emit a single,
well-typed notification for every terminal state.

## The gaps in the original protocol

1. **The lock step can fail.** A vault may not have the liquidity to lock the
   requested amount. The original protocol had no event for this, so such a swap
   would sit half-correlated forever (requested, never locked, never settled)
   and never notify — a silent stuck state.
2. **A swap can be cancelled.** The user may withdraw, or the protocol may abort
   (timeout, risk check, admin action) after the request but before settlement.
   Again, no event existed, so the swap would dangle.
3. **The notification couldn't express an early ending.** `SwapNotification`
   required `requested`, `fundsLocked`, **and** `settled`. A swap that ends after
   step 1 or 2 has no `settled` event, so it was impossible to represent.

## What I added

### New events (`src/types.ts`)

```ts
interface FundsLockFailedEvent {       // the vault could not lock the funds
  type: 'FundsLockFailed';
  swapId: string;
  vault: string;
  reason: string;                      // e.g. 'insufficient_liquidity'
  blockNumber: number;
  txHash: string;
}

interface SwapCancelledEvent {         // user or protocol aborted before settle
  type: 'SwapCancelled';
  swapId: string;
  by: 'user' | 'protocol';
  reason: string;
  blockNumber: number;
  txHash: string;
}
```

Both are **terminal**: like `SwapSettled`, they put a swap into a final state.
They are grouped under a new `TerminalSwapEvent` union so the scanner can treat
"the event that ends a swap" uniformly.

### Widened outcome and a relaxed notification

```ts
type SwapOutcome = 'filled' | 'expired' | 'lock_failed' | 'cancelled';

interface SwapNotification {
  swapId: string;
  outcome: SwapOutcome;
  requested: SwapRequestedEvent;   // still REQUIRED — the correlation anchor
  fundsLocked?: FundsLockedEvent;  // now OPTIONAL
  settled?: SwapSettledEvent;      // now OPTIONAL
  terminal: TerminalSwapEvent;     // the on-chain event that ended the swap
  reason?: string;                 // why a non-filled swap ended
}
```

The shape now answers *"how does the notification change when a swap ends without
all three steps?"*:

| Outcome       | `fundsLocked` | `settled` | `terminal`     | `reason`            |
| ------------- | ------------- | --------- | -------------- | ------------------- |
| `filled`      | set           | set       | = `settled`    | —                   |
| `expired`     | set           | set       | = `settled`    | —                   |
| `lock_failed` | unset         | unset     | `FundsLockFailed` | set (why it failed) |
| `cancelled`   | set *iff* locked before cancel | unset | `SwapCancelled` | set (who/why) |

`terminal` is always present and is the single place a consumer can read the
ending's `blockNumber` / `txHash` regardless of outcome, plus — by narrowing on
its `type` — the `by` of a cancel or the `vault` of a lock failure. Without it,
early-termination notifications would be lossy: the happy path exposed the full
`settled` event, but `lock_failed` / `cancelled` would have surfaced only a
free-text `reason`, dropping the on-chain handle and the structured fields.

The scanner still emits **exactly one** notification per swap. What changed is
that the notification can now describe an early ending instead of forcing every
swap through settlement.

## Design decisions and trade-offs

### 1. `requested` stays required; everything else is optional

`SwapRequested` is the **correlation anchor**. Every terminal event carries only
a `swapId`; the request is what supplies `user`, `tokenIn/Out`, and `amountIn` —
the context a downstream consumer actually needs. A terminal event with no
observed request means we started scanning mid-swap (the classic "start at block
500, requested at 490") and can't build a useful notification, so it is dropped
with a warning rather than emitted as a contentless shell. Backfill from the
protocol-deployment block is the production fix (see `NOTES.md`).

*Trade-off:* this keeps `requested` non-optional in the type, so consumers never
have to null-check the one field that's always meaningful, at the cost of
dropping terminals that arrive without a request in our window. That drop is the
correct behaviour for a mid-swap start, and backfill — not a weaker type — is the
right way to recover the rest.

### 2. The first terminal event seals the swap

Once any terminal event is processed and notified, the swap is added to the
`notified` set and its pending state is deleted. Any **later** terminal event for
that `swapId` (a settle racing a cancel, a chain replay) or straggler partial is
then ignored as a duplicate.

This gives a deterministic answer to the cancel-vs-settle race: **whichever
terminal the scanner observes first in block order wins.** On a single canonical
chain that is also the on-chain order, which is the only order a consumer can
reason about. The alternative — letting a settle "override" a prior cancel — would
require defining a priority lattice over outcomes and make notifications
retractable; not worth it when the protocol should not emit two contradictory
terminals for one swap in the first place.

### 3. `SwapSettled` keeps a stricter rule than the early-termination events

A terminal is correlated as follows:

- **`SwapSettled`** requires `requested` **and** `fundsLocked`. A `filled` /
  `expired` notification therefore always carries its full three-step provenance.
  A settle with no observed lock signals a *real gap* (a missed `FundsLocked`
  event), so it is dropped with a warning rather than emitted with a hole.
- **`FundsLockFailed` / `SwapCancelled`** require only `requested`. For these, a
  missing lock is the *expected* shape — `lock_failed` means the lock never
  succeeded, and a cancel can land before the lock — so requiring `fundsLocked`
  would be wrong.

*Trade-off:* this asymmetry is deliberate. It preserves the invariant
"`filled`/`expired` ⇒ we saw all three steps" (strong provenance for the happy
path) while still emitting the new early-termination outcomes, which by
definition skip a step. The cost is one path (settled-without-lock) that drops
instead of emitting; in practice confirmation depth plus RPC retries make a
genuinely missed middle event rare, and a dropped-and-alerted swap is safer than
a settlement notification that silently omits provenance.

### 4. `reason` is a free-form string, surfaced verbatim

`reason` is passed through from the event rather than mapped to a scanner-side
enum. The scanner's job is correlation and delivery, not interpreting protocol
semantics; keeping the reason opaque means a new failure or cancellation reason
added on-chain flows through without a scanner change. The structured `by` on a
cancel is *not* flattened into `reason` — it stays typed on the terminal event
(reachable via `notification.terminal`), so a consumer can route on it without
parsing strings.

### 5. Unknown enum values are never coerced to a plausible-but-wrong default

`parseLog` validates the two closed-set fields instead of silently defaulting
them — defaulting an unknown value to a *valid-looking* one is the dangerous
direction in a financial system:

- **`SwapSettled.status`** — an unrecognised status is **dropped with a warning**,
  not coerced to `filled`. A bogus status silently becoming a *successful
  settlement* is the worst failure mode; dropping leaves the swap pending and
  visible instead.
- **`SwapCancelled.by`** — attribution only, so the cancel is still emitted, but
  an unrecognised value is recorded as **`'unknown'`** rather than mis-attributed
  to `'user'`.

`correlate()` also carries a compile-time exhaustiveness guard (`never`), so a
future `TerminalSwapEvent` variant that isn't handled fails the build rather than
being silently dropped at runtime.

## Scanner changes (`src/scanner.ts`)

- `parseLog` now recognises `FundsLockFailed` and `SwapCancelled`.
- The per-block dispatch routes all three terminal events to a single
  `handleTerminal`, replacing the settle-only `handleSettled`.
- `handleTerminal` keeps the Part 1 guarantees unchanged: dedupe via the
  `notified` set and at-least-once delivery (the delivery mechanism was later
  moved off the critical path into an outbox — see Part 4). A new `correlate()`
  helper builds the right notification per outcome, or returns `null` (drop +
  warn) when it can't be anchored.
- A block is now processed in two passes — record all partials, then handle all
  terminals — so correlation no longer depends on the order of logs *within* a
  block. Previously a terminal positioned before its own prerequisite in the same
  block would fail to correlate and delete the swap's anchor, losing it
  permanently.

No change was needed in the store: pending state still tracks `requested` and
`fundsLocked`; terminal events trigger emission rather than being stored.

## Tests (`tests/scanner.test.ts`)

A new `early termination (Part 3)` suite covers: lock-failure → `lock_failed`;
cancel before lock (no `fundsLocked`); cancel after lock (`fundsLocked` present);
the first-terminal-wins sealing under a cancel/settle race; the deliberate
settle-without-lock drop; an orphan terminal with no request; and restart-dedupe
for an early-termination outcome.

## Out of scope (deliberately)

- **Deep-reorg rollback** — Part 1's Level-1 confirmation-depth strategy still
  applies; un-emitting a notification for a swap a reorg invalidated needs
  retractable notifications and is the same Level-2 work described in `NOTES.md`.
- **Contradictory on-chain terminals** (e.g. `FundsLockFailed` after a successful
  `FundsLocked`) — treated as the first-terminal-wins case; the scanner does not
  attempt to police protocol-level invariants the contract should enforce.

---

# Part 4 — Second operation type: loan repayments

Loans are a 2-step operation — `LoanRequested(loanId, borrower, amount, dueBlock)`
then `LoanRepaid(loanId, borrower, amountRepaid)` — emitting a `LoanNotification`
with outcome `repaid` or `defaulted`.

## The key difference from swaps: a terminal with no event

Every swap outcome corresponds to an on-chain *event* the scanner can react to. A
loan **default** does not: it is the *absence* of a `LoanRepaid` by the deadline.
Nothing is emitted on-chain when a loan defaults — the deadline simply passes. So
the scanner has to **synthesize** that terminal from block height rather than
parse it from a log.

That drives the core design decision:

> The scanner sweeps for defaults once per processed block. After applying block
> `N`'s events, it asks the store for outstanding loans with `dueBlock <= N` and
> emits a `defaulted` notification for each. Running the sweep on *every* block —
> including logless ones — is what lets a default fire exactly when its deadline
> is reached.

Because the sweep only runs for blocks up to `safeTip` (`latest - confirmations`),
a default inherits the same finality guarantee as every other notification: it is
not emitted until its `dueBlock` is confirmation-deep, so a reorg shallower than
`confirmations` cannot retract it. The loan tests assert this directly.

## Repaid vs defaulted boundary

The spec says *repaid if `LoanRepaid` is seen **before** `dueBlock`* and
*defaulted if `dueBlock` is reached with no repayment*. Taken literally, the
deadline is exclusive:

- `LoanRepaid` in a block **`< dueBlock`** → `repaid`.
- A repayment in or after `dueBlock` is **too late**; the loan defaults. The
  repayment handler detects this and leaves the loan for the default sweep (which
  fires in the same block), rather than emitting `repaid`.

This is the strict reading. If the protocol actually intends "repay by the end of
`dueBlock`" (inclusive), it is a one-character change (`>=` → `>`) in
`handleLoanRepaid`; the boundary is called out here precisely because it is a
judgement call a reviewer should be able to flip deliberately.

## Restart-safety and idempotency

Loans reuse the Part 1 machinery, in a parallel namespace so a `loanId` can never
collide with a `swapId`:

- **Pending loans persist** (the `LoanRequested`, which carries the deadline), so a
  loan requested before a restart still defaults on time after it — verified by
  test. This is *why* the deadline can't be tracked in memory alone.
- **A notified-loan set** dedupes, so re-processing a block after a crash or replay
  never emits a second `repaid`/`defaulted`.
- **At-least-once delivery** (`notify → mark → clean up`) and **two-pass intra-block
  ordering** carry over unchanged, so a `LoanRepaid` sharing a block with its own
  `LoanRequested` still correlates.

## Notification shape

```ts
interface LoanNotification {
  loanId: string;
  outcome: 'repaid' | 'defaulted';
  requested: LoanRequestedEvent;   // anchor + carries dueBlock; always present
  repaid?: LoanRepaidEvent;        // set for repaid; absent for defaulted
}
```

A `defaulted` notification has no `repaid` event by construction — there is no
terminating transaction. The missed deadline is read from `requested.dueBlock`.

## Implementation notes

- `INotifier` gains `notifyLoan(LoanNotification)` alongside `notify`. The two
  operation types deliver through one sink here; in production they would likely
  be separate topics/partitions (keyed by `loanId` / `swapId`).
- `IScannerStore` gains a parallel set of loan methods, including
  `loansDueBy(blockNumber)`. The in-memory implementation scans outstanding loans
  linearly; production indexes on `dueBlock` (a `WHERE due_block <= $1` query or a
  min-heap) so the per-block cost is the number of loans actually coming due.
- `parseLog` rejects a `LoanRequested` whose `dueBlock` is non-integer — a bad
  deadline would mis-time every default for that loan, so it is dropped with a
  warning rather than trusted.
- **First request wins.** A re-request for a still-pending loan that carries a
  *different* `dueBlock` is a protocol anomaly (the deadline would silently move),
  so `recordLoan` keeps the original and warns instead of overwriting — the loan
  analogue of the swap path's first-terminal-wins sealing.
- **Delivery is off the critical path (transactional outbox).** Both operation
  types finalise the same way: enqueue the notification to the outbox + mark +
  clean up (all local writes), then the checkpoint advances. A separate
  `drainOutbox()` delivers, retrying per call and dead-lettering after
  `maxDeliveryAttempts`. So an undeliverable `defaulted` (or any) notification
  never stalls the sweep, the block, the head, or the rest of the queue.
  `start()` auto-drains once at the end for convenience; `autoDrain: false` runs
  delivery as a fully separate loop. See the delivery section in `NOTES.md`.

## Out of scope

- **Partial repayments / refinancing** — `LoanRepaid` is treated as full
  settlement; a multi-instalment model would track a running balance against
  `amount` and only finalise when cleared.
- **A loan whose `dueBlock` is at or before its request block** defaults
  immediately on the sweep at its request block. That is a malformed loan
  (non-future deadline); the scanner handles it gracefully rather than rejecting
  it, on the assumption the contract enforces sane deadlines.
