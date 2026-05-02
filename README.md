# Take-Home Assignment: Multi-Block Operation Scanner

## Background

At scale, blockchain operations often span multiple transactions across multiple blocks. A DeFi swap, for example, might require one transaction to request the swap, another to lock funds, and a third to settle — each may land in a different block, minutes or even hours apart.

Your task is to implement a scanner that monitors a live blockchain, correlates these multi-step operations by a shared ID, and emits a notification when an operation reaches a final state.

This sounds straightforward. The interesting part is everything that can go wrong !!!

---

## The Protocol: VaultSwap

VaultSwap is a fictional swap protocol. Every swap emits exactly three on-chain events, tied together by a `swapId`:

| Step | Event | Description |
|------|-------|-------------|
| 1 | `SwapRequested(swapId, user, tokenIn, tokenOut, amountIn)` | User initiates a swap |
| 2 | `FundsLocked(swapId, vault, amountIn)` | A vault locks the funds |
| 3 | `SwapSettled(swapId, amountOut, status)` | Swap resolves — `status` is `"filled"` or `"expired"` |

All events are emitted by the contract at address `0xVaultSwap`.

---

## What You're Given

```
src/
  types.ts      — Interfaces for blocks, events, and notifications (do not modify)
  node.ts       — SimulatedNode: an in-memory blockchain node you can use in tests
  scanner.ts    — VaultSwapScanner skeleton — implement the start() method
  fixtures/
    chain.ts    — A sample happy-path chain for the main runner
  index.ts      — Entry point
tests/
  scanner.test.ts    — VaultSwapScanner tests skeleton — implement the required test suite based on your implementation
```

The `IBlockchainNode` interface is your only access to the chain:

```typescript
interface IBlockchainNode {
  getLatestBlockNumber(): Promise<number>;
  getBlock(blockNumber: number): Promise<Block>;
}
```

`SimulatedNode` implements this interface using an in-memory array of blocks, which you can use freely in your tests to construct any chain scenario.

---

## Part 1: Implement the Scanner

Implement `VaultSwapScanner.start()` in `src/scanner.ts`.

The scanner must:
- Implement a blockchain scanning process
- Correlate all 3 events for the same `swapId` and emit a `SwapNotification` when `SwapSettled` arrives
- **Not emit duplicate notifications** if restarted
- Handle the realities of working against a blockchain node !!!

You may add fields, helper classes, or new files as you see fit. Do not modify `src/types.ts` or `src/node.ts`.

Aim for a production-grade implementation, one you would be comfortable deploying to a live system.  
While a fully production-ready solution would require more extended implementation and infrastructure that is out of scope here (databases, message queues, distributed locks, etc), you may use a simplified equivalent, as long as you explicitly state the assumption and describe what you would use in production and why.  
For example: To allow persistent restart-safe state I would normally store the state in database of type X, and the reason I would go with database X is . . ., while for this assignment, I would use an in-memory `Map` with proper interface.

---

## Part 2: Write Tests

Enhance `tests/scanner.test.ts` and write a test suite for your implementation.

Use `SimulatedNode` to construct whatever chain state you need for each scenario.

We will read your tests as carefully as your implementation. The scenarios you choose to test, and the ones you don't, tell us how you think about correctness in a production blockchain system.

---

## Part 3: Harden the Protocol

The original author of the VaultSwap protocol made some naive assumptions. The current protocol only models the happy path, it has no way to express issues that can happen is a real world live blockchain, or a real world finance platform.

Your task is to identify the gaps, extend the protocol, and update your scanner implementation to handle the new cases.

Specifically, consider:
- What happens if the funds locking step fails (e.g. the vault has insufficient liquidity)?
- What happens if the user or the protocol needs to cancel a swap after it has been requested, but before it settles?
- How does your `SwapNotification` change when a swap ends without going through all three steps?

You are free to modify `src/types.ts` in this part only. Define any new events and update the notification type as you see fit. Document your design decisions in `PROTOCOL.md` — explain what you added, why, and what trade-offs you considered.

Update your scanner implementation and your tests to cover the new cases.

---

## Part 4: Add a Second Operation Type

The protocol also supports loan repayments — a 2-step operation:

| Step | Event | Description |
|------|-------|-------------|
| 1 | `LoanRequested(loanId, borrower, amount, dueBlock)` | Borrower requests a loan |
| 2 | `LoanRepaid(loanId, borrower, amountRepaid)` | Borrower repays |

A loan is `repaid` if `LoanRepaid` is seen before `dueBlock`.
A loan is `defaulted` if `dueBlock` is reached with no repayment.

Extend the scanner to handle this operation type and emit a `LoanNotification`. Add tests.

---

## Running the Project

```bash
npm install
npm test        # run your tests
npm start       # run against the sample fixture chain
```

---

## What We'll Discuss

In the follow-up interview we'll go through your submission and ask you to explain your decisions:

- What edge cases did you consider? Which did you decide not to handle, and why?
- How does your scanner behave if the node is unreliable?
- How does your implementation handle chain reorganisations?
- What happens if the scanner starts from block 500, but a swap was initiated at block 490?
- What if two instances of your scanner run in parallel?
- How would your design change if block time dropped from 10 seconds to 400ms?

---

## Submission Format

Each part must be submitted as a dedicated Git commit tagged with the part number. We will review your work part by part — checking out each tag, reading the implementation, then moving to the next to see what changed and why.

```bash
# After completing each part, commit and tag:
git add .
git commit -m "Part 1: implement scanner"
git tag part-1

git add .
git commit -m "Part 2: write tests"
git tag part-2

git add .
git commit -m "Part 3: harden protocol"
git tag part-3

git add .
git commit -m "Part 4: add loan operation"
git tag part-4
```

Submit by sharing your repository (GitHub, GitLab, or a zip including the `.git` folder).

---

## Constraints

- TypeScript only
- No external libraries beyond what is already in `package.json`
- Do not modify `src/types.ts` or `src/node.ts` (except in Part 3, where you are explicitly asked to)
- You may add new files freely

**Time budget:** ~3–4 hours. Depth over breadth — a well-reasoned partial solution is better than a shallow complete one.
