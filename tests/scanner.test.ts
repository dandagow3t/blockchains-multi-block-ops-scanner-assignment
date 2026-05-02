import { VaultSwapScanner } from '../src/scanner';
import { SimulatedNode } from '../src/node';
import { Block, INotifier, SwapNotification } from '../src/types';

// ── Test helpers ──────────────────────────────────────────────────────────────

class CapturingNotifier implements INotifier {
  public notifications: SwapNotification[] = [];
  async notify(n: SwapNotification): Promise<void> {
    this.notifications.push(n);
  }
}

// ── Your tests go here ────────────────────────────────────────────────────────

test('example - replace this with your own tests', async () => {
  const notifier = new CapturingNotifier();
  const node = new SimulatedNode([
    // Build your chain here using Block objects.
    // Each block has: number, hash, parentHash, logs[]
    // Each log has: address, event, args
    // Use '0xVaultSwap' as the contract address.
  ]);

  const scanner = new VaultSwapScanner(node, notifier, 0);
  await scanner.start();

  // expect(notifier.notifications).toHaveLength(...);
});
