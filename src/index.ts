import { SimulatedNode } from './node';
import { VaultSwapScanner } from './scanner';
import { SwapNotification, INotifier } from './types';
import { HAPPY_PATH_CHAIN } from './fixtures/chain';

class ConsoleNotifier implements INotifier {
  async notify(n: SwapNotification): Promise<void> {
    console.log(`\n[NOTIFICATION] Swap ${n.swapId} — ${n.outcome.toUpperCase()}`);
    console.log(`  Requested : block ${n.requested.blockNumber}, ${n.requested.amountIn} ${n.requested.tokenIn} → ${n.requested.tokenOut}`);
    console.log(`  Locked    : block ${n.fundsLocked.blockNumber}`);
    console.log(`  Settled   : block ${n.settled.blockNumber}, out=${n.settled.amountOut}`);
  }
}

async function main() {
  const node = new SimulatedNode(HAPPY_PATH_CHAIN);
  const notifier = new ConsoleNotifier();
  const scanner = new VaultSwapScanner(node, notifier, 999);

  await scanner.start();
  console.log('\nScanner finished.');
}

main().catch(console.error);
