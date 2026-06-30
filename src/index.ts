import { SimulatedNode } from './node';
import { VaultSwapScanner } from './scanner';
import { SwapNotification, INotifier } from './types';
import { HAPPY_PATH_CHAIN } from './fixtures/chain';

class ConsoleNotifier implements INotifier {
  async notify(n: SwapNotification): Promise<void> {
    console.log(`\n[NOTIFICATION] Swap ${n.swapId} — ${n.outcome.toUpperCase()}`);
    console.log(`  Requested : block ${n.requested.blockNumber}, ${n.requested.amountIn} ${n.requested.tokenIn} → ${n.requested.tokenOut}`);
    if (n.fundsLocked) console.log(`  Locked    : block ${n.fundsLocked.blockNumber}`);
    if (n.settled) console.log(`  Settled   : block ${n.settled.blockNumber}, out=${n.settled.amountOut}`);
    if (n.reason) console.log(`  Reason    : ${n.reason}`);
  }
}

async function main() {
  const node = new SimulatedNode(HAPPY_PATH_CHAIN);
  const notifier = new ConsoleNotifier();
  // startBlock is inclusive; the fixture's first block is 1000.
  const scanner = new VaultSwapScanner(node, notifier, 1000);

  await scanner.start();
  console.log('\nScanner finished.');
}

main().catch(console.error);
