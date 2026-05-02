import { Block, IBlockchainNode } from './types';

/**
 * SimulatedNode loads a chain fixture from a JS object.
 * In tests you can pass any chain you construct.
 * In the main runner it loads from src/fixtures/chain.json.
 */
export class SimulatedNode implements IBlockchainNode {
  private blocks: Map<number, Block>;
  private latest: number;

  constructor(blocks: Block[]) {
    this.blocks = new Map(blocks.map((b) => [b.number, b]));
    this.latest = blocks.length > 0 ? Math.max(...blocks.map((b) => b.number)) : 0;
  }

  async getLatestBlockNumber(): Promise<number> {
    return this.latest;
  }

  async getBlock(blockNumber: number): Promise<Block> {
    const block = this.blocks.get(blockNumber);
    if (!block) throw new Error(`Block ${blockNumber} not found`);
    return block;
  }
}
