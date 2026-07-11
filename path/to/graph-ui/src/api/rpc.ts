/**
 * RPC API for Aider integration
 */
import { RPC } from './rpc';

interface AiderIntegration {
  indexRepository: () => void;
  searchGraph: (query: string) => void;
  tracePath: (query: string) => void;
}

class AiderRPC extends RPC {
  private aiderIntegration: AiderIntegration;

  constructor(aiderIntegration: AiderIntegration) {
    super();
    this.aiderIntegration = aiderIntegration;
  }

  indexRepository(): void {
    this.aiderIntegration.indexRepository();
  }

  searchGraph(query: string): void {
    this.aiderIntegration.searchGraph(query);
  }

  tracePath(query: string): void {
    this.aiderIntegration.tracePath(query);
  }
}

export { AiderRPC };