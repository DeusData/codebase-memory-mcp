# Conventions

## Aider Integration

To use Aider with codebase-memory-mcp, follow these steps:

1. Install the Aider integration by running `npm install @aider/ai`
2. Update the `graph-ui/src/api/rpc.ts` file to include the Aider integration
3. Run the installation script to complete the setup

## RPC API

The RPC API for Aider integration is defined in `graph-ui/src/api/rpc.ts`. It provides the following methods:

* `indexRepository()`: Indexes the repository
* `searchGraph(query: string)`: Searches the graph for a query
* `tracePath(query: string)`: Traces a path in the graph for a query