#!/bin/bash

# Add Aider integration to RPC API
AIDER_INTEGRATION=$(cat graph-ui/src/api/rpc.ts | grep -oP '(?<=AiderIntegration {).*?(?=})')
sed -i "s/}/\n$AIDER_INTEGRATION}/g" graph-ui/src/api/rpc.ts

# Install Aider integration
npm install @aider/ai