#!/usr/bin/env bash
# Deletes the whole rg-vantage resource group (every Vantage Azure resource).
# Pay-as-you-go subscription: this stops all Azure billing for the project.
# Asks for explicit confirmation first. Run it yourself; nothing else does.
set -euo pipefail

RG="rg-vantage"

if ! command -v az >/dev/null 2>&1; then
  echo "az CLI not found. Install it first (brew install azure-cli)." >&2
  exit 1
fi

echo "Subscription: $(az account show --query name -o tsv)"
echo "About to DELETE resource group '$RG' and everything in it:"
az resource list -g "$RG" -o table 2>/dev/null || echo "  (could not list; group may already be gone)"
echo
read -r -p "Type the group name ($RG) to confirm deletion: " reply
if [ "$reply" != "$RG" ]; then
  echo "Aborted; nothing deleted."
  exit 1
fi

az group delete --name "$RG" --yes
echo "Delete request submitted. Confirm with: az group exists -n $RG  (expect false)"
