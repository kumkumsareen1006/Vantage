#!/usr/bin/env bash
# Deletes the whole rg-vantage resource group AND the vantage-sp app registration
# (an AAD app lives in the directory, not the resource group, so deleting the group
# does not remove it). Pay-as-you-go subscription: this stops all Azure billing for
# the project. Asks for explicit confirmation first. Run it yourself; nothing else does.
set -euo pipefail

RG="rg-vantage"
SP_NAME="vantage-sp"

if ! command -v az >/dev/null 2>&1; then
  echo "az CLI not found. Install it first (brew install azure-cli)." >&2
  exit 1
fi

echo "Subscription: $(az account show --query name -o tsv)"
echo "About to DELETE resource group '$RG' and everything in it:"
az resource list -g "$RG" -o table 2>/dev/null || echo "  (could not list; group may already be gone)"
APPID="$(az ad app list --display-name "$SP_NAME" --query "[0].appId" -o tsv 2>/dev/null || true)"
echo "And app registration '$SP_NAME' (appId: ${APPID:-not found})"
echo
read -r -p "Type the group name ($RG) to confirm deletion: " reply
if [ "$reply" != "$RG" ]; then
  echo "Aborted; nothing deleted."
  exit 1
fi

az group delete --name "$RG" --yes
echo "Resource group delete request submitted."

if [ -n "${APPID:-}" ]; then
  az ad app delete --id "$APPID"
  echo "App registration $SP_NAME deleted."
fi

echo "Confirm with: az group exists -n $RG  (expect false)"
# the Databricks workspace's managed group goes when the workspace does, a few minutes later
echo "          and: az group exists -n ${RG}-dbx-managed  (expect false)"
echo "Local credential file ~/.config/vantage/azure_sp.json is left for you to remove."
