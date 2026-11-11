#!/usr/bin/env bash
# Deploys the linked services, datasets and pipeline in this folder to an existing factory.
# Run from the repo root: bash infra/adf/deploy.sh
set -euo pipefail
RG=rg-vantage
DF=${DF:-adf-vantage}
cd "$(dirname "$0")"
for f in linkedService/*.json; do
  az datafactory linked-service create -g "$RG" --factory-name "$DF" -n "$(basename "$f" .json)" --properties @"$f" -o none
done
for f in dataset/*.json; do
  az datafactory dataset create -g "$RG" --factory-name "$DF" -n "$(basename "$f" .json)" --properties @"$f" -o none
done
for f in pipeline/*.json; do
  az datafactory pipeline create -g "$RG" --factory-name "$DF" -n "$(basename "$f" .json)" --pipeline @"$f" -o none
done
echo "deployed to $DF"
