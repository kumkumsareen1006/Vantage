# Databricks job (Phase 3, Day 21)

The batch pipeline's Silver and Gold stages run as an Azure Databricks job against the
Delta tables on ADLS Gen2. Bronze stays as landed on Day 20: the raw pageview `.gz` files
only exist on the laptop, and Bronze on ADLS already holds all 72 hours.

## What runs

| | |
|---|---|
| Job | `vantage-batch`, defined in [`databricks.yml`](../databricks.yml) (Databricks Asset Bundle) |
| Code | the repo, synced to the workspace by `databricks bundle deploy` (git-ignored paths excluded) |
| Entry point | `scripts/run_pipeline.py --target azure --stages silver,gold --full` (a Python script task, no notebook) |
| Compute | job cluster, single node `Standard_E4ds_v4` (4 vCPU, 32 GB), DBR 15.4 LTS, Photon off |
| Guards | `timeout_seconds: 7200`, `max_retries: 0`, `max_concurrent_runs: 1`; the cluster exists only for the run |
| Storage auth | service principal `vantage-sp` (Storage Blob Data Contributor on the storage account), read from Databricks secret scope `vantage` |

The subscription's eastus quota is 4 vCPUs in total, which is why the cluster is one node.

## How the code tells local from Databricks

`vantage/spark.py` checks `DATABRICKS_RUNTIME_VERSION`:

- **Local:** builds a session with the pip Delta jars, `hadoop-azure` and the SP from
  `~/.config/vantage/azure_sp.json`, as on Day 20.
- **Databricks:** reuses the cluster's session (Delta is built in), sets the same ABFS OAuth
  conf from the secret scope, and turns deletion vectors off for new tables so the local
  delta-spark 3.3.3 can still read anything the job creates.

Each pipeline stage stops its session locally (stage isolation, see Day 11). On Databricks
`release()` leaves the session alone, since stopping it would end the job.

## Setup

```bash
# workspace: trial tier, secure cluster connectivity off so there's no NAT gateway
az databricks workspace create -g rg-vantage -n dbw-vantage -l eastus --sku trial \
  --managed-resource-group rg-vantage-dbx-managed --enable-no-public-ip false \
  --tags project=vantage

# CLI profile using the existing `az login` (no personal access token)
cat >> ~/.databrickscfg <<EOF
[vantage]
host      = https://$(az databricks workspace show -g rg-vantage -n dbw-vantage --query workspaceUrl -o tsv)
auth_type = azure-cli
EOF

# secret scope, values piped from the SP file so they never hit the terminal
databricks secrets create-scope vantage -p vantage
for k in client_id client_secret tenant_id; do
  python -c "import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]], end='')" \
    ~/.config/vantage/azure_sp.json $k | databricks secrets put-secret vantage $k -p vantage
done

databricks bundle validate
databricks bundle deploy
databricks bundle run vantage_batch      # billed: VM + DBUs (DBUs waived on trial)
```

## Teardown

Deleting the workspace deletes `rg-vantage-dbx-managed` with it. ADLS is untouched:

```bash
az databricks workspace delete -g rg-vantage -n dbw-vantage --yes
```

`scripts/azure_teardown.sh` (deletes all of `rg-vantage`) covers the workspace too.

## Results

Filled in from the first job run: setup and execution time from the Jobs API, stage
seconds and row counts from `run_metrics`, compared with the local and laptop-to-ADLS runs.
