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
# workspace: secure cluster connectivity off so there's no NAT gateway. Premium, because
# the 14-day trial tier is refused on Azure Free Trial subscriptions.
az databricks workspace create -g rg-vantage -n dbw-vantage -l eastus --sku premium \
  --managed-resource-group rg-vantage-dbx-managed --enable-no-public-ip false \
  --tags project=vantage

# Open the workspace once from the portal (Launch Workspace). Until then every API call
# returns "Did not find workspace with specified org ID".

# CLI profile using the existing `az login` (no personal access token). azure-cli auth
# fails with "load tenant id" unless the resource id and tenant are set.
cat >> ~/.databrickscfg <<EOF
[vantage]
host      = https://$(az databricks workspace show -g rg-vantage -n dbw-vantage --query workspaceUrl -o tsv)
auth_type = azure-cli
azure_workspace_resource_id = $(az databricks workspace show -g rg-vantage -n dbw-vantage --query id -o tsv)
azure_tenant_id = $(az account show --query tenantId -o tsv)
EOF

# secret scope, values piped from the SP file so they never hit the terminal
databricks secrets create-scope vantage -p vantage
for k in client_id client_secret tenant_id; do
  python -c "import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]], end='')" \
    ~/.config/vantage/azure_sp.json $k | databricks secrets put-secret vantage $k -p vantage
done

databricks bundle validate
databricks bundle deploy
databricks bundle run vantage_batch      # billed: VM + DBUs
```

## Teardown

Deleting the workspace deletes `rg-vantage-dbx-managed` with it. ADLS is untouched:

```bash
az databricks workspace delete -g rg-vantage -n dbw-vantage --yes
```

`scripts/azure_teardown.sh` (deletes all of `rg-vantage`) covers the workspace too.

## Results (first successful run, 2026-11-10)

Times from the Jobs API (setup, execution) and `run_metrics` (stages):

| | Local (Day 11) | Laptop → ADLS (Day 20) | Databricks, 1 × E4ds_v4 |
|---|---|---|---|
| Cluster setup | – | – | 352 s |
| Silver `--full` | 1,521 s | 1,852 s | 5,044 s |
| Gold | 369 s | 620 s | 732 s |
| Job execution | – | – | 5,820 s |

The single 4-vCPU node (the subscription's whole regional quota) is slower than the
laptop. Each Silver day's MERGE took about 25-28 minutes on the cluster against about
10 on the laptop.

Gold matched Day 20 exactly: 84,348 rows (7,200 hourly + 300 daily + 76,848 spikes),
2,027 `is_spike`, 7 `likely_automated`. Delta protocol was unchanged on every table
(reader 1, writer 2, no deletion vectors), and the laptop still reads all of it.
Databricks' optimized writes left Silver at 11 files, down from 202.

**Silver did not match.** It ended at 142,728,536 rows (Day 20: 142,728,341) with 0
duplicate keys. The cause is title normalization's first-letter uppercase:

- On Java 17 (laptop, CI), `upper('ს')` gives Georgian Mtavruli `Ს` (U+1CA1).
- DBR 15.4 runs Java 8 by default, whose older Unicode tables leave the letter unchanged.
- So 195 rows whose title starts with a Georgian letter got a different key and were
  inserted next to the Day 20 rows (MERGE never deletes). 190 of the 195 start with a
  lowercase Georgian letter.
- Collisions merged: 8,675 here against 8,678 on Day 20.

The fix is to run the cluster on Java 17 so normalization matches the laptop.

### Gotchas hit on the way

- A Python script task is `exec()`'d inside a kernel, so `__file__` is not set.
  `run_pipeline.py` falls back to `sys.argv[0]`. It also returns normally on success
  instead of calling `sys.exit(0)`. The first run failed on this after 414 s of setup
  and 39 s of execution, before touching any table.
- The trial workspace tier is refused on Azure Free Trial subscriptions.
- The workspace API answers "Did not find workspace" until it's opened once in the portal.
