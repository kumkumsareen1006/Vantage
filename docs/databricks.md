# Databricks job (Phase 3, Days 21-22)

The batch pipeline's Silver and Gold stages run as an Azure Databricks job against the
Delta tables on ADLS Gen2. Bronze stays as landed on Day 20: the raw pageview `.gz` files
only exist on the laptop, and Bronze on ADLS already holds all 72 hours.

## What runs

| | |
|---|---|
| Job | `vantage-batch`, defined in [`databricks.yml`](../databricks.yml) (Databricks Asset Bundle) |
| Code | the repo, synced to the workspace by `databricks bundle deploy` (git-ignored paths excluded) |
| Entry point | `scripts/run_pipeline.py --target azure --stages silver,gold` (a Python script task, no notebook). Day 21 ran it with `--full`. |
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

**Fix, verified (2026-11-10).** The job cluster now sets `JNAME: zulu17-ca-amd64`.
Silver was restored to version 2 (the Day 20 state: 142,728,341 rows, 562,648,784
views). Then one day was re-run on the cluster:

```bash
databricks bundle run vantage_batch \
  --python-params="--target,azure,--stages,silver,--days,2026-10-15,--full"
```

- The run logged `java_version 17.0.20` and `upper('ს')` = `U+1CA1`, the same as the laptop.
- Time: setup 322 s, Silver 1,395 s (24 files, 50,420,726 rows in, 50,417,645 out, 0 quarantined).
- The 2026-10-15 partition in the new version (v7) matches v2 exactly: 50,417,645 rows,
  189,432,040 views, and an `exceptAll` on `(project, title, hour, views)` of 0 in
  both directions.

## Scheduled run (Day 22, 2026-11-10)

The schedule fired the job once: Quartz cron `0 21 17 10 11 ? 2026`, timezone
America/New_York. The year in the cron stops it from firing again. Development mode pauses
schedules unless they say `pause_status: UNPAUSED`, so the bundle stayed in development mode
with that set. The run's job cluster carried the tag `vantage_run=day22-scheduled` so its cost
can be found later. The schedule was removed from `databricks.yml` and redeployed right after
the run.

- Run 636280560090751, trigger `PERIODIC` (the schedule), started 17:21:20 EST, result SUCCESS.
- Time: setup 323 s, execution 869 s, run 1,193 s. Silver 63.0 s (0 new files, 0 rows),
  Gold 757.4 s, built from Silver version 7.
- Silver: 142,728,341 rows, 0 duplicate keys, 562,648,784 views.
- Gold: 84,348 rows (7,200 + 300 + 76,848), 2,027 `is_spike`, 7 `likely_automated`.
  Gold spikes `exceptAll` against the local Gold spikes gives 0 rows in both directions.

Cost view for 2026-11-10, read after the run (runs 1-3; the scheduled run lags behind):
VM (E4ds v4) $0.7152, Premium Jobs Compute DBU $0.6441, storage $0.0498, public IP $0.0159,
bandwidth $0.0005, total $1.4255. With secure cluster connectivity off, each job cluster
node gets a public IP for the run. That IP is the public IP meter.

### Gotchas hit on the way

- A Python script task is `exec()`'d inside a kernel, so `__file__` is not set.
  `run_pipeline.py` falls back to `sys.argv[0]`. It also returns normally on success
  instead of calling `sys.exit(0)`. The first run failed on this after 414 s of setup
  and 39 s of execution, before touching any table.
- The trial workspace tier is refused on Azure Free Trial subscriptions.
- The workspace API answers "Did not find workspace" until it's opened once in the portal.
