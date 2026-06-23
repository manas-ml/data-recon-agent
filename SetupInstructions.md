# SetupInstructions.md

End-to-end deployment guide for the Agentic Data Reconciliation app, including all fixes required for a clean environment.

---

## Prerequisites

- Databricks CLI **v1.2.1 or later** (see note below)
- Databricks workspace with Unity Catalog enabled and **serverless compute** (no classic clusters)
- Anthropic API key
- A SQL Warehouse — note its ID

---

## 1. Databricks CLI version

Versions before 1.2.1 have an expired Terraform GPG key that causes `bundle deploy` to fail with:

```
Error: error downloading Terraform: unable to verify checksums signature: openpgp: key expired
```

Upgrade before doing anything else:

```bash
brew upgrade databricks
databricks version   # must be >= 1.2.1
```

---

## 2. Clean up `~/.databrickscfg`

The CLI's `configure` command sometimes writes a `workspace_id` field that conflicts with what the Terraform provider detects from the host. Remove it if present:

```ini
# ~/.databrickscfg — correct form
[DEFAULT]
host   = https://<your-workspace>.cloud.databricks.com
token  = dapi...
# account_id is optional, workspace_id must NOT be present
```

If `workspace_id` is present, delete that line. Otherwise `bundle deploy` fails with:

```
failed to validate workspace_id: workspace_id mismatch: provider is configured
for workspace X but got Y in provider_config
```

---

## 3. Configure the CLI

```bash
databricks configure   # enter host + PAT token when prompted
databricks auth describe   # verify auth_type: pat and correct host
```

---

## 4. Create the secret scope and secrets

```bash
databricks secrets create-scope recon-agent
databricks secrets put-secret recon-agent anthropic-api-key --string-value YOUR_ANTHROPIC_KEY
databricks secrets put-secret recon-agent warehouse-id      --string-value YOUR_WAREHOUSE_ID
```

---

## 5. `databricks.yml` — serverless compute and secret resources

The workspace only supports serverless compute. All `new_cluster` blocks must be removed from jobs. Also, the app's secret resources require a `permission: READ` field or the bundle deploy will fail.

The correct `databricks.yml` shape for jobs (no cluster config at all — serverless is the default):

```yaml
tasks:
  - task_key: profiler
    notebook_task:
      notebook_path: ./notebooks/02_profiler.py
    # no new_cluster block
```

The correct app resource block with secrets:

```yaml
resources:
  apps:
    recon-agent:
      name: recon-agent
      source_code_path: .          # must be "." not "./app" — see note below
      resources:
        - name: anthropic-api-key
          secret:
            scope: recon-agent
            key: anthropic-api-key
            permission: READ       # required — omitting it causes a Terraform error
        - name: warehouse-id
          secret:
            scope: recon-agent
            key: warehouse-id
            permission: READ
```

---

## 6. `app.yaml` — correct secret reference format

`valueFrom` takes a flat resource key string (not a nested object). The old form with `secret:` or `secretKeyRef:` nested under `valueFrom` is rejected with "Error reading app.yaml file".

```yaml
# app.yaml — correct
env:
  - name: ANTHROPIC_API_KEY
    valueFrom: anthropic-api-key    # matches the resource name in databricks.yml

  - name: DATABRICKS_WAREHOUSE_ID
    valueFrom: warehouse-id
```

---

## 7. `source_code_path` must be the project root

`app.yaml` and `requirements.txt` must be present in the deployed source code directory. Setting `source_code_path: ./app` uploads only the `app/` subdirectory and excludes them, causing the app to crash on startup.

```yaml
# databricks.yml
apps:
  recon-agent:
    source_code_path: .    # project root, not ./app
```

---

## 8. Deploy the bundle and create demo tables

```bash
# Deploy bundle (uploads files + creates jobs + creates app resource)
databricks bundle deploy -t dev

# Run the setup notebook once to create main.recon_demo.orders_source/orders_warehouse
databricks jobs submit --json '{
  "run_name": "recon-agent-setup-tables",
  "tasks": [{
    "task_key": "setup",
    "notebook_task": {
      "notebook_path": "/Workspace/Users/<your-email>/.bundle/recon-agent/dev/files/notebooks/01_setup_tables",
      "base_parameters": {"catalog": "main", "schema": "recon_demo"}
    }
  }]
}'
# Note: workspace path has no .py extension
```

---

## 9. Start and deploy the app

The app must be in RUNNING state before `apps deploy` works:

```bash
databricks apps start recon-agent
databricks apps deploy recon-agent \
  --source-code-path "/Workspace/Users/<your-email>/.bundle/recon-agent/dev/files"
```

Get the URL:

```bash
databricks apps get recon-agent -o json | python3 -c "import sys,json; print(json.load(sys.stdin)['url'])"
```

---

## 10. Grant permissions to the app service principal

The app runs as its own service principal. Without these grants the `/api/tables` endpoint returns empty and the UI dropdowns are blank.

Get the service principal client ID first:

```bash
databricks apps get recon-agent -o json | python3 -c \
  "import sys,json; print(json.load(sys.stdin)['service_principal_client_id'])"
```

Then grant (replace `<sp-client-id>` and `<warehouse-id>`):

```bash
# SQL Warehouse — CAN_USE (use update-permissions, NOT set-permissions)
databricks warehouses update-permissions <warehouse-id> --json '{
  "access_control_list": [{"service_principal_name": "<sp-client-id>", "permission_level": "CAN_USE"}]
}'

# Unity Catalog — USE CATALOG
databricks grants update catalog main --json '{
  "changes": [{"principal": "<sp-client-id>", "add": ["USE CATALOG"]}]
}'

# Schema — full access needed: read for queries, write for backfill/verifier jobs
databricks grants update schema main.recon_demo --json '{
  "changes": [{"principal": "<sp-client-id>", "add": ["USE SCHEMA", "SELECT", "MODIFY", "CREATE TABLE"]}]
}'

# Jobs — CAN_MANAGE_RUN so the SP can trigger the notebook jobs
# (use update-permissions, NOT set-permissions — set-permissions removes the owner)
for JOB_ID in <profiler_job_id> <differ_job_id> <backfill_job_id> <verifier_job_id>; do
  databricks jobs update-permissions $JOB_ID --json '{
    "access_control_list": [{"service_principal_name": "<sp-client-id>", "permission_level": "CAN_MANAGE_RUN"}]
  }'
done
```

Get the job IDs after bundle deploy with:
```bash
databricks jobs list -o json | python3 -c "
import sys, json
for j in json.load(sys.stdin):
    print(j['job_id'], j['settings']['name'])
"
```

---

## 11. SQL connector credentials (code fix)

Inside Databricks Apps the SDK uses M2M OAuth, so `wc.config.token` is `None`. The `databricks-sql-connector` must use the SDK's credential provider instead of a raw token. In `app/tools.py`, the `_connect` method must read:

```python
if self.settings.databricks_token:
    token = self.settings.databricks_token
else:
    # wc.config.authenticate() returns Dict[str, str] of auth headers.
    # Works for all SDK auth types: PAT locally, M2M OAuth in Databricks Apps.
    headers = wc.config.authenticate()
    auth_header = headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise RuntimeError("SDK credential chain did not return a Bearer token")
    token = auth_header[len("Bearer "):]

return dbsql.connect(
    server_hostname=server_hostname,
    http_path=http_path,
    access_token=token,
)
```

Note: `credentials_provider=wc.config.authenticate` does NOT work — `authenticate()` is a no-arg method that returns headers, not a `HeaderFactory` callable for the SQL connector.

This fix is already applied in the codebase. If you clone into a new environment, verify it is present before deploying.

---

## 12. Job name resolution (code fix)

DAB dev-mode prefixes job names with `[dev <user>]`, e.g. the backfill job is named `[dev theoriginalzoidberg] [recon-agent] Partition Backfill`. Claude guesses names like `orders_warehouse_backfill` or `backfill_job` which don't exact-match.

The fix in `app/tools.py` `trigger_databricks_job` scores each job by the count of significant words (>3 chars) shared with the query and picks the highest scorer:

```python
def _words(s: str) -> set[str]:
    return set(re.sub(r"[^a-z0-9]+", " ", s.lower()).split())

sig_words = {w for w in _words(job_name) if len(w) > 3}
best_id, best_score = None, 0
for job in wc.jobs.list():
    if job.settings and job.settings.name:
        score = len(sig_words & _words(job.settings.name))
        if score > best_score:
            best_score, best_id = score, job.job_id
job_id = best_id if best_score > 0 else None
```

This fix is already applied. Verify `import re` is present at the top of `tools.py`.

---

## 13. Column profiler string column fix (code fix)

`TRY_AVG(col)` fails with `CAST_INVALID_INPUT` on string columns like `status` or `region`. The correct SQL uses `AVG(TRY_CAST(...))` so individual non-numeric values become NULL and are skipped by AVG.

In `app/tools.py` `run_column_profiler`, the avg line must be:

```sql
CAST(AVG(TRY_CAST(`{col}` AS DOUBLE)) AS STRING) AS avg_val
```

Not:
```sql
CAST(TRY_AVG(`{col}`) AS STRING) AS avg_val   -- fails on STRING columns
```

This fix is already applied in the codebase.

---

## 14. Flushing stale app process after deploy

`databricks apps deploy` creates a new code snapshot but does **not** kill in-flight background tasks. If an agent run was started before a deploy, it continues executing the old code. To guarantee the new code runs:

```bash
databricks apps stop recon-agent
databricks apps start recon-agent
# wait ~30s for compute to become ACTIVE, then:
databricks apps deploy recon-agent \
  --source-code-path "/Workspace/Users/<your-email>/.bundle/recon-agent/dev/files"
```

Or verify the active deployment ID matches the latest deploy before starting a new run:

```bash
databricks apps get recon-agent -o json | python3 -c "
import sys, json
d = json.load(sys.stdin)
print('active:', d.get('active_deployment', {}).get('deployment_id', 'none')[:12])
print('app state:', d.get('app_status', {}).get('state'))
"
```

---

## Quick reference — deploy from scratch

```bash
brew upgrade databricks                          # ensure >= 1.2.1
# edit ~/.databrickscfg: remove workspace_id if present
databricks configure
databricks secrets create-scope recon-agent
databricks secrets put-secret recon-agent anthropic-api-key --string-value $ANTHROPIC_KEY
databricks secrets put-secret recon-agent warehouse-id      --string-value $WAREHOUSE_ID
databricks bundle deploy -t dev
databricks jobs submit --json '{ ... 01_setup_tables ... }'   # see step 8
databricks apps start recon-agent
databricks apps deploy recon-agent --source-code-path "/Workspace/Users/<email>/.bundle/recon-agent/dev/files"
# grant SP permissions — see step 10
# after any code fix: stop → start → deploy to flush old processes (see step 14)
```
