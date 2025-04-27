# HubSpot → BigQuery ELT

**Sync your HubSpot contacts into BigQuery with true upserts, incremental fetches, dynamic schema evolution, and nightly cleanup of deleted records.**

---

## Features

- **Dynamic Schema**: Automatically adds new HubSpot contact properties as BigQuery columns.
- **Incremental Fetch**: Pulls only contacts changed since the last run, using `hs_lastmodifieddate`.
- **True Upsert**: Uses a staging table and `MERGE` to update existing rows, insert new ones, and (optionally) delete removed contacts.
- **Error-Tolerant Batching**: Streams in 200-row batches for inserts and logs row-level errors without aborting.
- **Nightly Cleanup**: Removes contacts in BigQuery that have been deleted or merged in HubSpot.

---

## Prerequisites

1. **Node.js v18+**
2. **Google Cloud Service Account** with BigQuery Data Editor rights.  
3. **HubSpot Private App Token** (CRM `contacts` scope).  
4. A BigQuery dataset (e.g. `HubSpot_Contacts`) containing `sync_tracker`:

   ```sql
   CREATE TABLE IF NOT EXISTS `PROJECT.HubSpot_Contacts.sync_tracker` (
     entity STRING,
     last_sync_timestamp TIMESTAMP
   );
   ```

---

## Setup

1. **Clone this repo**
2. **Install dependencies**:
   ```bash
   npm install
   ```
3. **Add GitHub Secrets** in Settings → Secrets:
   - `GCP_KEY` (service account JSON)
   - `BQ_PROJECT_ID`
   - `BQ_DATASET` (`HubSpot_Contacts`)
   - `HUBSPOT_TOKEN`

---

## Local Testing

```bash
# Run the ELT locally (reads .env)
npm run ELT
```

You should see logs like:

```
📡 Fetching contacts…
⚡️ Uploaded 7213 contacts across 389 columns
```

---

## GitHub Actions

### 1. Hourly ELT (`.github/workflows/ELT.yml`)

Runs every hour to sync new/updated contacts:

```yaml
name: Hourly HubSpot ELT
on:
  schedule:
    - cron: '0 * * * *'  # hourly
  workflow_dispatch:

jobs:
  run-ELT:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-node@v3
        with: { node-version: 18 }
      - run: npm install
      - uses: google-github-actions/auth@v1
        with: { credentials_json: ${{ secrets.GCP_KEY }} }
      - name: Run ELT
        env:
          HUBSPOT_TOKEN: ${{ secrets.HUBSPOT_TOKEN }}
          BQ_PROJECT_ID: ${{ secrets.BQ_PROJECT_ID }}
          BQ_DATASET:    ${{ secrets.BQ_DATASET }}
        run: node index.js
```

### 2. Nightly Cleanup (`.github/workflows/cleanup.yml`)

Runs once a night at 2 AM to remove deleted HubSpot contacts from BigQuery:

```yaml
name: Nightly HubSpot Cleanup
on:
  schedule:
    - cron: '0 2 * * *'
  workflow_dispatch:

jobs:
  cleanup:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-node@v3
        with: { node-version: 18 }
      - run: npm install
      - uses: google-github-actions/auth@v1
        with: { credentials_json: ${{ secrets.GCP_KEY }} }
      - name: Remove deleted contacts
        env:
          HUBSPOT_TOKEN: ${{ secrets.HUBSPOT_TOKEN }}
          BQ_PROJECT_ID: ${{ secrets.BQ_PROJECT_ID }}
          BQ_DATASET:    ${{ secrets.BQ_DATASET }}
        run: node cleanup.js
```

---

## Looker Studio / Reports

- **Query** the `Contacts` table in BigQuery.  
- Numeric properties (like `hs_analytics_num_page_views`, `hs_email_open`) are stored as `FLOAT`.  
- For best performance, schedule your Looker Studio data source refresh to align with the ELT runs.

---

## Troubleshooting

- **413 Request Too Large**: Lower `batchSize` in `streamToStage` (default is 200).  
- **Missing columns**: Confirm your HubSpot token has access to all required properties.  
- **Cleanup failures**: Ensure `cleanup.js` can fetch all live IDs and that the BigQuery table names match your secrets.
