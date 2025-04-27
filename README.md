# HubSpot → BigQuery ETL

This repository contains a Node.js script (`index.js`) that synchronizes HubSpot contacts into BigQuery, with true upsert behavior via a staging table and merge.

## Features

- **Dynamic schema**: Automatically discovers HubSpot contact properties and evolves the BigQuery table schema.
- **Incremental sync**: Tracks `hs_lastmodifieddate` to only fetch changed contacts since the last run.
- **Staging + MERGE**: Streams updates to a staging table, then performs a BigQuery `MERGE` to upsert into the master table.
- **Error handling**: Batches inserts, logs row-level errors, and continues.
- **Automated**: Scheduled via GitHub Actions to run hourly.

---

## Prerequisites

- Node.js v18+
- A Google Cloud project with BigQuery enabled
- A BigQuery dataset (e.g. `Thrive_HubSpot`) and two tables: `Contacts` and `sync_tracker` (created automatically)
- HubSpot private app token with `crm.objects.contacts.read` scope

## Setup

1. **Clone** this repo:
   ```bash
   git clone https://github.com/your-org/mdo-etl.git
   cd mdo-etl
   ```

2. **Install dependencies**:
   ```bash
   npm install
   ```

3. **Environment variables**: Create a `.env` file with:
   ```ini
   HUBSPOT_TOKEN=your_hubspot_private_app_token
   BQ_PROJECT_ID=your_gcp_project_id
   BQ_DATASET=your_bigquery_dataset
   BQ_TABLE=Contacts
   ```

4. **GitHub secrets** (if using Actions):
   - `HUBSPOT_TOKEN`
   - `GCP_KEY` (service account JSON)
   - `BQ_PROJECT_ID`
   - `BQ_DATASET`
   - `BQ_TABLE`

---

## Usage

Run locally:
```bash
node index.js
```

Or via GitHub Actions (hourly):
```yaml
on:
  schedule:
    - cron: '0 * * * *'
jobs:
  run-etl: ...
```

After each run, the `Contacts` table in BigQuery will be upserted with the latest HubSpot contact data.

---

## Troubleshooting

- **413 Request Entity Too Large**: Reduce batch size in `streamToStage()`.
- **Schema errors**: Ensure the BigQuery tables exist or let the script auto-create them.
- **Authentication**: Verify your `GCP_KEY` service account has BigQuery Data Editor rights.

---

## License

MIT © MDO Holdings
