# HubSpot → BigQuery ETL

This ETL syncs your HubSpot `contacts` into BigQuery every hour, performing true upserts so that any updated properties (including numeric fields like `hs_analytics_num_page_views` and `hs_email_open`) are reflected without duplicates.

---

## Features

- **Dynamic schema**: Automatically adds new HubSpot properties as BigQuery columns.
- **Incremental fetch**: Only fetches contacts modified since the last run.
- **True upsert**: Uses a staging table + `MERGE` to update existing rows and insert new ones.
- **Error-tolerant batching**: Streams data in batches and logs partial failures without aborting the entire load.

---

## Prerequisites

1. **Node.js v18+**
2. **Google Cloud service account** with BigQuery Data Editor rights.
3. **HubSpot private app token** with CRM `contacts` scope.
4. A BigQuery dataset (`Thrive_HubSpot`) with an existing `sync_tracker` table:

   ```sql
   CREATE TABLE IF NOT EXISTS `PROJECT.Thrive_HubSpot.sync_tracker` (
     entity STRING,
     last_sync_timestamp TIMESTAMP
   );
   ```

---

## Setup

1. **Clone this repo**
2. **Install dependencies**
   ```bash
   npm install
   ```
3. **Add repository secrets** in GitHub Settings → Secrets:
   - `GCP_KEY` (service account JSON)
   - `BQ_PROJECT_ID`
   - `BQ_DATASET` (`Thrive_HubSpot`)
   - `HUBSPOT_TOKEN`

---

## Local Testing

```bash
# Load environment variables from your .env file
npm run etl
```

You should see logs indicating how many contacts were fetched and upserted.

---

## GitHub Actions

The workflow `etl.yml` is configured to run hourly:

```yaml
on:
  schedule:
    - cron: '0 * * * *'
  workflow_dispatch:

jobs:
  run-etl:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-node@v3
        with:
          node-version: 18
      - run: npm install
      - uses: google-github-actions/auth@v1
        with:
          credentials_json: ${{ secrets.GCP_KEY }}
      - run: npm run etl
        env:
          HUBSPOT_TOKEN: ${{ secrets.HUBSPOT_TOKEN }}
          BQ_PROJECT_ID: ${{ secrets.BQ_PROJECT_ID }}
          BQ_DATASET:    ${{ secrets.BQ_DATASET }}
```

---

## Querying in Looker Studio

- Use the `Contacts` table in your BigQuery dataset.
- Numeric properties (e.g. `hs_analytics_num_page_views`) are stored as `FLOAT` and will display zero correctly.
- Schedule a BigQuery source refresh in Looker Studio to match the ETL schedule.

---

## Troubleshooting

1. **413 Request Too Large**: Adjust batch size in `streamToStage` (default is 200).  
2. **Missing column errors**: Ensure your HubSpot app has access to all properties and they appear in `getProps()`.

---

