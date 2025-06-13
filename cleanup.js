// cleanup.js — scalable deletion of removed HubSpot contacts from BigQuery

require('dotenv').config();
const axios = require('axios');
const { BigQuery } = require('@google-cloud/bigquery');

// HubSpot client
const hubspot = axios.create({
  baseURL: 'https://api.hubapi.com/crm/v3/objects/contacts',
  headers: { Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}` }
});

// BigQuery client
const bq = new BigQuery({ projectId: process.env.BQ_PROJECT_ID });
const dataset = process.env.BQ_DATASET;

(async () => {
  try {
    console.log('⏳ Fetching live HubSpot contact IDs…');

    // Step 1: Fetch all live contact IDs from HubSpot
    const allIds = [];
    let after;
    do {
      const { data } = await hubspot.get('', { params: { limit: 100, after } });
      allIds.push(...data.results.map(c => c.id));
      after = data.paging?.next?.after;
    } while (after);

    console.log(`✅ Retrieved ${allIds.length} live contact IDs`);

    if (allIds.length === 0) {
      console.warn('⚠️ No live IDs found, skipping delete');
      return;
    }

    // Step 2: Create or replace temporary table
    const tempTableId = 'live_contact_ids';
    const tempTableRef = bq.dataset(dataset).table(tempTableId);

    console.log('🛠️ Creating or replacing temp table with live IDs…');
    await bq.query({
      query: `CREATE OR REPLACE TABLE \`${process.env.BQ_PROJECT_ID}.${dataset}.${tempTableId}\` (id STRING)`,
    });

    // Step 3: Insert live contact IDs in batches
    const BATCH_SIZE = 5000;
    for (let i = 0; i < allIds.length; i += BATCH_SIZE) {
      const rows = allIds.slice(i, i + BATCH_SIZE).map(id => ({ id }));
      await tempTableRef.insert(rows);
    }

    // Step 4: Delete from Contacts where id is not in live_contact_ids
    console.log('⏳ Deleting removed contacts from BigQuery…');
    const deleteQuery = `
      DELETE FROM \`${process.env.BQ_PROJECT_ID}.${dataset}.Contacts\`
      WHERE id NOT IN (SELECT id FROM \`${process.env.BQ_PROJECT_ID}.${dataset}.${tempTableId}\`)
    `;
    const [job] = await bq.createQueryJob({ query: deleteQuery });
    await job.getQueryResults();

    const [metadata] = await job.getMetadata();
    const deletedCount = metadata.statistics?.query?.dmlStats?.deletedRowCount || 0;
    console.log(`✅ Deleted ${deletedCount} contacts from BigQuery`);
  } catch (err) {
    console.error('❌ Cleanup failed:', err);
    process.exit(1);
  }
})();
