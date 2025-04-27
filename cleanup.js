// cleanup.js  — remove deleted HubSpot contacts from BigQuery

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

(async () => {
  try {
    console.log('⏳ Fetching live HubSpot contact IDs…');
    // 1. Retrieve all existing contact IDs from HubSpot
    let allIds = [];
    let after;
    do {
      const { data } = await hubspot.get('', { params: { limit: 100, after } });
      allIds.push(...data.results.map(c => c.id));
      after = data.paging?.next?.after;
    } while (after);

    console.log(`✅ Retrieved ${allIds.length} live contact IDs`);

    // 2. Delete rows in BigQuery where id is not in live HubSpot IDs
    if (allIds.length === 0) {
      console.warn('⚠️ No live IDs found, skipping delete');
      return;
    }

    // Parameterize list
    const placeholders = allIds.map(() => '?').join(', ');
    const deleteSql = `
      DELETE FROM \`${process.env.BQ_PROJECT_ID}.${process.env.BQ_DATASET}.Contacts\`
      WHERE id NOT IN (${placeholders})
    `;

    console.log('⏳ Deleting removed contacts from BigQuery…');
    const [job] = await bq.createQueryJob({
      query: deleteSql,
      params: allIds
    });
    // wait for completion
    await job.getQueryResults();

    // fetch metadata for dml stats
    const [metadata] = await job.getMetadata();
    const deletedCount = metadata.statistics?.query?.dmlStats?.deletedRowCount || 0;

    console.log(`✅ Deleted ${deletedCount} contacts from BigQuery`);
  } catch (err) {
    console.error('❌ Cleanup failed:', err);
    process.exit(1);
  }
})();

