// cleanup.js
require('dotenv').config();
const axios      = require('axios');
const { BigQuery } = require('@google-cloud/bigquery');
const bq = new BigQuery({ projectId: process.env.BQ_PROJECT_ID });

// 1) Fetch all archived/deleted contacts in HubSpot
async function fetchDeletedIds() {
  let deleted = [], after;
  const client = axios.create({
    baseURL: 'https://api.hubapi.com/crm/v3/objects/contacts',
    headers: { Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}` }
  });
  do {
    const { data } = await client.get('', {
      params: { archived: true, limit: 100, after }
    });
    deleted.push(...data.results.map(c => c.id));
    after = data.paging?.next?.after;
  } while (after);
  return deleted;
}

// 2) Delete them from BQ
async function deleteFromBQ(ids) {
  if (!ids.length) {
    console.log('✅ No deleted contacts to remove.');
    return;
  }
  const sql = `
    DELETE FROM \`${process.env.BQ_PROJECT_ID}.${process.env.BQ_DATASET}.${process.env.BQ_TABLE}\`
    WHERE id IN UNNEST(@ids)
  `;
  await bq.query({ query: sql, params: { ids } });
  console.log(`✅ Deleted ${ids.length} contacts from BigQuery`);
}

(async () => {
  try {
    const ids = await fetchDeletedIds();
    await deleteFromBQ(ids);
  } catch (e) {
    console.error('❌ Cleanup failed:', e);
    process.exit(1);
  }
})();
