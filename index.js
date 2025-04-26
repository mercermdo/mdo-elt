require('dotenv').config();
const axios = require('axios');
const { BigQuery } = require('@google-cloud/bigquery');

const hubspot = axios.create({
  baseURL: 'https://api.hubapi.com/crm/v3/objects/contacts',
  headers: { Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}` }
});

const bigquery = new BigQuery({
  projectId: process.env.BQ_PROJECT_ID,
});

// 📥 Step 1: Fetch all property names from HubSpot
async function getAllPropertyNames() {
  const res = await axios.get('https://api.hubapi.com/crm/v3/properties/contacts', {
    headers: { Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}` }
  });
  return res.data.results.map(p => p.name);
}

// 🕒 Step 2: Load the last sync time
async function getLastSyncTimestamp() {
  const query = `
    SELECT last_sync_timestamp
    FROM \`${process.env.BQ_PROJECT_ID}.${process.env.BQ_DATASET}.sync_tracker\`
    WHERE entity = 'contacts'
    LIMIT 1
  `;
  const [rows] = await bigquery.query({ query });

  if (rows.length > 0 && rows[0].last_sync_timestamp) {
    let ts = rows[0].last_sync_timestamp;
    console.log('🛰️ Raw last_sync_timestamp from BigQuery:', ts);

    if (ts && typeof ts.value === 'string') {
      console.log('🔵 Extracting timestamp value from BigQueryTimestamp...');
      ts = ts.value;
    }

    const parsedDate = new Date(ts);
    if (!isNaN(parsedDate)) {
      console.log(`✅ Parsed last sync timestamp: ${parsedDate.toISOString()}`);
      return parsedDate.getTime();
    }
    console.warn('❗ Failed to parse timestamp, using fallback.');
  }

  console.warn('⚠️ Could not parse last sync timestamp, using fallback.');
  const fallback = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
  console.log(`✅ Fallback last sync timestamp: ${fallback.toISOString()}`);
  return fallback.getTime();
}

// 🕒 Step 3: Save the current sync time
async function saveLastSyncTimestamp(timestamp) {
  const query = `
    MERGE \`${process.env.BQ_PROJECT_ID}.${process.env.BQ_DATASET}.sync_tracker\` t
    USING (SELECT 'contacts' AS entity) s
    ON t.entity = s.entity
    WHEN MATCHED THEN
      UPDATE SET last_sync_timestamp = TIMESTAMP_MILLIS(${timestamp})
    WHEN NOT MATCHED THEN
      INSERT (entity, last_sync_timestamp) VALUES ('contacts', TIMESTAMP_MILLIS(${timestamp}))
  `;
  await bigquery.query({ query });
}

// 🔄 Step 4: Fetch contacts (with all properties)
async function fetchContacts() {
  console.log('📡 Fetching contacts from HubSpot...');
  let allContacts = [];
  let after = undefined;
  const lastSync = await getLastSyncTimestamp();
  const now = Date.now();
  const properties = await getAllPropertyNames();  // ✅ fetch all property names!

  console.log(`Last sync was at: ${lastSync ? new Date(parseInt(lastSync)).toISOString() : 'Never (fetching all)'}`);

  do {
    const params = {
      limit: 100,
      after: after,
      properties: properties  // ✅ use the full list of properties
    };

    if (lastSync && lastSync > 0) {
      params['filterGroups'] = [
        {
          filters: [
            {
              propertyName: 'hs_lastmodifieddate',
              operator: 'GT',
              value: lastSync.toString()
            }
          ]
        }
      ];
    }

    const { data } = await hubspot.get('', { params });

    const mapped = data.results
      .filter(contact => contact.id) // Skip contacts missing an ID
      .map(contact => ({
        id: contact.id,
        ...contact.properties
      }));

    allContacts = allContacts.concat(mapped);
    after = data.paging?.next?.after;

    console.log(`📦 Retrieved ${mapped.length} contacts... (Total so far: ${allContacts.length})`);
  } while (after);

  console.log(`✅ Finished fetching ${allContacts.length} contacts`);

  await saveLastSyncTimestamp(now);

  return allContacts;
}

// 📤 Step 5: Upload contacts to BigQuery
async function loadToBigQuery(rows) {
  const table = bigquery.dataset(process.env.BQ_DATASET).table(process.env.BQ_TABLE);
  await table.insert(rows, { ignoreUnknownValues: true, skipInvalidRows: true });
  console.log(`🎉 Uploaded ${rows.length} contacts to BigQuery`);
}

// 🚀 Main ETL Runner
(async () => {
  try {
    const rows = await fetchContacts();
    if (rows.length > 0) {
      await loadToBigQuery(rows);
    } else {
      console.log('ℹ️ No new contacts to update.');
    }
  } catch (err) {
    console.error('❌ ETL failed:', err.message);
  }
})();


