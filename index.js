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

// 📥 Fetch all HubSpot contact properties
async function getAllPropertyNames() {
  const res = await axios.get('https://api.hubapi.com/crm/v3/properties/contacts', {
    headers: { Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}` }
  });
  return res.data.results.map(p => p.name);
}

// 🕒 Load last sync timestamp
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

// 🕒 Save the new sync timestamp
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

// 🔄 Fetch contacts (full properties)
async function fetchContacts() {
  console.log('📡 Fetching contacts from HubSpot...');
  let allContacts = [];
  let after = undefined;
  const lastSync = await getLastSyncTimestamp();
  const now = Date.now();
  const properties = await getAllPropertyNames();

  console.log(`Last sync was at: ${lastSync ? new Date(parseInt(lastSync)).toISOString() : 'Never (fetching all)'}`);

  do {
    const params = {
      limit: 100,
      after: after,
      properties: properties
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
      .filter(contact => contact.id)
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

// 📤 Upload to BigQuery (auto-create schema)
async function loadToBigQuery(rows) {
  const dataset = bigquery.dataset(process.env.BQ_DATASET);
  const table = dataset.table(process.env.BQ_TABLE);

  try {
    await table.get(); // Check if table exists
  } catch (err) {
    if (err.code === 404) {
      console.log('🚧 Table does not exist. Creating it dynamically...');
      const schema = Object.keys(rows[0]).map(key => ({
        name: key,
        type: 'STRING', // Simplification: store all as string for now
        mode: 'NULLABLE'
      }));
      await dataset.createTable(process.env.BQ_TABLE, { schema });
      console.log('✅ Created table with dynamic schema.');
      await new Promise(resolve => setTimeout(resolve, 2000)); // <- 🕑 Add a 2-second wait
    } else {
      throw err; // Unexpected error
    }
  }

  // ✅ Now table is ready
  await table.insert(rows, { ignoreUnknownValues: true, skipInvalidRows: true });
  console.log(`🎉 Uploaded ${rows.length} contacts to BigQuery`);
}


// 🚀 Main ETL runner
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
