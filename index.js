// index.js

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

// 📥 Fetch all property names from HubSpot
async function getAllPropertyMetadata() {
  const res = await axios.get('https://api.hubapi.com/crm/v3/properties/contacts', {
    headers: { Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}` }
  });
  return res.data.results;
}

// 🕒 Load the last sync time
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

    if (ts && typeof ts.value === 'string') ts = ts.value;

    const parsedDate = new Date(ts);
    if (!isNaN(parsedDate)) {
      console.log(`✅ Parsed last sync timestamp: ${parsedDate.toISOString()}`);
      return parsedDate.getTime();
    }
  }

  console.warn('⚠️ Using fallback: 30 days ago.');
  return Date.now() - 30 * 24 * 60 * 60 * 1000;
}

// 🕒 Save the current sync time
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

// 🔄 Fetch all contacts
async function fetchContacts(properties) {
  console.log('📡 Fetching contacts from HubSpot...');
  let allContacts = [];
  let after = undefined;
  const lastSync = await getLastSyncTimestamp();
  const now = Date.now();

  console.log(`Last sync was at: ${new Date(lastSync).toISOString()}`);

  do {
    const params = {
      limit: 100,
      after,
      properties: properties.map(p => p.name)
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
      .map(contact => ({ id: contact.id, ...contact.properties }));

    allContacts = allContacts.concat(mapped);
    after = data.paging?.next?.after;

    console.log(`📦 Retrieved ${mapped.length} contacts... (Total so far: ${allContacts.length})`);
  } while (after);

  console.log(`✅ Finished fetching ${allContacts.length} contacts`);

  await saveLastSyncTimestamp(now);

  return allContacts;
}

// 🛠️ Build BigQuery schema dynamically
function buildBigQuerySchema(properties) {
  const typeMap = {
    string: 'STRING',
    number: 'FLOAT',
    datetime: 'TIMESTAMP',
    bool: 'BOOLEAN',
    date: 'DATE'
  };

  const fields = [
    { name: 'id', type: 'STRING', mode: 'NULLABLE' }
  ];

  for (const prop of properties) {
    fields.push({
      name: prop.name,
      type: typeMap[prop.type] || 'STRING',
      mode: 'NULLABLE'
    });
  }

  return fields;
}

// 📤 Upload contacts to BigQuery
async function uploadToBigQuery(rows, schemaFields) {
  const dataset = bigquery.dataset(process.env.BQ_DATASET);
  const table = dataset.table(process.env.BQ_TABLE);

  // Check if table exists
  try {
    const [exists] = await table.exists();
    if (!exists) {
      console.log('🚧 Table does not exist. Creating it dynamically...');
      await dataset.createTable(process.env.BQ_TABLE, { schema: { fields: schemaFields } });
      console.log('✅ Created table with dynamic schema.');
    }
  } catch (err) {
    console.error('❌ Error checking/creating table:', err);
    throw err;
  }

  await table.insert(rows, { ignoreUnknownValues: true, skipInvalidRows: true });
  console.log(`🎉 Uploaded ${rows.length} contacts to BigQuery`);
}

// 🚀 Main runner
(async () => {
  try {
    const properties = await getAllPropertyMetadata();
    const contacts = await fetchContacts(properties);
    const schema = buildBigQuerySchema(properties);

    if (contacts.length > 0) {
      await uploadToBigQuery(contacts, schema);
    } else {
      console.log('ℹ️ No new contacts to update.');
    }
  } catch (err) {
    console.error('❌ ETL failed:', err.message);
  }
})();
