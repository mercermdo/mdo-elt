require('dotenv').config();
const axios = require('axios');
const { BigQuery } = require('@google-cloud/bigquery');

// Set up HubSpot API client
const hubspot = axios.create({
  baseURL: 'https://api.hubapi.com/crm/v3/objects/contacts',
  headers: { Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}` }
});

// Set up BigQuery client
const bigquery = new BigQuery({
  projectId: process.env.BQ_PROJECT_ID,
});

// Step 1: Fetch all contact property names from HubSpot
async function getAllPropertyNames() {
  const res = await axios.get('https://api.hubapi.com/crm/v3/properties/contacts', {
    headers: { Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}` }
  });
  return res.data.results.map(p => p.name);
}

// Step 2: Fetch all contacts (with pagination and all properties)
async function fetchContacts() {
  console.log('📡 Fetching contacts from HubSpot...');
  let allContacts = [];
  let after = undefined;
  const properties = await getAllPropertyNames(); // Pull all properties

  do {
    const params = {
      limit: 100,
      after: after,
      properties: properties
    };

    const { data } = await hubspot.get('', { params });

    const mapped = data.results
      .filter(contact => contact.id) // Only keep contacts with ID
      .map(contact => ({
        id: contact.id,
        ...contact.properties
      }));

    allContacts = allContacts.concat(mapped);
    after = data.paging?.next?.after;

    console.log(`📦 Retrieved ${mapped.length} contacts... (Total so far: ${allContacts.length})`);
  } while (after);

  console.log(`✅ Finished fetching ${allContacts.length} contacts`);
  return allContacts;
}

// Step 3: Upload contacts to BigQuery
async function loadToBigQuery(rows) {
  const dataset = bigquery.dataset(process.env.BQ_DATASET);
  const table = dataset.table(process.env.BQ_TABLE);

  console.log('🚧 Checking if table exists...');
  const [exists] = await table.exists();
  if (!exists) {
    console.log('🚧 Table does not exist. Creating it dynamically...');
    const schemaFields = Object.keys(rows[0]).map(key => ({
      name: key,
      type: 'STRING',
      mode: 'NULLABLE'
    }));
    await dataset.createTable(process.env.BQ_TABLE, { schema: { fields: schemaFields } });
    console.log('✅ Created table with dynamic schema.');
  }

  await table.insert(rows, { ignoreUnknownValues: true, skipInvalidRows: true });
  console.log(`🎉 Uploaded ${rows.length} contacts to BigQuery`);
}

// Step 4: Main ETL Runner
(async () => {
  try {
    const rows = await fetchContacts();
    if (rows.length > 0) {
      await loadToBigQuery(rows);
    } else {
      console.log('ℹ️ No contacts to upload.');
    }
  } catch (err) {
    console.error('❌ ETL failed:', err.message);
  }
})();
