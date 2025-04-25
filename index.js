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

async function fetchContacts() {
  const { data } = await hubspot.get('', {
    params: {
      limit: 100,
      properties: ['firstname', 'email', 'lifecyclestage', 'createdate', 'hs_lastmodifieddate']
    }
  });

  return data.results.map(contact => ({
    id: contact.id,
    firstname: contact.properties.firstname || null,
    email: contact.properties.email || null,
    lifecycle: contact.properties.lifecyclestage || null,
    createdate: new Date(contact.properties.createdate),
    hs_lastmodifieddate: new Date(contact.properties.hs_lastmodifieddate)
  }));
}

async function loadToBigQuery(rows) {
  const table = bigquery.dataset(process.env.BQ_DATASET).table(process.env.BQ_TABLE);
  await table.insert(rows, { ignoreUnknownValues: true, skipInvalidRows: true });
  console.log(`✅ Uploaded ${rows.length} contacts`);
}

(async () => {
  const rows = await fetchContacts();
  await loadToBigQuery(rows);
})();
