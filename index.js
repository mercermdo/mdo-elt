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

// 🔍 Step 1: Get all property names from HubSpot
async function getAllPropertyNames() {
  const res = await axios.get('https://api.hubapi.com/crm/v3/properties/contacts', {
    headers: { Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}` }
  });

  return res.data.results.map(p => p.name);
}

// 📥 Step 2: Fetch all contacts using pagination
async function fetchContacts() {
  console.log('📡 Fetching contacts from HubSpot...');
  let allContacts = [];
  let after = undefined;
  const properties = await getAllPropertyNames();

  do {
    const { data } = await hubspot.get('', {
      params: {
        limit: 100,
        after: after,
        properties: properties
      }
    });

    const mapped = data.results.map(contact => ({
      id: contact.id,
      ...contact.properties  // spreads all properties
