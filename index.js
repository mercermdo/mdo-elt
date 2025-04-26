// index.js  ── dynamic, paginated, self-expanding schema

require('dotenv').config();
const axios       = require('axios');
const {BigQuery}  = require('@google-cloud/bigquery');

const hubspot  = axios.create({
  baseURL : 'https://api.hubapi.com/crm/v3/objects/contacts',
  headers : { Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}` }
});

const bigquery = new BigQuery({ projectId: process.env.BQ_PROJECT_ID });

/* ──────────────────────────────────────────────────────────── helpers */

function sanitise(name) {
  // to lower-case, replace anything not [a-z0-9_] with underscore
  let n = name.toLowerCase().replace(/[^a-z0-9_]/g, '_');
  // BigQuery cannot start with digit or underscore
  if (/^[^a-z]/.test(n)) n = 'p_' + n;
  return n;
}

function hubTypeToBq(type) {
  return (
    { string:'STRING',number:'FLOAT',datetime:'TIMESTAMP',date:'DATE',bool:'BOOLEAN' }[type] ||
    'STRING'
  );
}

async function getAllPropertyMetadata() {
  let props = [];
  let after = undefined;
  do {
    const { data } = await hubspot.get('properties/contacts', {
      params: { limit: 100, after, archived:false }
    });
    props = props.concat(data.results);
    after = data.paging?.next?.after;
  } while (after);
  return props;
}

async function getLastSyncTimestamp() {
  const sql = `
    SELECT last_sync_timestamp
    FROM \`${process.env.BQ_PROJECT_ID}.${process.env.BQ_DATASET}.sync_tracker\`
    WHERE entity = 'contacts' LIMIT 1`;
  const [rows] = await bigquery.query({query:sql});
  if (rows.length && rows[0].last_sync_timestamp) {
    return new Date(rows[0].last_sync_timestamp.value || rows[0].last_sync_timestamp).getTime();
  }
  // default: 30 days ago
  return Date.now() - 30*24*60*60*1000;
}

async function saveLastSyncTimestamp(ts) {
 {
  const sql = `
    MERGE \`${process.env.BQ_PROJECT_ID}.${process.env.BQ_DATASET}.sync_tracker\` T
    USING (SELECT 'contacts' AS entity) S
    ON T.entity = S.entity
    WHEN MATCHED THEN
      UPDATE SET last_sync_timestamp = TIMESTAMP_MILLIS(${ts})
    WHEN NOT MATCHED THEN
      INSERT (entity,last_sync_timestamp) VALUES ('contacts',TIMESTAMP_MILLIS(${ts}))`;
  await bigquery.query({query:sql});
}

/* ─────────────────────────────────────────────── contact fetch & load */

async function fetchContacts(allProps) {
  console.log('📡 Fetching contacts from HubSpot…');

  // HubSpot allows max-100 properties per call
  const propChunks = [];
  for (let i = 0; i < allProps.length; i += 100) {
    propChunks.push(allProps.slice(i, i + 100).map(p => p.name));
  }

  let after = undefined;
  const lastSync = await getLastSyncTimestamp();
  const now      = Date.now();
  const contacts = {};

  do {
    for (const props of propChunks) {
      const params = {
        limit:      100,
        after,
        properties: props
      };

      if (lastSync) {
        params.filterGroups = [{
          filters: [{
            propertyName: 'hs_lastmodifieddate',
            operator:     'GT',
            value:        lastSync.toString()
          }]
        }];
      }

      const { data } = await hubspot.get('', { params });

      data.results.forEach(c => {
        // Merge chunks for the same contact ID
        contacts[c.id] = { id: c.id, ...(contacts[c.id] || {}), ...c.properties };
      });

      // use paging info only from the first chunk
      if (!after) after = data.paging?.next?.after;
    }
  } while (after);

  console.log(`✅ Finished fetching ${Object.keys(contacts).length} contacts`);
  await saveLastSyncTimestamp(now);
  return Object.values(contacts);
}

async function ensureTable(schemaFields) {
  const ds    = bigquery.dataset(process.env.BQ_DATASET);
  const table = ds.table(process.env.BQ_TABLE);
  const [exists] = await table.exists();
  if (!exists) {
    await ds.createTable(process.env.BQ_TABLE, { schema:{fields:schemaFields} });
    return table;
  }
  // If table exists, expand schema with any new columns
  const [meta] = await table.getMetadata();
  const present = new Set(meta.schema.fields.map(f => f.name));
  const toAdd   = schemaFields.filter(f => !present.has(f.name));
  if (toAdd.length) {
    meta.schema.fields.push(...toAdd);
    await table.setMetadata({schema:meta.schema});
  }
  return table;
}

(async () => {
  try {
    /* 1 ─ property catalogue & schema */
    const hubProps = await getAllPropertyMetadata();

    const schema = [
      {name:'id', type:'STRING', mode:'REQUIRED'},
      ...hubProps.map(p => ({
        name : sanitise(p.name),
        type : hubTypeToBq(p.type),
        mode : 'NULLABLE'
      }))
    ];

    /* 2 ─ fetch contacts changed since last sync */
    const contacts  = await fetchContacts(hubProps);

    /* 3 ─ map contact keys to sanitised column names */
    const propMap = Object.fromEntries(hubProps.map(p => [p.name, sanitise(p.name)]));
    const rows = contacts.map(c => {
      const obj = { id: c.id };
      for (const [k,v] of Object.entries(c)) {
        if (k === 'id') continue;
        obj[propMap[k]] = v ?? null;
      }
      return obj;
    });

    /* 4 ─ ensure table & upload */
    const table = await ensureTable(schema);
    if (rows.length) await table.insert(rows, {ignoreUnknownValues:true, skipInvalidRows:true});

    /* 5 ─ record sync */
    await saveLastSyncTimestamp(Date.now());
    
    console.log(`✅ Uploaded ${rows.length} rows across ${schema.length} columns`);
  } catch (e) {
    console.error('❌ ETL failed:', e);
    process.exit(1);
  }
})();

