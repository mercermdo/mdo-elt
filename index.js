// index.js  ── dynamic, paginated, self‑expanding schema

require('dotenv').config();
const axios      = require('axios');
const { BigQuery } = require('@google-cloud/bigquery');

// ───────────────────────────── HubSpot + BigQuery clients
const hubspot = axios.create({
  baseURL : 'https://api.hubapi.com/crm/v3/objects/contacts', // ← for contact pages
  headers : { Authorization:`Bearer ${process.env.HUBSPOT_TOKEN}` }
});

const bigquery = new BigQuery({ projectId: process.env.BQ_PROJECT_ID });

/* ───────────────────────────────────────── helpers */
function sanitise(name) {
  let n = name.toLowerCase().replace(/[^a-z0-9_]/g, '_');   // keep only safe chars
  if (/^[^a-z]/.test(n)) n = 'p_' + n;                      // must start with letter
  return n;
}

function hubTypeToBq(t) {
  return ({string:'STRING', number:'FLOAT', datetime:'TIMESTAMP', date:'DATE', bool:'BOOLEAN'}[t] || 'STRING');
}

/* ─────────────────── property catalogue (stand‑alone request) */
async function getAllPropertyMetadata() {
  let props = [];
  let after;
  do {
    const { data } = await axios.get(
      'https://api.hubapi.com/crm/v3/properties/contacts',
      {
        headers: { Authorization:`Bearer ${process.env.HUBSPOT_TOKEN}` },
        params : { limit:100, after, archived:false }
      }
    );
    props = props.concat(data.results);
    after  = data.paging?.next?.after;
  } while (after);
  return props;
}

/* ─────────────────── sync tracker helpers */
async function getLastSyncTimestamp() {
  const sql = `SELECT last_sync_timestamp
              FROM \`${process.env.BQ_PROJECT_ID}.${process.env.BQ_DATASET}.sync_tracker\`
              WHERE entity='contacts' LIMIT 1`;
  const [rows] = await bigquery.query({ query: sql });
  if (rows.length && rows[0].last_sync_timestamp) {
    return new Date(rows[0].last_sync_timestamp.value || rows[0].last_sync_timestamp).getTime();
  }
  return Date.now() - 30*24*60*60*1000; // default 30 days
}

async function saveLastSyncTimestamp(ts) {
  const sql = `MERGE \`${process.env.BQ_PROJECT_ID}.${process.env.BQ_DATASET}.sync_tracker\` T
               USING (SELECT 'contacts' AS entity) S
               ON T.entity = S.entity
               WHEN MATCHED THEN UPDATE SET last_sync_timestamp = TIMESTAMP_MILLIS(${ts})
               WHEN NOT MATCHED THEN INSERT (entity,last_sync_timestamp)
               VALUES ('contacts', TIMESTAMP_MILLIS(${ts}))`;
  await bigquery.query({ query: sql });
}

/* ─────────────────── fetch contacts (all props, de‑chunked) */
async function fetchContacts(allProps) {
  console.log('📡 Fetching contacts from HubSpot…');

  // chunk properties (HubSpot max 100 per request)
  const propChunks = [];
  for (let i = 0; i < allProps.length; i += 100) {
    propChunks.push(allProps.slice(i, i + 100).map(p => p.name));
  }

  const lastSync = await getLastSyncTimestamp();
  const contacts = {};       // id → merged record
  let after;

  do {
    let nextAfter;
    for (let idx = 0; idx < propChunks.length; idx++) {
      const params = {
        limit:100,
        after,
        properties: propChunks[idx]
      };
      if (lastSync) {
        params.filterGroups = [{
          filters:[{ propertyName:'hs_lastmodifieddate', operator:'GT', value:lastSync.toString() }]
        }];
      }
      const { data } = await hubspot.get('', { params });

      // keep paging cursor only from the *first* chunk
      if (idx === 0) nextAfter = data.paging?.next?.after;

      data.results.forEach(c => {
        contacts[c.id] = { id:c.id, ...(contacts[c.id]||{}), ...c.properties };
      });
    }
    after = nextAfter;
  } while (after);

  console.log(`✅ Finished fetching ${Object.keys(contacts).length} contacts`);
  await saveLastSyncTimestamp(Date.now());
  return Object.values(contacts);
}

/* ─────────────────── table creator/extender */
async function ensureTable(schemaFields) {
  const ds    = bigquery.dataset(process.env.BQ_DATASET);
  const table = ds.table(process.env.BQ_TABLE);
  const [exists] = await table.exists();

  if (!exists) {
    await ds.createTable(process.env.BQ_TABLE, { schema:{ fields:schemaFields } });
    return table;
  }

  const [meta] = await table.getMetadata();
  const present = new Set(meta.schema.fields.map(f => f.name));
  const add     = schemaFields.filter(f => !present.has(f.name));
  if (add.length) {
    meta.schema.fields.push(...add);
    await table.setMetadata({ schema: meta.schema });
  }
  return table;
}

/* ─────────────────── main */
(async () => {
  try {
    // 1. schema
    const hubProps = await getAllPropertyMetadata();
    const schema = [
      { name:'id', type:'STRING', mode:'REQUIRED' },
      ...hubProps.map(p => ({ name:sanitise(p.name), type:hubTypeToBq(p.type), mode:'NULLABLE' }))
    ];

    // 2. data
    const contacts = await fetchContacts(hubProps);
    const map      = Object.fromEntries(hubProps.map(p => [p.name, sanitise(p.name)]));

    const rows = contacts.map(c => {
      const r = { id:c.id };
      for (const [k,v] of Object.entries(c)) if (k !== 'id') r[ map[k] ] = v ?? null;
      return r;
    });

    // 3. load
    const table = await ensureTable(schema);
    if (rows.length) await table.insert(rows, { ignoreUnknownValues:true, skipInvalidRows:true });

    console.log(`✅ Uploaded ${rows.length} rows across ${schema.length} columns`);
  } catch (err) {
    console.error('❌ ETL failed:', err);
    process.exit(1);
  }
})();
