// index.js  —— HubSpot ➜ BigQuery with true upsert (stream → stage → MERGE)

require('dotenv').config();
const axios = require('axios');
const { BigQuery } = require('@google-cloud/bigquery');

const hubspot = axios.create({
  baseURL: 'https://api.hubapi.com/crm/v3/objects/contacts',
  headers: { Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}` }
});
const bq = new BigQuery({ projectId: process.env.BQ_PROJECT_ID });

/* ---------- helpers -------------------------------------------------- */
const sanitise = n =>
  (/^[^a-z]/i.test(n = n.toLowerCase().replace(/[^a-z0-9_]/g, '_')) ? 'p_' + n : n);

const hub2bq = t => ({
  string: 'STRING',
  number: 'FLOAT',
  datetime: 'TIMESTAMP',
  date: 'DATE',
  bool: 'BOOLEAN'
}[t] || 'STRING');

/* ---------- property catalogue -------------------------------------- */
async function getProps() {
  let props = [], after;
  do {
    const { data } = await axios.get(
      'https://api.hubapi.com/crm/v3/properties/contacts',
      { headers: { Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}` }, params: { limit: 100, after, archived: false } }
    );
    props = props.concat(data.results);
    after = data.paging?.next?.after;
  } while (after);
  return props;
}

/* ---------- sync-tracker -------------------------------------------- */
async function lastSync() {
  const sql = `
    SELECT last_sync_timestamp
    FROM \`${process.env.BQ_PROJECT_ID}.${process.env.BQ_DATASET}.sync_tracker\`
    WHERE entity='contacts' LIMIT 1
  `;
  const [rows] = await bq.query({ query: sql });
  if (rows.length && rows[0].last_sync_timestamp) {
    return new Date(rows[0].last_sync_timestamp.value || rows[0].last_sync_timestamp).getTime();
  }
  return Date.now() - 30 * 24 * 60 * 60 * 1e3; // 30 days ago
}

async function saveSync(ts) {
  const sql = `
    MERGE \`${process.env.BQ_PROJECT_ID}.${process.env.BQ_DATASET}.sync_tracker\` T
    USING (SELECT 'contacts' AS entity) S
    ON T.entity = S.entity
    WHEN MATCHED THEN
      UPDATE SET last_sync_timestamp = TIMESTAMP_MILLIS(${ts})
    WHEN NOT MATCHED THEN
      INSERT(entity, last_sync_timestamp) VALUES('contacts', TIMESTAMP_MILLIS(${ts}));
  `;
  await bq.query({ query: sql });
}

/* ---------- fetch changed contacts ---------------------------------- */
async function fetchContacts(props) {
  const params = { limit: 100, properties: props.map(p => p.name).join(',') };
  const out = {};
  const since = await lastSync();
  const now = Date.now();

  params.filterGroups = [{
    filters: [{
      propertyName: 'hs_lastmodifieddate',
      operator: 'GT',
      value: since.toString()
    }]
  }];

  let after;
  do {
    params.after = after;
    const { data } = await hubspot.get('', { params });
    data.results.forEach(c => out[c.id] = { id: c.id, ...(out[c.id] || {}), ...c.properties });
    after = data.paging?.next?.after;
  } while (after);

  await saveSync(now);
  return Object.values(out);
}

/* ---------- ensure schema on table ---------------------------------- */
async function ensureTable(tableName, schema) {
  const ds = bq.dataset(process.env.BQ_DATASET);
  const tb = ds.table(tableName);
  const [exists] = await tb.exists();

  if (!exists) {
    const [table] = await ds.createTable(tableName, { schema: { fields: schema } });
    return table;
  }

  const [meta] = await tb.getMetadata();
  const have = new Set(meta.schema.fields.map(f => f.name));
  const add = schema.filter(f => !have.has(f.name));

  if (add.length) {
    console.log(`🛠️ Adding new fields to schema: ${add.map(f => f.name).join(', ')}`);
    meta.schema.fields.push(...add);
    await tb.setMetadata({ schema: meta.schema });
  }

  return tb;
}


/* ---------- batch-insert into staging with retry/errors ------------ */
async function streamToStage(rows, schema) {
  const stage = await ensureTable('Contacts_stage', schema);
  // truncate existing stage
  await bq.query({ query: `TRUNCATE TABLE \`${process.env.BQ_PROJECT_ID}.${process.env.BQ_DATASET}.Contacts_stage\`` });

  const batchSize = 200;
  for (let i = 0; i < rows.length; i += batchSize) {
    const slice = rows.slice(i, i + batchSize);
    try {
      await stage.insert(slice, { ignoreUnknownValues: true, skipInvalidRows: true });
    } catch (e) {
      if (e.name === 'PartialFailureError' && e.errors?.length) {
        console.warn('⚠️  stage batch errors (first 3):');
        e.errors.slice(0, 3).forEach(err =>
          console.warn(err.errors, 'row snippet', JSON.stringify(err.row).slice(0,200))
        );
      } else {
        throw e;
      }
    }
  }
}

/* ---------- merge stage → master ------------------------------------ */
async function mergeStageIntoMaster(schema) {
  const cols = schema.map(f => `\`${f.name}\``).join(', ');
  const updates = schema
    .filter(f => f.name !== 'id')
    .map(f => `T.\`${f.name}\` = S.\`${f.name}\``).join(', ');

  const sql = `
    MERGE \`${process.env.BQ_PROJECT_ID}.${process.env.BQ_DATASET}.Contacts\` T
    USING \`${process.env.BQ_PROJECT_ID}.${process.env.BQ_DATASET}.Contacts_stage\` S
    ON T.id = S.id
    WHEN MATCHED THEN
      UPDATE SET ${updates}
    WHEN NOT MATCHED THEN
      INSERT (${cols}) VALUES (${cols})
  `;

  await bq.query({ query: sql });
}


/* ---------- main ----------------------------------------------------- */
(async () => {
  try {
    const props = await getProps();
    const rawSchema = [
      { name: 'id', type: 'STRING', mode: 'REQUIRED' },
      ...props.map(p => ({ name: sanitise(p.name), type: hub2bq(p.type), mode: 'NULLABLE' }))
    ];

    // 🧠 Ensure both stage and master tables have updated schema
    const stageTable = await ensureTable('Contacts_stage', rawSchema);
    await ensureTable('Contacts', rawSchema);

    const schema = rawSchema;

    const typeMap = Object.fromEntries(props.map(p => [p.name, p.type]));
    const propMap = Object.fromEntries(props.map(p => [p.name, sanitise(p.name)]));

    const contacts = await fetchContacts(props);
    if (!contacts.length) return console.log('ℹ️ No changes');

    const rows = contacts.map(c => {
  const r = { id: c.id };
  for (const [k, v] of Object.entries(c)) {
    if (k === 'id') continue;
    const col = propMap[k];
    const type = typeMap[k];

    if (!col) continue; // Prevent mismatch if HubSpot returned an unknown field

    if (v === '' || v == null) {
      r[col] = null;
      continue;
    }

    if (/^hs_email_optout_\d+$/.test(k)) {
      r[col] = String(v);
      continue;
    }

    switch (type) {
      case 'number': {
        const n = parseFloat(v.toString().replace(/[^\d.-]/g, ''));
        r[col] = isNaN(n) ? null : n;
        break;
      }
      case 'bool': {
        if (v === true || v === false) {
          r[col] = v;
        } else if (typeof v === 'string') {
          const lower = v.toLowerCase();
          r[col] = lower === 'true' ? true : lower === 'false' ? false : null;
        } else {
          r[col] = null;
        }
        break;
      }
      default:
        r[col] = v;
    }
  }
  return r;
});


    await streamToStage(rows, schema);
    await mergeStageIntoMaster(schema);

    console.log(`✅ Upserted ${rows.length} contacts`);
  } catch (e) {
    console.error('❌ ETL failed:', e);
    process.exit(1);
  }
})();
