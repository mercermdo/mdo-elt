// index.js  – dynamic schema, MERGE-upsert, 500-row batches
require('dotenv').config();
const axios      = require('axios');
const {BigQuery} = require('@google-cloud/bigquery');

/* ── clients ───────────────────────────────────────────────────────── */
const hubspot = axios.create({
  baseURL : 'https://api.hubapi.com/crm/v3/objects/contacts',
  headers : {Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}`}
});
const bq = new BigQuery({projectId: process.env.BQ_PROJECT_ID});

/* ── helpers ───────────────────────────────────────────────────────── */
const sanitise = n =>
  (/^[^a-z]/i.test(n = n.toLowerCase().replace(/[^a-z0-9_]/g, '_')) ? 'p_' + n : n);

const hub2bq = t =>
  ({string: 'STRING', number: 'FLOAT', datetime: 'TIMESTAMP', date: 'DATE', bool: 'BOOLEAN'}[t] || 'STRING');

/* ── property catalogue ───────────────────────────────────────────── */
async function getProps() {
  let props = [], after;
  do {
    const {data} = await axios.get(
      'https://api.hubapi.com/crm/v3/properties/contacts',
      {headers: {Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}`},
       params : {limit: 100, after, archived: false}});
    props = props.concat(data.results);
    after  = data.paging?.next?.after;
  } while (after);
  return props;
}

/* ── sync tracker ──────────────────────────────────────────────────── */
async function lastSync() {
  const sql = `SELECT last_sync_timestamp
               FROM \`${process.env.BQ_PROJECT_ID}.${process.env.BQ_DATASET}.sync_tracker\`
               WHERE entity='contacts' LIMIT 1`;
  const [r] = await bq.query({query: sql});
  return r.length
         ? new Date(r[0].last_sync_timestamp.value || r[0].last_sync_timestamp).getTime()
         : Date.now() - 30 * 24 * 60 * 60 * 1e3;          // 30 days ago
}
async function saveSync(ts) {
  const sql = `MERGE \`${process.env.BQ_PROJECT_ID}.${process.env.BQ_DATASET}.sync_tracker\` T
               USING (SELECT 'contacts' AS entity) S
               ON T.entity = S.entity
               WHEN MATCHED THEN UPDATE SET last_sync_timestamp = TIMESTAMP_MILLIS(${ts})
               WHEN NOT MATCHED THEN INSERT(entity,last_sync_timestamp)
               VALUES('contacts', TIMESTAMP_MILLIS(${ts}))`;
  await bq.query({query: sql});
}

/* ── fetch contacts (all props in one call) ───────────────────────── */
async function fetchContacts(props) {
  const params = {
    limit: 100,
    properties: props.map(p => p.name).join(',')
  };
  const out   = {};
  const since = await lastSync();
  const now   = Date.now();

  params.filterGroups = [{
    filters: [{
      propertyName: 'hs_lastmodifieddate',
      operator    : 'GT',
      value       : since.toString()
    }]
  }];

  let after;
  do {
    params.after = after;
    const {data} = await hubspot.get('', {params});
    data.results.forEach(c => out[c.id] = {id: c.id, ...(out[c.id] || {}), ...c.properties});
    after = data.paging?.next?.after;
  } while (after);

  await saveSync(now);
  return Object.values(out);
}

/* ── ensure table & evolve schema ─────────────────────────────────── */
async function ensureTable(schema) {
  const ds = bq.dataset(process.env.BQ_DATASET);
  const tb = ds.table(process.env.BQ_TABLE);
  const [exists] = await tb.exists();

  if (!exists) {
    await ds.createTable(process.env.BQ_TABLE, {schema: {fields: schema}});
    return {ds, tb};
  }
  const [meta] = await tb.getMetadata();
  const have   = new Set(meta.schema.fields.map(f => f.name));
  const add    = schema.filter(f => !have.has(f.name));
  if (add.length) {
    meta.schema.fields.push(...add);
    await tb.setMetadata({schema: meta.schema});
  }
  return {ds, tb};
}

/* ── helper: upsert one slice (temp table + MERGE) ─────────────────── */
async function upsertSlice(ds, target, slice, schema) {
  const tempName = `tmp_contacts_${Date.now()}_${Math.random().toString(36).slice(2)}`;
  const [temp]   = await ds.createTable(tempName, {schema: {fields: schema}, temporary: true});
  await temp.insert(slice.map(r => ({insertId: r.id, json: r})));

  const tgt = `\`${ds.id}.${target.id}\``;
  const src = `\`${ds.id}.${tempName}\``;

  const cols = schema.map(f => f.name).join(', ');
  const sets = schema.map(f => `T.${f.name}=S.${f.name}`).join(', ');

  const mergeSQL = `
    MERGE ${tgt} T
    USING ${src} S
    ON T.id = S.id
    WHEN MATCHED THEN UPDATE SET ${sets}
    WHEN NOT MATCHED THEN INSERT (${cols}) VALUES (${cols})
  `;
  await bq.query({query: mergeSQL});
}

/* ── batch upsert ─────────────────────────────────────────────────── */
async function upsertBatches(ds, table, rows, schema, size = 500) {
  for (let i = 0; i < rows.length; i += size) {
    const slice = rows.slice(i, i + size);
    await upsertSlice(ds, table, slice, schema);
  }
}

/* ── main ─────────────────────────────────────────────────────────── */
(async () => {
  try {
    /* 1 ─ build schema */
    const props  = await getProps();
    const schema = [
      {name: 'id', type: 'STRING', mode: 'REQUIRED'},
      ...props.map(p => ({name: sanitise(p.name), type: hub2bq(p.type), mode: 'NULLABLE'}))
    ];

    /* 2 ─ fetch changed contacts */
    const contacts = await fetchContacts(props);
    const map      = Object.fromEntries(props.map(p => [p.name, sanitise(p.name)]));

    /* 3 ─ build rows ("" → NULL) */
    const rows = contacts.map(c => {
      const r = {id: c.id};
      for (const [k, v] of Object.entries(c)) if (k !== 'id') r[map[k]] = (v === '' ? null : v ?? null);
      return r;
    });

    /* 4 ─ ensure table & MERGE-upsert in batches */
    const {ds, tb} = await ensureTable(schema);
    await upsertBatches(ds, tb, rows, schema);

    console.log(`✅ Up-serted ${rows.length} contacts across ${schema.length} columns`);
  } catch (e) {
    console.error('❌ ETL failed:', e);
    process.exit(1);
  }
})();



