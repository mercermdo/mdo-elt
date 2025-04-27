// index.js  – HubSpot → BigQuery  (hourly UPSERT)

require('dotenv').config();
const axios      = require('axios');
const {BigQuery} = require('@google-cloud/bigquery');

/* ───────────── clients */
const hubspot = axios.create({
  baseURL : 'https://api.hubapi.com/crm/v3/objects/contacts',
  headers : {Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}`}
});
const bq = new BigQuery({projectId: process.env.BQ_PROJECT_ID});

/* ───────────── helpers */
const sanitise = n =>
  (/^[^a-z]/i.test(n = n.toLowerCase().replace(/[^a-z0-9_]/g, '_')) ? 'p_' + n : n);

const hub2bq = t =>
  ({string: 'STRING', number: 'FLOAT', datetime: 'TIMESTAMP',
    date: 'DATE', bool: 'BOOLEAN'}[t] || 'STRING');

/* ───────────── property catalogue (once per run) */
async function getProps() {
  let props = [], after;
  do {
    const {data} = await axios.get(
      'https://api.hubapi.com/crm/v3/properties/contacts',
      {headers: {Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}`},
       params : {limit: 100, after, archived: false}});
    props = props.concat(data.results);
    after = data.paging?.next?.after;
  } while (after);
  return props;
}

/* ───────────── sync-tracker (so we only fetch changes) */
async function lastSync() {
  const sql = `
    SELECT last_sync_timestamp
    FROM   \`${process.env.BQ_PROJECT_ID}.${process.env.BQ_DATASET}.sync_tracker\`
    WHERE  entity = 'contacts'
    LIMIT  1`;
  const [r] = await bq.query({query: sql});
  return r.length
       ? new Date(r[0].last_sync_timestamp.value || r[0].last_sync_timestamp).getTime()
       : Date.now() - 30 * 24 * 60 * 60 * 1e3;     // 30 days ago
}
async function saveSync(ts) {
  const sql = `
    MERGE \`${process.env.BQ_PROJECT_ID}.${process.env.BQ_DATASET}.sync_tracker\` T
    USING (SELECT 'contacts' AS entity) S
    ON T.entity = S.entity
    WHEN MATCHED THEN
      UPDATE SET last_sync_timestamp = TIMESTAMP_MILLIS(${ts})
    WHEN NOT MATCHED THEN
      INSERT (entity, last_sync_timestamp)
      VALUES ('contacts', TIMESTAMP_MILLIS(${ts}))`;
  await bq.query({query: sql});
}

/* ───────────── fetch all contacts changed since last sync */
async function fetchContacts(props) {
  const params = {
    limit: 100,
    properties: props.map(p => p.name).join(',')
  };
  const since = await lastSync();
  if (since) {
    params.filterGroups = [{
      filters: [{
        propertyName: 'hs_lastmodifieddate',
        operator:     'GT',
        value:        since.toString()
      }]
    }];
  }

  const out = {}, now = Date.now();
  let after;
  do {
    params.after = after;
    const {data} = await hubspot.get('', {params});
    data.results.forEach(c =>
      out[c.id] = {id: c.id, ...(out[c.id] || {}), ...c.properties});
    after = data.paging?.next?.after;
  } while (after);

  await saveSync(now);
  return Object.values(out);
}

/* ───────────── ensure main & staging tables exist / evolve schema */
async function ensureTables(schema) {
  const ds   = bq.dataset(process.env.BQ_DATASET);
  const main = ds.table(process.env.BQ_TABLE);
  const stage= ds.table(`${process.env.BQ_TABLE}_stage`);

  // helper to create / widen a table
  async function ensure(tb){
    const [exists] = await tb.exists();
    if (!exists) {
      await ds.createTable(tb.id, {schema: {fields: schema}});
      return;
    }
    const [meta] = await tb.getMetadata();
    const have   = new Set(meta.schema.fields.map(f => f.name));
    const add    = schema.filter(f => !have.has(f.name));
    if (add.length) {
      meta.schema.fields.push(...add);
      await tb.setMetadata({schema: meta.schema});
    }
  }

  await Promise.all([ensure(main), ensure(stage)]);
  return {main, stage};
}

/* ───────────── batch-insert into the staging table */
async function insertStage(table, rows, batchSize = 500) {
  for (let i = 0; i < rows.length; i += batchSize) {
    const slice   = rows.slice(i, i + batchSize);
    const payload = slice.map(r => ({insertId: r.id, json: r}));
    await table.insert(payload, {ignoreUnknownValues: true, skipInvalidRows: true});
  }
}

/* ───────────── perform the MERGE main ⇐ stage, then clear stage */
async function mergeIntoMain(main, stage, schema) {
  const cols = schema.map(c => c.name);              // every column incl. id
  const set  = cols.filter(c => c !== 'id')
                   .map(c => `T.${c}=S.${c}`)
                   .join(', ');
  const insertCols = cols.join(', ');
  const insertVals = cols.map(c => `S.${c}`).join(', ');

  const sql = `
    MERGE \`${main.id}\` T
    USING \`${stage.id}\` S
    ON T.id = S.id
    WHEN MATCHED THEN
      UPDATE SET ${set}
    WHEN NOT MATCHED THEN
      INSERT (${insertCols}) VALUES (${insertVals});
    DELETE FROM \`${stage.id}\` WHERE TRUE;   -- clear stage`;
  await bq.query({query: sql});
}

/* ───────────── main */
(async () => {
  try {
    /* 1 – columns / schema */
    const props  = await getProps();
    const schema = [
      {name: 'id', type: 'STRING', mode: 'REQUIRED'},
      ...props.map(p => ({
        name: sanitise(p.name),
        type: hub2bq(p.type),
        mode: 'NULLABLE'
      }))
    ];

    /* 2 – fetch data from HubSpot */
    const contacts = await fetchContacts(props);
    if (!contacts.length) {
      console.log('ℹ️  Nothing new to sync.');
      return;
    }
    const map = Object.fromEntries(props.map(p => [p.name, sanitise(p.name)]));
    const rows = contacts.map(c => {
      const r = {id: c.id};
      for (const [k, v] of Object.entries(c))
        if (k !== 'id') r[map[k]] = v === '' ? null : v ?? null;
      return r;
    });

    /* 3 – BQ tables & schema */
    const {main, stage} = await ensureTables(schema);

    /* 4 – stream into staging, then MERGE */
    await insertStage(stage, rows);
    await mergeIntoMain(main, stage, schema);

    console.log(`✅ Upserted ${rows.length} contacts into ${main.id}`);
  } catch (e) {
    console.error('❌ ETL failed:', e);
    process.exit(1);
  }
})();
