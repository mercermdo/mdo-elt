// index.js  —— HubSpot ➜ BigQuery  (stage-and-merge TRUE upsert)

require('dotenv').config();
const axios      = require('axios');
const {BigQuery} = require('@google-cloud/bigquery');

/* ------------------------------------------------------------------ */
/*  0.  clients                                                       */
/* ------------------------------------------------------------------ */
const hubspot = axios.create({
  baseURL : 'https://api.hubapi.com/crm/v3/objects/contacts',
  headers : { Authorization:`Bearer ${process.env.HUBSPOT_TOKEN}` }
});
const bq = new BigQuery({ projectId: process.env.BQ_PROJECT_ID });

/* ------------------------------------------------------------------ */
/*  1.  helper functions                                              */
/* ------------------------------------------------------------------ */
const sanitise = n =>
  (/^[^a-z]/i.test(n = n.toLowerCase().replace(/[^a-z0-9_]/g, '_')) ? 'p_'+n : n);

const hub2bq = t =>
  ({ string:'STRING', number:'FLOAT', datetime:'TIMESTAMP',
     date:'DATE',  bool:'BOOLEAN' }[t] || 'STRING');

/* ------------------------------------------------------------------ */
/*  2.  property catalogue                                            */
/* ------------------------------------------------------------------ */
async function getProperties () {
  let props = [], after;
  do {
    const {data} = await axios.get(
      'https://api.hubapi.com/crm/v3/properties/contacts',
      { headers:{Authorization:`Bearer ${process.env.HUBSPOT_TOKEN}`},
        params :{limit:100, after, archived:false}});
    props = props.concat(data.results);
    after  = data.paging?.next?.after;
  } while (after);
  return props;
}

/* ------------------------------------------------------------------ */
/*  3.  last-sync tracker                                             */
/* ------------------------------------------------------------------ */
async function getLastSync () {
  const sql = `SELECT last_sync_timestamp
               FROM   \`${process.env.BQ_PROJECT_ID}.${process.env.BQ_DATASET}.sync_tracker\`
               WHERE  entity='contacts' LIMIT 1`;
  const [rows] = await bq.query({query:sql});
  return rows.length
       ? new Date(rows[0].last_sync_timestamp.value || rows[0].last_sync_timestamp).getTime()
       : Date.now() - 30*24*60*60*1000;        // 30 days ago
}

async function saveLastSync (ts) {
  const sql = `MERGE \`${process.env.BQ_PROJECT_ID}.${process.env.BQ_DATASET}.sync_tracker\` T
               USING (SELECT 'contacts' AS entity) S
               ON   T.entity=S.entity
               WHEN MATCHED THEN UPDATE SET last_sync_timestamp=TIMESTAMP_MILLIS(${ts})
               WHEN NOT MATCHED THEN INSERT(entity,last_sync_timestamp)
               VALUES('contacts', TIMESTAMP_MILLIS(${ts}))`;
  await bq.query({query:sql});
}

/* ------------------------------------------------------------------ */
/*  4.  fetch only contacts changed since last run                     */
/* ------------------------------------------------------------------ */
async function fetchContacts (props) {
  const since   = await getLastSync();
  const out     = {};
  const params  = {
    limit:100,
    properties:props.map(p=>p.name).join(','),
    filterGroups:[{filters:[{
      propertyName:'hs_lastmodifieddate', operator:'GT', value:String(since)
    }]}]
  };

  let after;
  do {
    params.after = after;
    const {data} = await hubspot.get('', {params});
    data.results.forEach(c => out[c.id] = {id:c.id, ...(out[c.id]||{}), ...c.properties});
    after = data.paging?.next?.after;
  } while (after);

  await saveLastSync(Date.now());
  return Object.values(out);
}

/* ------------------------------------------------------------------ */
/*  5.  table utilities                                               */
/* ------------------------------------------------------------------ */
async function ensureTable (name, schema) {
  const ds = bq.dataset(process.env.BQ_DATASET);
  const tb = ds.table(name);
  const [exists] = await tb.exists();
  if (!exists) {
    await ds.createTable(name, {schema:{fields:schema}});
    return tb;
  }

  const [meta] = await tb.getMetadata();
  const have   = new Set(meta.schema.fields.map(f=>f.name));
  const add    = schema.filter(f=>!have.has(f.name));
  if (add.length) {
    meta.schema.fields.push(...add);
    await tb.setMetadata({schema:meta.schema});
  }
  return tb;
}

/* ------------------------------------------------------------------ */
/*  6.  stage loader (fresh table every run)                          */
/* ------------------------------------------------------------------ */
async function streamToStage (rows, schema) {
  const ds      = bq.dataset(process.env.BQ_DATASET);
  const stageId = 'Contacts_stage';

  // drop & recreate the staging table each run
  try { await ds.table(stageId).delete(); } catch (_) {}      // ignore if not found
  await ds.createTable(stageId, {schema:{fields:schema}});

  const stage   = ds.table(stageId);
  const batched = [];
  for (const r of rows) batched.push({insertId:r.id, json:r});
  await stage.insert(batched, {ignoreUnknownValues:true, skipInvalidRows:true});
}

/* ------------------------------------------------------------------ */
/*  7.  MERGE stage -> master (true upsert)                           */
/* ------------------------------------------------------------------ */
async function mergeStageIntoMaster (schema) {
  const cols    = schema.map(f=>`\`${f.name}\``).join(', ');
  const updates = schema.filter(f=>f.name!=='id')
                        .map(  f=>`T.${f.name}=S.${f.name}`).join(', ');
  const sql = `
    MERGE \`${process.env.BQ_PROJECT_ID}.${process.env.BQ_DATASET}.Contacts\` T
    USING \`${process.env.BQ_PROJECT_ID}.${process.env.BQ_DATASET}.Contacts_stage\` S
    ON    T.id = S.id
    WHEN MATCHED THEN UPDATE SET ${updates}
    WHEN NOT MATCHED THEN INSERT (${cols}) VALUES (${cols})`;
  await bq.query({query:sql});
}

/* ------------------------------------------------------------------ */
/*  8.  main                                                          */
/* ------------------------------------------------------------------ */
(async()=>{
  try {
    /* schema --------------------------------------------------------- */
    const props   = await getProperties();
    const schema  = [
      {name:'id', type:'STRING', mode:'REQUIRED'},
      ...props.map(p=>({name:sanitise(p.name), type:hub2bq(p.type), mode:'NULLABLE'}))
    ];
    const propMap = Object.fromEntries(props.map(p=>[p.name,sanitise(p.name)]));

    /* data ----------------------------------------------------------- */
    const changed = await fetchContacts(props);
    if (!changed.length) {
      console.log('ℹ️  No contacts changed since last run');
      return;
    }

    /* rows ----------------------------------------------------------- */
    const rows = changed.map(c=>{
      const r={id:c.id};
      for(const[k,v] of Object.entries(c)) if(k!=='id') r[propMap[k]] = (v===''?null:v??null);
      return r;
    });

    /* load ----------------------------------------------------------- */
    await ensureTable('Contacts',       schema);   // evolve master if new columns
    await streamToStage(rows,           schema);   // fresh staging table
    await mergeStageIntoMaster(schema);            // true upsert

    console.log(`✅ Upserted ${rows.length} contacts`);
  } catch (e) {
    console.error('❌ ETL failed:', e);
    process.exit(1);
  }
})();
