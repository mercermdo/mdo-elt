// index.js  – HubSpot → BigQuery upsert (staging-table + MERGE)

require('dotenv').config();
const axios      = require('axios');
const {BigQuery} = require('@google-cloud/bigquery');

/* ── clients ────────────────────────────────────────────────────────── */
const hubspot = axios.create({
  baseURL : 'https://api.hubapi.com/crm/v3/objects/contacts',
  headers : { Authorization:`Bearer ${process.env.HUBSPOT_TOKEN}` }
});
const bq = new BigQuery({ projectId: process.env.BQ_PROJECT_ID });

/* ── helpers ────────────────────────────────────────────────────────── */
const sanitise = n =>
  (/^[^a-z]/i.test(n = n.toLowerCase().replace(/[^a-z0-9_]/g,'_')) ? 'p_'+n : n);

const hub2bq = t =>
  ({ string:'STRING', number:'FLOAT', datetime:'TIMESTAMP', date:'DATE', bool:'BOOLEAN' }[t] || 'STRING');

/* ── property catalogue ────────────────────────────────────────────── */
async function getProps(){
  let props=[],after;
  do{
    const {data}=await axios.get(
      'https://api.hubapi.com/crm/v3/properties/contacts',
      { headers:{Authorization:`Bearer ${process.env.HUBSPOT_TOKEN}`},
        params :{limit:100,after,archived:false}});
    props = props.concat(data.results);
    after  = data.paging?.next?.after;
  }while(after);
  return props;
}

/* ── sync-tracker helpers ──────────────────────────────────────────── */
async function lastSync(){
  const sql=`SELECT last_sync_timestamp
             FROM \`${process.env.BQ_PROJECT_ID}.${process.env.BQ_DATASET}.sync_tracker\`
             WHERE entity='contacts' LIMIT 1`;
  const [r]=await bq.query({ query:sql });
  return r.length
         ? new Date(r[0].last_sync_timestamp.value || r[0].last_sync_timestamp).getTime()
         : Date.now() - 30*24*60*60*1e3;  // 30 days ago on first run
}
async function saveSync(ts){
  const sql=`MERGE \`${process.env.BQ_PROJECT_ID}.${process.env.BQ_DATASET}.sync_tracker\` T
             USING (SELECT 'contacts' AS entity) S
             ON T.entity=S.entity
             WHEN MATCHED THEN UPDATE SET last_sync_timestamp=TIMESTAMP_MILLIS(${ts})
             WHEN NOT MATCHED THEN INSERT(entity,last_sync_timestamp)
             VALUES('contacts',TIMESTAMP_MILLIS(${ts}))`;
  await bq.query({ query:sql });
}

/* ── fetch contacts (single call – API supports 300+ props) ────────── */
async function fetchContacts(props){
  const params={
    limit:100,
    properties:props.map(p=>p.name).join(',')
  };
  const out={}, since=await lastSync(), now=Date.now();
  if(since){
    params.filterGroups=[{filters:[{
      propertyName:'hs_lastmodifieddate',operator:'GT',value:since.toString()
    }]}];
  }

  let after;
  do{
    params.after = after;
    const {data}=await hubspot.get('',{params});
    data.results.forEach(c=>out[c.id]={id:c.id,...(out[c.id]||{}),...c.properties});
    after=data.paging?.next?.after;
  }while(after);

  await saveSync(now);
  return Object.values(out);
}

/* ── BigQuery helpers ──────────────────────────────────────────────── */
async function ensureTable(name, schema){
  const ds   = bq.dataset(process.env.BQ_DATASET);
  const tb   = ds.table(name);
  const [ok] = await tb.exists();
  if(!ok){
    await ds.createTable(name,{schema:{fields:schema}});
    return tb;
  }
  // extend schema if new columns appeared
  const [meta] = await tb.getMetadata();
  const have   = new Set(meta.schema.fields.map(f=>f.name));
  const add    = schema.filter(f=>!have.has(f.name));
  if(add.length){
    meta.schema.fields.push(...add);
    await tb.setMetadata({schema:meta.schema});
  }
  return tb;
}

async function insertStaging(table, rows, batchSize=500){
  for(let i=0;i<rows.length;i+=batchSize){
    const slice   = rows.slice(i,i+batchSize);
    const payload = slice.map(r=>({insertId:r.id,json:r}));
    await table.insert(payload,{ignoreUnknownValues:true,skipInvalidRows:true});
  }
}

/* ── main ──────────────────────────────────────────────────────────── */
(async ()=>{
  try{
    /* 1 ─ schema build */
    const props  = await getProps();
    const schema = [
      {name:'id',type:'STRING',mode:'REQUIRED'},
      ...props.map(p=>({name:sanitise(p.name),type:hub2bq(p.type),mode:'NULLABLE'}))
    ];
    const map    = Object.fromEntries(props.map(p=>[p.name,sanitise(p.name)]));

    /* 2 ─ fetch data from HubSpot */
    const contacts = await fetchContacts(props);
    const rows = contacts.map(c=>{
      const r={id:c.id};
      for(const[k,v] of Object.entries(c)){
        if(k==='id') continue;
        r[ map[k] ] = v==='' ? null : v ?? null;
      }
      return r;
    });

    /* 3 ─ staging table (auto-dropped afterwards) */
    const ds       = bq.dataset(process.env.BQ_DATASET);
    const stageId  = `Contacts_stage_${Date.now()}`;
    await ds.createTable(stageId,{schema:{fields:schema}});
    const stageTbl = ds.table(stageId);

    await insertStaging(stageTbl, rows);

    /* 4 ─ MERGE into production table */
    const prodTbl  = await ensureTable(process.env.BQ_TABLE, schema);

    const mergeSql = `
      MERGE \`${prodTbl.id}\` T
      USING \`${stageTbl.id}\` S
      ON T.id = S.id
      WHEN MATCHED THEN UPDATE SET ${schema.map(f=>`T.${f.name}=S.${f.name}`).join(', ')}
      WHEN NOT MATCHED THEN INSERT (${schema.map(f=>f.name).join(', ')})
      VALUES (${schema.map(f=>`S.${f.name}`).join(', ')})
    `;
    await bq.query({query:mergeSql});

    /* 5 ─ drop staging table */
    await stageTbl.delete();

    console.log(`✅ Upserted ${rows.length} contacts into ${process.env.BQ_TABLE}`);
  }catch(err){
    console.error('❌ ETL failed:',err);
    process.exit(1);
  }
})();
