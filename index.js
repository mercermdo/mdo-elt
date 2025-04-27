// index.js  – dynamic, self-expanding schema (batch-inserts ≤500 rows)

require('dotenv').config();
const axios = require('axios');
const {BigQuery} = require('@google-cloud/bigquery');

const hubspot = axios.create({
  baseURL : 'https://api.hubapi.com/crm/v3/objects/contacts',
  headers : {Authorization:`Bearer ${process.env.HUBSPOT_TOKEN}`}
});
const bq = new BigQuery({projectId:process.env.BQ_PROJECT_ID});

/* helpers ------------------------------------------------------------- */
const sanitise = n =>
  (/^[^a-z]/i.test(n=n.toLowerCase().replace(/[^a-z0-9_]/g,'_')) ? 'p_'+n : n);
const hub2bq = t =>
  ({string:'STRING',number:'FLOAT',datetime:'TIMESTAMP',date:'DATE',bool:'BOOLEAN'}[t]||'STRING');

/* property catalogue -------------------------------------------------- */
async function getProps(){
  let props=[],after;
  do{
    const {data}=await axios.get(
      'https://api.hubapi.com/crm/v3/properties/contacts',
      {headers:{Authorization:`Bearer ${process.env.HUBSPOT_TOKEN}`},
       params:{limit:100,after,archived:false}});
    props=props.concat(data.results); after=data.paging?.next?.after;
  }while(after);
  return props;
}

/* sync-tracker helpers ------------------------------------------------ */
async function lastSync(){
  const sql=`SELECT last_sync_timestamp
             FROM \`${process.env.BQ_PROJECT_ID}.${process.env.BQ_DATASET}.sync_tracker\`
             WHERE entity='contacts' LIMIT 1`;
  const [r]=await bq.query({query:sql});
  return r.length
    ? new Date(r[0].last_sync_timestamp.value||r[0].last_sync_timestamp).getTime()
    : Date.now()-30*24*60*60*1e3;
}
async function saveSync(ts){
  const sql=`MERGE \`${process.env.BQ_PROJECT_ID}.${process.env.BQ_DATASET}.sync_tracker\` T
             USING (SELECT 'contacts' AS entity) S
             ON T.entity=S.entity
             WHEN MATCHED THEN UPDATE SET last_sync_timestamp=TIMESTAMP_MILLIS(${ts})
             WHEN NOT MATCHED THEN INSERT(entity,last_sync_timestamp)
             VALUES('contacts',TIMESTAMP_MILLIS(${ts}))`;
  await bq.query({query:sql});
}

/* fetch contacts in one pass ----------------------------------------- */
async function fetchContacts(props){
  const params={
    limit:100,
    properties:props.map(p=>p.name).join(',')
  };
  const contacts={}, since=await lastSync(), now=Date.now();
  if(since) params.filterGroups=[{filters:[{
    propertyName:'hs_lastmodifieddate',operator:'GT',value:since.toString()}]}];

  let after;
  do{
    params.after=after;
    const {data}=await hubspot.get('',{params});
    data.results.forEach(c=>contacts[c.id]={id:c.id,...(contacts[c.id]||{}),...c.properties});
    after=data.paging?.next?.after;
  }while(after);

  await saveSync(now);
  return Object.values(contacts);
}

/* ensure-table & schema-evolution ------------------------------------ */
async function ensureTable(schema){
  const ds=bq.dataset(process.env.BQ_DATASET);
  const tb=ds.table(process.env.BQ_TABLE);
  const [exists]=await tb.exists();
  if(!exists){
    await ds.createTable(process.env.BQ_TABLE,{schema:{fields:schema}});
    return tb;
  }
  const [meta]=await tb.getMetadata();
  const have=new Set(meta.schema.fields.map(f=>f.name));
  const add =schema.filter(f=>!have.has(f.name));
  if(add.length){
    meta.schema.fields.push(...add);
    await tb.setMetadata({schema:meta.schema});
  }
  return tb;
}

/* NEW: insert rows in ≤500-row batches -------------------------------- */
async function insertBatches(table, rows, batchSize=500){
  for(let i=0;i<rows.length;i+=batchSize){
    const slice=rows.slice(i,i+batchSize);
    await table.insert(slice,{ignoreUnknownValues:true,skipInvalidRows:true});
  }
}

/* main ---------------------------------------------------------------- */
(async()=>{
 try{
   const props  = await getProps();
   const schema = [{name:'id',type:'STRING',mode:'REQUIRED'},
                   ...props.map(p=>({name:sanitise(p.name),type:hub2bq(p.type),mode:'NULLABLE'}))];

   const data = await fetchContacts(props);
   const map  = Object.fromEntries(props.map(p=>[p.name,sanitise(p.name)]));
   const rows = data.map(c=>{
     const r={id:c.id};
     for(const[k,v]of Object.entries(c)) if(k!=='id') r[map[k]]=v??null;
     return r;
   });

   const table = await ensureTable(schema);
   if(rows.length) await insertBatches(table, rows);      // ← uses new helper
   console.log(`✅ Uploaded ${rows.length} contacts with ${schema.length} columns`);
 }catch(e){
   console.error('❌ ETL failed:',e); process.exit(1);
 }
})();
