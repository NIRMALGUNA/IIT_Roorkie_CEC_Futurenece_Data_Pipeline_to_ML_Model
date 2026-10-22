"""Local integration checks of task bodies; does not execute an Airflow scheduler."""
import ast,sys,types,pathlib,contextlib,io,json,tempfile,shutil,re,csv
from datetime import datetime,timezone
package=pathlib.Path(__file__).resolve().parents[1]
temporary_directory=tempfile.TemporaryDirectory(prefix='namma_fixed_test_')
workspace=pathlib.Path(temporary_directory.name)
required_files = [
    'dags/nammamart_etl_nirmal.py', 'dags/nammamart_elt_nirmal.py',
    'include/nammamart_common.py', 'include/nammamart_rules.json',
    'include/sql/raw_append.sql', 'include/sql/stg_dimensions.sql',
    'include/sql/stg_orders.sql', 'include/sql/marts.sql',
    'include/sql/business_questions.sql',
]
for relative in required_files:
    target=workspace/relative
    target.parent.mkdir(parents=True,exist_ok=True)
    shutil.copy2(package/relative,target)
shutil.copytree(package/'include/data/nammamart',workspace/'include/data/nammamart')
sys.path.insert(0,str(workspace/'include'))
import duckdb,nammamart_common as common
sdk=types.ModuleType('airflow.sdk');sdk.dag=lambda *a,**k:lambda f:f;sdk.task=sdk.dag
sys.modules['airflow']=types.ModuleType('airflow');sys.modules['airflow.sdk']=sdk
exc=types.ModuleType('airflow.exceptions');exc.AirflowException=type('AirflowException',(Exception,),{});sys.modules['airflow.exceptions']=exc

def lift(name):
 p=workspace/'dags'/name;t=ast.parse(p.read_text());body=[]
 for n in t.body:
  if isinstance(n,ast.FunctionDef) and n.name.startswith('nammamart_'):
   for fn in n.body:
    if isinstance(fn,ast.FunctionDef):fn.decorator_list=[];body.append(fn)
  elif not isinstance(n,ast.Expr) or not isinstance(n.value,ast.Call):body.append(n)
 t.body=body;ast.fix_missing_locations(t);ns={'__file__':str(p)};exec(compile(t,str(p),'exec'),ns);return ns
etl=lift('nammamart_etl_nirmal.py');elt=lift('nammamart_elt_nirmal.py')
checks=[]
def check(label,condition):
 if not condition:raise AssertionError(label)
 checks.append(label)
def db(q,params=None):
 with duckdb.connect(str(common.WAREHOUSE)) as c:return c.execute(q,params or []).fetchall()
class TI:
 def __init__(self,xcom,success_count):self.xcom=xcom;self.success_count=success_count
 def xcom_pull(self,task_ids):return self.xcom.get(task_ids)
 def get_ti_count(self,**kwargs):return self.success_count
class DR:start_date=datetime.now(timezone.utc)
def run_etl(rid):
 v=common.validate_datasets(rid,'nammamart_etl_nirmal');m=etl['transform'](v);result=etl['load'](v,m);rep=etl['report']()
 ti=TI({'validate_and_quarantine':v,'load':result,'report':rep},10)
 ctx={'run_id':rid,'ti':ti,'dag_run':DR()};a=etl['audit'](**ctx);ti.xcom['audit']=a;etl['verify_run'](**ctx)
 return v

def run_elt(rid):
 x={}
 for name,file in elt['RAW_INPUTS'].items():x[f'load_raw_{name}']=elt['load_raw'](name,file,run_id=rid)
 d=elt['stage_dimensions'](run_id=rid);o=elt['stage_orders'](d,run_id=rid);m=elt['build_marts'](o,run_id=rid)
 x.update(stage_dimensions=d,stage_orders=o,build_marts=m);ti=TI(x,9);ctx={'run_id':rid,'ti':ti,'dag_run':DR()};a=elt['audit'](**ctx);ti.xcom['audit']=a;elt['verify_run'](**ctx)
 return o
log=io.StringIO()
with contextlib.redirect_stdout(log):
 increment=common.DATA_DIR/'orders_2026-10-01_increment.csv';original_increment=increment.read_bytes();increment.unlink()
 run_etl('baseline_etl');run_elt('baseline_elt')
 baseline_ids={r[0] for r in db('select order_id from stg_orders')}
 baseline_valid=db('select count(*) from etl_fact_orders')[0][0]
 check('Baseline staging has 2400 unique orders',len(baseline_ids)==2400)
 increment.write_bytes(original_increment)
 run_etl('increment_etl');run_elt('increment_elt')
 ids={r[0] for r in db('select order_id from stg_orders')}
 check('MERGE audit reports 150 inserts and 10 changed statuses',db("select inserted_orders,changed_status_orders from elt_order_merge_audit where run_id='increment_elt'")[0]==(150,10))
 check('MERGE adds 150 new staging order keys',len(ids-baseline_ids)==150 and len(ids)==2550)
 incrows=list(csv.DictReader(io.StringIO(original_increment.decode('utf-8-sig'))));corrected=[r['order_id'] for r in incrows if r['order_id'] in baseline_ids]
 check('All 10 corrections become RETURNED',len(corrected)==10 and all(db('select order_status from stg_orders where order_id=?',[k])[0][0]=='RETURNED' for k in corrected))
 totals_etl=db("select count(*),count(*) filter(where order_status='DELIVERED'),round(sum(net_revenue),2) from etl_fact_orders")[0]
 totals_elt=db("select count(*),count(*) filter(where order_status='DELIVERED'),sum(order_amount) filter(where order_status='DELIVERED') from stg_orders where dq_status<>'FAIL'")[0]
 check('ETL and ELT valid rows, delivered rows, revenue agree',totals_etl[:2]==totals_elt[:2] and round(float(totals_etl[2]),2)==float(totals_elt[2]))
 check('Non-delivered ETL fact margins are zero',db("select count(*) from etl_fact_orders where order_status<>'DELIVERED' and gross_margin<>0")[0][0]==0)
 check('One DQ result per run/dag/check',not db('select run_id,dag_id,check_name,count(*) from dq_results group by all having count(*)>1'))
 check('Both reconciliation paths include 8 orphan payments',db("select count(*) from etl_payment_reconciliation where run_id='increment_etl' and reason='payment has no matching order'")[0][0]==8 and db("select count(*) from mart_payment_reconciliation where reason='payment has no matching order'")[0][0]==8)
 raw_before=db('select count(*) from raw_orders')[0][0]
 run_elt('increment_elt')
 check('Same run retry does not append raw orders again',db('select count(*) from raw_orders')[0][0]==raw_before)
 run_elt('rerun_elt');run_etl('increment_etl')
 check('New run appends exactly 2572 raw order records',db('select count(*) from raw_orders')[0][0]==raw_before+2572)
 check('Repeated ETL load does not duplicate scorecard records',not db("select run_id,dag_id,check_name,count(*) from dq_results where dag_id='nammamart_etl_nirmal' group by all having count(*)>1"))
 check('Report revenue idempotent',float(db('select sum(net_revenue) from mart_daily_store_kpis')[0][0])==round(float(totals_etl[2]),2))
 check('Current order keys remain unique',not db('select order_id,count(*) from stg_orders group by order_id having count(*)>1'))
 # Round-trip original source fields, no PII is printed or exported in evidence.
 with duckdb.connect(str(common.WAREHOUSE)) as c:
  columns=[r[1] for r in c.execute("pragma table_info('raw_customers')").fetchall() if not r[1].startswith('_')]
  loaded=c.execute('select '+','.join('"'+x+'"' for x in columns)+" from raw_customers where _run_id='rerun_elt' order by _source_row_number").fetchall()
  original=list(csv.DictReader((common.DATA_DIR/'customers.csv').open(encoding='utf-8-sig')))
  check('Raw customer fields exactly equal parsed CSV values',loaded==[tuple(r[k] for k in columns) for r in original])
 # Reporting queries execute with an actual local audit row for this task-body test.
 sql=re.sub(r'--[^\n]*','',(workspace/'include/sql/business_questions.sql').read_text());query_counts=[]
 for i,q in enumerate(filter(str.strip,sql.split(';')),1):query_counts.append(len(db(q)))
 check('All 14 business SELECT statements execute',len(query_counts)==14)
 check('Rejected-row ETL query has records with local audit',query_counts[6]>0)
 # Emulate the Task SDK state response; this is not scheduler/UI evidence.
 for module,count,label in [(etl,10,'ETL'),(elt,9,'ELT')]:
  for passed in (False,True):
   ti=TI({},count if passed else count-1);rid=f'{label}_audit_{passed}';ctx={'run_id':rid,'ti':ti,'dag_run':DR()}
   result=module['audit'](**ctx);ti.xcom['audit']=result
   check(f'{label} audit reflects actual state count {passed}',result['status']==('SUCCESS' if passed else 'FAILED'))
   blocked=False
   try:module['verify_run'](**ctx)
   except exc.AirflowException:blocked=True
   check(f'{label} final gate propagates failure {passed}',blocked is not passed)
 # Failed/absent audit must also fail the final leaf.
 blocked=False
 try:etl['verify_run'](ti=TI({},0),run_id='missing_audit')
 except exc.AirflowException:blocked=True
 check('Absent audit result fails closed',blocked)
 # A source schema change must fail before staging.
 p=common.DATA_DIR/'orders.csv';original=p.read_bytes();p.write_bytes(original.replace(b'order_amount',b'total',1))
 for fn,label in [(lambda:common.validate_datasets('bad_schema_etl','nammamart_etl_nirmal'),'ETL'),(lambda:elt['load_raw']('orders','orders.csv',run_id='bad_schema_elt'),'ELT')]:
  failed=False
  try:fn()
  except ValueError:failed=True
  check(label+' schema drift fails loudly',failed)
 p.write_bytes(original)
 # Synthetic unit-level SQL precision: assert orphan receipt totals match both paths.
 a=db("select round(sum(abs(coalesce(expected_amount,0)-coalesce(successful_amount,0))),2) from etl_payment_reconciliation where run_id='increment_etl'")[0][0]
 b=db('select round(sum(abs(difference)),2) from mart_payment_reconciliation')[0][0]
 check('Reconciliation monetary totals agree including orphans',round(float(a),2)==float(b))
 # All parsed Python modules are syntactically valid.
 for p in package.rglob('*.py'):ast.parse(p.read_text())
 check('Patched Python syntax valid',True)
 september=db("select round(sum(net_revenue),2) from etl_fact_orders where order_date >= '2026-09-01' and order_date < '2026-10-01'")[0][0]
 summary={'checked_at':datetime.now(timezone.utc).isoformat(),'duckdb':duckdb.__version__,'checks_passed':len(checks),'checks':checks,'valid_orders':totals_etl[0],'delivered_orders':totals_etl[1],'all_period_revenue':str(totals_elt[2]),'september_revenue':str(september),'reconciliation_absolute_difference_total':str(b),'query_row_counts':query_counts,'airflow_scheduler_executed':False}
(package/'LOCAL_VERIFICATION.json').write_text(json.dumps(summary,indent=2))
print(json.dumps(summary,indent=2));temporary_directory.cleanup()
