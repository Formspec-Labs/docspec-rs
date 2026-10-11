"""Opt-in maintained REST nested-namespace capability, no objects or deletion."""
import json,os,sys,signal,time,uuid
from pathlib import Path
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchNamespaceError
from tenacity import stop_after_attempt

def main():
 folder=Path(sys.argv[1]); suffix=str(uuid.UUID(sys.argv[2]));assert suffix==sys.argv[2] and folder.is_absolute() and not folder.exists()
 root=('docspec_generation_'+suffix.replace('-',''),); child=(*root,'qualification_probe');folder.mkdir()
 report={'passed':False,'generation':suffix,'root':root,'child':child,'steps':[],'object_requests':0,'deletions':0}
 def save(): (folder/'receipt.json').write_text(json.dumps(report,indent=2)+'\n')
 def expired(*_):raise TimeoutError('bounded namespace probe deadline')
 signal.signal(signal.SIGALRM,expired); signal.signal(signal.SIGTERM,expired)
 started=time.monotonic();catalog=None
 def step(name,func):
  event={'stage':name,'acknowledged':False};report['steps'].append(event);save();signal.alarm(20)
  try:
   result=func();event['acknowledged']=True;return result
  except BaseException as error:
   event['error_type']=type(error).__name__;event['http_status']=getattr(getattr(error.__cause__,'response',None),'status_code',None);raise
  finally:signal.alarm(0);save()
 try:
  catalog=step('connect',lambda:RestCatalog('docspec-nested-probe',**{k:os.environ[v] for k,v in {'uri':'DOCSPEC_CATALOG_URI','warehouse':'DOCSPEC_CATALOG_WAREHOUSE','token':'DOCSPEC_CATALOG_TOKEN'}.items()}))
  def call(method,*args):return getattr(RestCatalog,method).retry_with(stop=stop_after_attempt(1))(catalog,*args)
  try:step('require_root_absent',lambda:call('load_namespace_properties',root))
  except NoSuchNamespaceError:pass
  else:raise ValueError('fresh root must be absent')
  step('create_root',lambda:call('create_namespace',root))
  step('create_child',lambda:call('create_namespace',child))
  actual=step('list_children',lambda:call('list_namespaces',root));report['observed']=actual
  assert actual==[child]
  step('read_exact_child',lambda:call('load_namespace_properties',child))
  report['passed']=True
 except BaseException as error:report['error_type']=type(error).__name__
 finally:
  signal.alarm(0)
  if catalog is not None:catalog.close()
  report['elapsed_seconds']=time.monotonic()-started;save()
 print(json.dumps({'passed':report['passed']}));return 0 if report['passed'] else 1
if __name__=='__main__':raise SystemExit(main())
