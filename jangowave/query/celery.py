from __future__ import absolute_import, unicode_literals
import threading
from celery.signals import worker_ready
from celery.decorators import task
from django.apps import apps
import datetime
import os
from django.core.cache import caches
from celery import shared_task
from celery import Celery
from celery.decorators import periodic_task
from datetime import timedelta
import Uid_pb2
import EdgeData_pb2
import query.WritableUtils
import traceback
# set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'queryapp.settings')

app = Celery('queryapp')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object('django.conf:settings', namespace='CELERY')

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()


def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)+1):
        yield start_date + datetime.timedelta(n)

def getDateRange(days : int ):
  adjusted_date = datetime.datetime.now() + datetime.timedelta(days)
  now_date = datetime.datetime.now()
  if (days >= 0):
    return daterange(now_date,adjusted_date)
  else:
    return daterange(adjusted_date,now_date)


class ZkInstance(object):
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if ZkInstance._instance is None:
            with ZkInstance._lock:
                if ZkInstance._instance is None:
                    ZkInstance._instance = super(ZkInstance, cls).__new__(cls)
        return ZkInstance._instance

    def __init__(self):
        import sharkbite
        model = apps.get_model(app_label='query', model_name='AccumuloCluster')
        AccumuloCluster = model.objects.first()
        self.zoo_keeper = sharkbite.ZookeeperInstance(accumulo_cluster.instance, accumulo_cluster.zookeeper, 1000, conf)

    def get(self):
        return self.zoo_keeper


@shared_task
def run_edge_query(query_id):
  model = apps.get_model(app_label='query', model_name='EdgeQuery')
  objs = model.objects.filter(query_id=query_id)
  for obj in objs:
    obj.running = True
    obj.save()
    import sharkbite
    conf = sharkbite.Configuration()
    conf.set ("FILE_SYSTEM_ROOT", "/accumulo");
    model = apps.get_model(app_label='query', model_name='AccumuloCluster')
    accumulo_cluster = model.objects.first()
    if accumulo_cluster is None:
      return;
    zk = sharkbite.ZookeeperInstance(accumulo_cluster.instance, accumulo_cluster.zookeeper, 1000, conf)
    user = sharkbite.AuthInfo("root","secret", zk.getInstanceId())
    connector = sharkbite.AccumuloConnector(user, zk)
    auths = sharkbite.Authorizations()
    if obj.auths:
      for auth in obj.auths.split(","):
        auths.addAuthorization(auth)
    
    
    sres_model = apps.get_model(app_label='query', model_name='ScanResult')
    res_model = apps.get_model(app_label='query', model_name='Result')
    sr = sres_model.objects.filter(query_id=obj.query_id).first()
    if not sr:
      print("No scan result, returning")
      return
    print("here")

    graphTableOps = connector.tableOps("graph")
    scanner = graphTableOps.createScanner(auths,10)
    range = sharkbite.Range(obj.query,True,obj.query + "\uffff" + "\uffff",False) ## for now the range should be this
    scanner.addRange(range)
    resultset = scanner.getResultSet()
    count=0
    try:
      for indexKeyValue in resultset:
        value = "0"
        ## row will be the to 
        ## direction will be the cf
        to_value = ""
        direction="one"
        try:
          to_value = indexKeyValue.getKey().getRow().split("\u0000")[1]
          direction = indexKeyValue.getKey().getColumnFamily().split("/")[1]
          direction_split = direction.split("-")
          if len(direction_split) != 2 or direction_split[0] == direction_split[1]:
            continue
          protobuf = EdgeData_pb2.EdgeValue()
          protobuf.ParseFromString(indexKeyValue.getValue().get_bytes())
          value = str(protobuf.count) + "/" + protobuf.uuid_string
        except Exception as e: 
          print(e)
          continue
        except:
          continue
        scanresult = res_model.objects.create(scanResult=sr,value=value,row=to_value,cf=direction,cq=indexKeyValue.getKey().getColumnQualifier())
        scanresult.save()
        count=count+1
        if count > 1000:
          break
      sr.is_finished=True
      sr.save()
      scanner.close()
    except Exception as e: print(e)
    except:
      print("An error occurred")
      pass ## user does not have PROV
    obj.running = False
    obj.finished = True
    obj.save()
  

@shared_task
def initial_upload():
  get_uploads()

@periodic_task(run_every=timedelta(minutes=10))
def get_uploads():
  model = apps.get_model(app_label='query', model_name='FileUpload')
  objs = model.objects.filter(status="NEW")
  haveNew = False
  for obj in objs:
      if obj.status == "NEW":
        caches['eventcount'].set("ingestcomplete",95)
        haveNew=True
  if not haveNew:
    caches['eventcount'].set("ingestcomplete",100)
    return
  import sharkbite
  conf = sharkbite.Configuration()
  conf.set ("FILE_SYSTEM_ROOT", "/accumulo");
  model = apps.get_model(app_label='query', model_name='AccumuloCluster')
  accumulo_cluster = model.objects.first()
  if accumulo_cluster is None:
    return
  zk = sharkbite.ZookeeperInstance(accumulo_cluster.instance, accumulo_cluster.zookeeper, 1000, conf)
  user = sharkbite.AuthInfo("root","secret", zk.getInstanceId())
  connector = sharkbite.AccumuloConnector(user, zk)
  auths = sharkbite.Authorizations()
  auths.addAuthorization("PROV")
  indexTableOps = connector.tableOps("provenanceIndex")
  indexScanner = indexTableOps.createScanner(auths,10)
  indexrange = sharkbite.Range()
  indexScanner.addRange(indexrange)
  indexScanner.fetchColumn("CONTENTURI","")
  indexScanner.fetchColumn("TRANSITURI","")
  indexSet = indexScanner.getResultSet()
  count=0
  usercount=0
  try:
    for indexKeyValue in indexSet:
      if indexKeyValue.getKey().getColumnFamily() == "CONTENTURI":
        count=count+1
      else:
        usercount=usercount+1
    if count > 0:
      caches['eventcount'].set("ingestcount",count,3600*48)
    if usercount > 0:
      caches['eventcount'].set("useruploads",usercount,3600*48)
    indexScanner.close()
  except:
    pass ## user does not have PROV
  
@periodic_task(run_every=timedelta(seconds=10))
def check():
  model = apps.get_model(app_label='query', model_name='FileUpload')
  objs = model.objects.filter(status="NEW")
  for obj in objs:
      if obj.status == "NEW":
        import sharkbite
        conf = sharkbite.Configuration()
        conf.set ("FILE_SYSTEM_ROOT", "/accumulo");
        model = apps.get_model(app_label='query', model_name='AccumuloCluster')
        accumulo_cluster = model.objects.first()
        if accumulo_cluster is None:
          return;
        zk = sharkbite.ZookeeperInstance(accumulo_cluster.instance, accumulo_cluster.zookeeper, 1000, conf)
        user = sharkbite.AuthInfo("root","secret", zk.getInstanceId())
        connector = sharkbite.AccumuloConnector(user, zk)

        indexTableOps = connector.tableOps("provenanceIndex")

        auths = sharkbite.Authorizations()
        auths.addAuthorization("PROV")

        indexScanner = indexTableOps.createScanner(auths,2)

        indexrange = sharkbite.Range(str(obj.uuid))

        indexScanner.addRange(indexrange)
        indexSet = indexScanner.getResultSet()

        rangelist = list()
        provops = connector.tableOps("provenance")
        scanner = provops.createScanner(auths,10)
        for indexKeyValue in indexSet:
         value = indexKeyValue.getValue()
         protobuf = Uid_pb2.List()
         protobuf.ParseFromString(value.get().encode())
         for uidvalue in protobuf.UID:
              shard = indexKeyValue.getKey().getColumnQualifier().split("\u0000")[0]
              datatype = indexKeyValue.getKey().getColumnQualifier().split("\u0000")[1]
              startKey = sharkbite.Key()
              stopKey = sharkbite.Key()
              startKey.setRow(shard)
              stopKey.setRow(shard)
              startKey.setColumnFamily(datatype + "\x00" + uidvalue)
              stopKey.setColumnFamily(datatype + "\x00" + uidvalue + "\xff")
              rangelist.append( sharkbite.Range(startKey,True,stopKey,False))
              scanner = provops.createScanner(auths,10)              
              scanner.addRange( sharkbite.Range(startKey,True,stopKey,False))
              resultset = scanner.getResultSet()
              for keyvalue in resultset:
                key = keyvalue.getKey()
                value = keyvalue.getValue()
                eventid = key.getColumnFamily().split("\u0000")[1];
                fieldname = key.getColumnQualifier().split("\u0000")[0];
                fieldvalue = key.getColumnQualifier().split("\u0000")[1];
                if (fieldname == "EVENTTYPE"):
                  if fieldvalue == "DROP":
                    obj.status="COMPLETE" 
                    obj.save()
                    break
              scanner.close()

        indexScanner.close()

@periodic_task(run_every=timedelta(seconds=60))
@shared_task
def populateFieldMetadata():
      print("322")
      metadata = caches['metadata'].get("fieldchart")
      if not metadata is None and not (isinstance(metadata, str) and (len(metadata)==0 or metadata=="{}" )):
        print("returning " + metadata)
        return metadata
      import time
      import sharkbite
      conf = sharkbite.Configuration()
      conf.set ("FILE_SYSTEM_ROOT", "/accumulo");
      model = apps.get_model(app_label='query', model_name='AccumuloCluster')
      accumulo_cluster = model.objects.first()
      if accumulo_cluster is None:
        return
        print("270")
      zk = sharkbite.ZookeeperInstance(accumulo_cluster.instance, accumulo_cluster.zookeeper, 1000, conf)
      user = sharkbite.AuthInfo("root","secret", zk.getInstanceId())
      connector = sharkbite.AccumuloConnector(user, zk)

      indexTableOps = connector.tableOps("datawave.metadata")

      auths = sharkbite.Authorizations()

      indexScanner = indexTableOps.createScanner(auths,100)
      start=time.time()
      indexrange = sharkbite.Range()

      indexScanner.addRange(indexrange)
      indexScanner.fetchColumn("f","")

      combinertxt=""
      print("287")
      indexSet = indexScanner.getResultSet()

      counts=0
      mapping={}
      for indexKeyValue in indexSet:
       value = indexKeyValue.getValue()
       key = indexKeyValue.getKey()
       if key.getColumnFamily() == "f":
         day = key.getColumnQualifier().split("\u0000")[1]
         dt = key.getColumnQualifier().split("\u0000")[0]

         if key.getRow() in mapping:
            pass # mapping[key.getRow()].append(int( value.get() ))
         else:
           try:

             binstream = query.WritableUtils.DataInputStream( query.WritableUtils.ByteArrayInputStream(value.get_bytes()) )
             val = query.WritableUtils.readVLong(binstream)
             mapping[key.getRow()] = list()
             mapping[key.getRow()].append(val)
           except:
             traceback.print_exc()
             pass
      import json
      ret = json.dumps(mapping)
      caches['metadata'].set("fieldchart",ret,3600*1)
      return ret



@shared_task
def buildon_startup():
      import sharkbite
      import time
      conf = sharkbite.Configuration()
      conf.set ("FILE_SYSTEM_ROOT", "/accumulo");
      model = apps.get_model(app_label='query', model_name='AccumuloCluster')
      accumulo_cluster = model.objects.first()
      if accumulo_cluster is None:
        return;
      zk = sharkbite.ZookeeperInstance(accumulo_cluster.instance, accumulo_cluster.zookeeper, 1000, conf)
      user = sharkbite.AuthInfo("root","secret", zk.getInstanceId())
      connector = sharkbite.AccumuloConnector(user, zk)
      queryRanges = list()
      #last seven days
      for dateinrange in getDateRange(-15):
        shardbegin = dateinrange.strftime("%Y%m%d")
        if caches['eventcount'].get(shardbegin) is None or caches['eventcount'].get(shardbegin)==0:
          queryRanges.append(shardbegin)
        else:
          pass # don't add to range

      if len(queryRanges) > 0:
        ## all is cached

        user = sharkbite.AuthInfo("root","secret", zk.getInstanceId())
        connector = sharkbite.AccumuloConnector(user, zk)

        indexTableOps = connector.tableOps("datawave.metadata")

        auths = sharkbite.Authorizations()
        

        indexScanner = indexTableOps.createScanner(auths,100)
        start=time.time()
        for dt in queryRanges:
          indexrange = sharkbite.Range(dt,True,dt+"\uffff",False)
          indexScanner.addRange(indexrange)
        indexScanner.fetchColumn("e","")

        combinertxt=""
        ## load the combiner from the file system and send it to accumulo
        with open('metricscombiner.py', 'r') as file:
          combinertxt = file.read()
        combiner=sharkbite.PythonIterator("MetadataCounter",combinertxt,200)
        #indexScanner.addIterator(combiner)
        indexSet = indexScanner.getResultSet()

        counts=0
        mapping={}
        for indexKeyValue in indexSet:
         value = indexKeyValue.getValue()
         key = indexKeyValue.getKey()
         if key.getColumnFamily() == "e":
           dt = key.getRow().split("_")[0]
           binstream = query.WritableUtils.DataInputStream( query.WritableUtils.ByteArrayInputStream(value.get_bytes()) )
           val = query.WritableUtils.readVLong(binstream)  
           if dt in mapping:
              mapping[dt] += val
           else:
             mapping[dt] = val
        arr = [None] * len(mapping.keys())
        for field in mapping:
          caches['eventcount'].set(field,str(mapping[field]),3600*48)

@periodic_task(run_every=timedelta(minutes=10))
@shared_task
def populateMetadata():
      import sharkbite      
      conf = sharkbite.Configuration()
      conf.set ("FILE_SYSTEM_ROOT", "/accumulo");

      model = apps.get_model(app_label='query', model_name='AccumuloCluster')
      accumulo_cluster = model.objects.first()
      if accumulo_cluster is None:
        return
      zk = sharkbite.ZookeeperInstance(accumulo_cluster.instance, accumulo_cluster.zookeeper, 1000, conf)
      user = sharkbite.AuthInfo("root","secret", zk.getInstanceId())
      connector = sharkbite.AccumuloConnector(user, zk)

      indexTableOps = connector.tableOps("datawave.metadata")

      auths = sharkbite.Authorizations()


      indexScanner = indexTableOps.createScanner(auths,100)
      indexrange = sharkbite.Range()

      indexScanner.addRange(indexrange)
      indexScanner.fetchColumn("f","")

      combinertxt=""
      indexSet = indexScanner.getResultSet()
      import json
      counts=0
      mapping={}
      for indexKeyValue in indexSet:
       value = indexKeyValue.getValue()
       key = indexKeyValue.getKey()
       if key.getColumnFamily() == "f":
         day = key.getColumnQualifier().split("\u0000")[1]
         dt = key.getColumnQualifier().split("\u0000")[0]
         binstream = query.WritableUtils.DataInputStream( query.WritableUtils.ByteArrayInputStream(value.get_bytes()) )
         val = query.WritableUtils.readVLong(binstream)
         if day in mapping:
           if key.getRow() in mapping[day]:
            try:
              mapping[day][key.getRow()] += val
            except:
              pass
           else:
            try:
              mapping[day][key.getRow()] = val
            except:
              pass
         else:
           mapping[day]={}
           try:
             mapping[day][key.getRow()] = val
           except:
             pass
      caches['metadata'].set("field",json.dumps(mapping),3600*1)
      return json.dumps(mapping)


@app.task(bind=True)
def debug_task(self):
    print('Request: {0!r}'.format(self.request))
