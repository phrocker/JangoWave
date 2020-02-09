
from __future__ import absolute_import, unicode_literals
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
#strftime("%Y%m%d")
  if (days >= 0):
    return daterange(now_date,adjusted_date)
  else:
    return daterange(adjusted_date,now_date)

@periodic_task(run_every=timedelta(minutes=45))
def pouplateEventCountMetadata():
      import pysharkbite
      import time
      try:
        AccumuloCluster = apps.get_model(app_label='query', model_name='AccumuloCluster')
        conf = pysharkbite.Configuration()
        conf.set ("FILE_SYSTEM_ROOT", "/accumulo");
        zk = pysharkbite.ZookeeperInstance(AccumuloCluster.objects.first().instance, AccumuloCluster.objects.first().zookeeper, 1000, conf)
        user = pysharkbite.AuthInfo(AccumuloCluster.objects.first().user,AccumuloCluster.objects.first().password, ZkInstance().get().getInstanceId())
        connector = pysharkbite.AccumuloConnector(user, zk)
        queryRanges = list()
        #last seven days
        for dateinrange in getDateRange(-15):
          shardbegin = dateinrange.strftime("%Y%m%d")
          if caches['eventcount'].get(shardbegin) is None:
            queryRanges.append(shardbegin)
          else:
            pass # don't add to range

        if len(queryRanges) > 0:
          ## all is cached

          user = pysharkbite.AuthInfo("root","secret", zk.getInstanceId())
          connector = pysharkbite.AccumuloConnector(user, zk)

          indexTableOps = connector.tableOps("DatawaveMetrics")

          auths = pysharkbite.Authorizations()
          auths.addAuthorization("MTRCS")

          indexScanner = indexTableOps.createScanner(auths,100)
          start=time.time()
          for dt in queryRanges:
            indexrange = pysharkbite.Range(dt,True,dt+"\uffff",False)
            indexScanner.addRange(indexrange)
          indexScanner.fetchColumn("EVENT_COUNT","")

          combinertxt=""
          ## load the combiner from the file system and send it to accumulo
          with open('metricscombiner.py', 'r') as file:
            combinertxt = file.read()
          combiner=pysharkbite.PythonIterator("MetadataCounter",combinertxt,200)
          indexScanner.addIterator(combiner)
          indexSet = indexScanner.getResultSet()

          counts=0
          mapping={}
          for indexKeyValue in indexSet:
            value = indexKeyValue.getValue()
            key = indexKeyValue.getKey()
            if key.getColumnFamily() == "EVENT_COUNT":
              dt = key.getRow().split("_")[0]
              if dt in mapping:
                  mapping[dt] += int(value.get())
              else:
                mapping[dt] = int(value.get())
          arr = [None] * len(mapping.keys())
          for field in mapping:
            caches['eventcount'].set(field,str(mapping[field]),3600*1)
      except:
        pass


@periodic_task(run_every=timedelta(seconds=5))
def check():
  model = apps.get_model(app_label='query', model_name='FileUpload')
  AccumuloCluster = apps.get_model(app_label='query', model_name='AccumuloCluster')
  objs = model.objects.filter(status="NEW")
  for obj in objs:
      if obj.status == "NEW":
        import pysharkbite
        try:
          conf = pysharkbite.Configuration()
          conf.set ("FILE_SYSTEM_ROOT", "/accumulo");
          zk = pysharkbite.ZookeeperInstance(AccumuloCluster.objects.first().instance, AccumuloCluster.objects.first().zookeeper, 1000, conf)
          user = pysharkbite.AuthInfo(AccumuloCluster.objects.first().user,AccumuloCluster.objects.first().password, ZkInstance().get().getInstanceId())
          connector = pysharkbite.AccumuloConnector(user, zk)

          indexTableOps = connector.tableOps("provenanceIndex")

          auths = pysharkbite.Authorizations()
          auths.addAuthorization("PROV")

          indexScanner = indexTableOps.createScanner(auths,2)

          indexrange = pysharkbite.Range(str(obj.uuid))

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
                startKey = pysharkbite.Key()
                stopKey = pysharkbite.Key()
                startKey.setRow(shard)
                stopKey.setRow(shard)
                startKey.setColumnFamily(datatype + "\x00" + uidvalue)
                stopKey.setColumnFamily(datatype + "\x00" + uidvalue + "\xff")
                rangelist.append( pysharkbite.Range(startKey,True,stopKey,False))
                scanner = provops.createScanner(auths,10)              
                scanner.addRange( pysharkbite.Range(startKey,True,stopKey,False))
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
        except:
          pass

@shared_task
@periodic_task(run_every=timedelta(seconds=60))
def populateFieldMetadata():
      if not caches['metadata'].get("fieldchart") is None:
        return caches['metadata'].get("fieldchart")
      import time
      AccumuloCluster = apps.get_model(app_label='query', model_name='AccumuloCluster')
      import pysharkbite
      conf = pysharkbite.Configuration()
      conf.set ("FILE_SYSTEM_ROOT", "/accumulo");
      zk = pysharkbite.ZookeeperInstance(AccumuloCluster.objects.first().instance, AccumuloCluster.objects.first().zookeeper, 1000, conf)
      user = pysharkbite.AuthInfo(AccumuloCluster.objects.first().user,AccumuloCluster.objects.first().password, ZkInstance().get().getInstanceId())
      connector = pysharkbite.AccumuloConnector(user, zk)

      indexTableOps = connector.tableOps("DatawaveMetadata")

      auths = pysharkbite.Authorizations()

      indexScanner = indexTableOps.createScanner(auths,100)
      start=time.time()
      indexrange = pysharkbite.Range()

      indexScanner.addRange(indexrange)
      indexScanner.fetchColumn("f","")

      combinertxt=""
        ## load the combiner from the file system and send it to accumulo
      with open('countgatherer.py', 'r') as file:
        combinertxt = file.read()
      combiner=pysharkbite.PythonIterator("MetadataCounter",combinertxt,200)
      indexScanner.addIterator(combiner)
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
             val = int( value.get() )
             mapping[key.getRow()] = list()
             mapping[key.getRow()].append(int( value.get() ))
           except:
             pass
      import json
      ret = json.dumps(mapping)
      caches['metadata'].set("fieldchart",ret,3600)
      return ret

@shared_task
def populateMetadata():
      if not caches['metadata'].get("field") is None:
        return caches['metadata'].get("field")
      mapping={}
      import pysharkbite      
      try:
        conf = pysharkbite.Configuration()
        conf.set ("FILE_SYSTEM_ROOT", "/accumulo");
        AccumuloCluster = apps.get_model(app_label='query', model_name='AccumuloCluster')
        zk = pysharkbite.ZookeeperInstance(AccumuloCluster.objects.first().instance, AccumuloCluster.objects.first().zookeeper, 1000, conf)
        user = pysharkbite.AuthInfo(AccumuloCluster.objects.first().user,AccumuloCluster.objects.first().password, ZkInstance().get().getInstanceId())
        connector = pysharkbite.AccumuloConnector(user, zk)

        indexTableOps = connector.tableOps("DatawaveMetadata")

        auths = pysharkbite.Authorizations()


        indexScanner = indexTableOps.createScanner(auths,100)
        indexrange = pysharkbite.Range()

        indexScanner.addRange(indexrange)
        indexScanner.fetchColumn("f","")

        combinertxt=""
          ## load the combiner from the file system and send it to accumulo
        with open('countgatherer.py', 'r') as file:
          combinertxt = file.read()
        combiner=pysharkbite.PythonIterator("MetadataCounter",combinertxt,200)
        indexScanner.addIterator(combiner)
        indexSet = indexScanner.getResultSet()
        import json
        counts=0
        for indexKeyValue in indexSet:
          value = indexKeyValue.getValue()
          key = indexKeyValue.getKey()
          if key.getColumnFamily() == "f":
            day = key.getColumnQualifier().split("\u0000")[1]
            dt = key.getColumnQualifier().split("\u0000")[0]
            if day in mapping:
              if key.getRow() in mapping[day]:
                try:
                  mapping[day][key.getRow()] += int( value.get() )
                except:
                  pass
              else:
                try:
                  mapping[day][key.getRow()] = int( value.get() )
                except:
                  pass
            else:
              mapping[day]={}
              try:
                mapping[day][key.getRow()] = int( value.get() )
              except:
                pass
        caches['metadata'].set("field",json.dumps(mapping),3600)
      except:
        pass
      return json.dumps(mapping)


@app.task(bind=True)
def debug_task(self):
    print('Request: {0!r}'.format(self.request))
