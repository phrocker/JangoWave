from django.utils.decorators import method_decorator
from django.contrib.auth.decorators import login_required
from django.shortcuts import render
from django.shortcuts import render_to_response
from ctypes import cdll
from argparse import ArgumentParser
from ctypes import cdll
import ctypes
import os
import traceback
import sys
sys.path.insert(1, '/home/centos/')
sys.path.insert(1, '/usr/local/lib/python3.7/site-packages/')
import Uid_pb2
import time
import json
from django.http import JsonResponse



# cities/views.py
from django.views.generic import TemplateView, ListView, View

from stronghold.views import StrongholdPublicMixin


from .models import Query

import pysharkbite
conf = pysharkbite.Configuration()

conf.set ("FILE_SYSTEM_ROOT", "/accumulo");

zk = pysharkbite.ZookeeperInstance("muchos", "mycluster-LeaderZK-1:2181,mycluster-LeaderZK-3:2181", 1000, conf)

pysharkbite.LoggingConfiguration.enableTraceLogger()

class HomePageView(StrongholdPublicMixin,TemplateView):
    login_url = '/accounts/login/'
    redirect_field_name = 'login'
    template_name = 'home.html'

    @method_decorator(login_required)
    def dispatch(self, *args, **kwargs):
        return super(TemplateView, self).dispatch(*args, **kwargs)
    

class SearchResultsView(StrongholdPublicMixin,TemplateView):
    login_url = '/accounts/login/'
    redirect_field_name = 'login'
    model = Query
    template_name = 'search_results.html'    
    
    @method_decorator(login_required)
    def dispatch(self, *args, **kwargs):
        return super(TemplateView, self).dispatch(*args, **kwargs)

    @method_decorator(login_required)
    def get(self, request, *args, **kwargs):
    #def get_queryset(self): # new
      #import pysharkbite


#pysharkbite.LoggingConfiguration.enableTraceLogger()

      #conf = pysharkbite.Configuration()

      #conf.set ("FILE_SYSTEM_ROOT", "/accumulo");

      #zk = pysharkbite.ZookeeperInstance("muchos", "mycluster-LeaderZK-1:2181,mycluster-LeaderZK-3:2181", 1000, conf)

      user = pysharkbite.AuthInfo("root","secret", zk.getInstanceId())  
      connector = pysharkbite.AccumuloConnector(user, zk)

      entry = request.GET.get('q').lower()
      try:
        skip = int(request.GET.get('s'))
      except:
        skip=0
      print("skipping " + str(skip))
      field = request.GET.get('f')

      table = "shard"
      tableOperations = connector.tableOps(table)

      indexTableOps = connector.tableOps("shardIndex")

      auths = pysharkbite.Authorizations()
      auths.addAuthorization("MKT")


      indexScanner = indexTableOps.createScanner(auths,10)
      start=time.time()
      indexrange = pysharkbite.Range(entry)

      indexScanner.addRange(indexrange)
      indexScanner.fetchColumn(field.upper(),"")
      indexSet = indexScanner.getResultSet()

      scanner = tableOperations.createScanner(auths,30)

      ranges = []
      counts=0
      for indexKeyValue in indexSet:
       value = indexKeyValue.getValue()
       protobuf = Uid_pb2.List()
       protobuf.ParseFromString(value.get().encode())
       for uidvalue in protobuf.UID:
         shard = indexKeyValue.getKey().getColumnQualifier().split("\u0000")[0]
         datatype = indexKeyValue.getKey().getColumnQualifier().split("\u0000")[1]
         startKey = pysharkbite.Key()
         endKey = pysharkbite.Key()
         startKey.setRow(shard)
         docid = datatype + "\x00" + uidvalue;
         startKey.setColumnFamily(docid)
         endKey.setRow(shard)
         endKey.setColumnFamily(docid + "\xff")
         rng = pysharkbite.Range(startKey,True,endKey,True)
         if counts >= skip:
           scanner.addRange( rng )
         counts=counts+1
       if counts > (10+skip):
         break
      indexScanner.close() 
      combinertxt=""
        ## load the combiner from the file system and send it to accumulo
      with open('jsoncombiner.py', 'r') as file:
        combinertxt = file.read()
      combiner=pysharkbite.PythonIterator("PythonCombiner",combinertxt,100)
      scanner.addIterator(combiner)

      count=0

      wanted_items = set()
      print("Found " + str(counts))
      if counts > 0:

        resultset = scanner.getResultSet()
        events = {}
        print("awaiting results")
        for keyvalue in resultset:
          key = keyvalue.getKey()
          value = keyvalue.getValue()
          eventid = key.getColumnFamily().split("\u0000")[1];
          fieldname = key.getColumnQualifier().split("\u0000")[0];
          fieldvalue = key.getColumnQualifier().split("\u0000")[1];
          if events.get(eventid) is None:
            count=count+1
            events[eventid] = {}
          events[eventid] = value.get()
          counts=count+1
          if counts > 25:
            break
        print ("technically finished")
        scanner.close()
        for key, value in events.items():
           wanted_items.add(value)
      #return Query.objects.filter(pk__in = wanted_items)
#      return JsonResponse(events)
      nxt=""
      prv=""
      if skip > 0:
        rd = skip-11
        if rd < 0:
          rd=0
        prv="/search/?f=" + field + "&q=" + entry + "&s=" + str(rd)
      if counts > (10+skip):
        nxt="/search/?f=" + field + "&q=" + entry + "&s=" + str(counts-1)
      return render_to_response('search_results.html', {'results': wanted_items, 'time': (time.time() - start), 'prv': prv, 'nxt': nxt,'field': field,'query': entry})
# Create your views here.
