import random
import uuid
import asyncio
import heapq
import ctypes
import itertools
import os
import traceback
import sys
import Uid_pb2
import time
import datetime
import json
import requests
from django.utils.decorators import method_decorator

from decimal import Decimal
from django.core import serializers
from django.contrib.auth.decorators import user_passes_test, login_required
from django.template import RequestContext
from django.core.cache import caches
from django.shortcuts import render
from django import http
from django.http import HttpResponseRedirect,JsonResponse
from django.shortcuts import render_to_response
from ctypes import cdll
from argparse import ArgumentParser

from sortedcontainers import SortedList, SortedSet, SortedDict
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
from django.views.generic import TemplateView, ListView, View
from stronghold.views import StrongholdPublicMixin
import threading
from .models import FileUpload, AccumuloCluster, Query, UserAuths, Auth, IngestConfiguration,  Result, ScanResult, EdgeQuery, DatawaveWebservers
from .forms import DocumentForm
from .rangebuilder import *
from luqum.parser import lexer, parser, ParseError
from luqum.pretty import prettify
from luqum.utils import UnknownOperationResolver, LuceneTreeVisitorV2
from luqum.exceptions import OrAndAndOnSameLevel
from luqum.tree import OrOperation, AndOperation, UnknownOperation
from luqum.tree import Word  # noqa: F401

from dwython import query

from .celery import run_edge_query

from collections import deque
import queue
import concurrent.futures

resolver = UnknownOperationResolver()

import faulthandler
faulthandler.enable()

#import jnius_config
#jnius_config.set_classpath('.', '/home/centos/datawave-dev-3.1.0-SNAPSHOT/lib/*')
#import jnius



import sharkbite

class JSONResponseMixin(object):
    def render_to_response(self, context):
        "Returns a JSON response containing 'context' as payload"
        return self.get_json_response(self.convert_context_to_json(context))

    def get_json_response(self, content, **httpresponse_kwargs):
        "Construct an `HttpResponse` object."
        return http.HttpResponse(
            content, content_type="application/json", **httpresponse_kwargs
        )

    def convert_context_to_json(self, context):
        "Convert the context dictionary into a JSON object"
        return json.dumps(context, cls=ComplexEncoder)

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
        self.zk = sharkbite.ZookeeperInstance(AccumuloCluster.objects.first().instance, AccumuloCluster.objects.first().zookeeper, 1000, conf)

    def get(self):
        return self.zk

conf = sharkbite.Configuration()

conf.set ("FILE_SYSTEM_ROOT", "/accumulo");


#sharkbite.LoggingConfiguration.enableTraceLogger()

class HomePageView(StrongholdPublicMixin,TemplateView):
    login_url = '/accounts/login/'
    redirect_field_name = 'login'
    template_name = 'home.html'

    @method_decorator(login_required)
    def dispatch(self, *args, **kwargs):
      return super(TemplateView, self).dispatch(*args, **kwargs)

    @method_decorator(login_required)
    def get(self, request, *args, **kwargs):
      userAuths = set()
      try:
        auths =  UserAuths.objects.get(name=request.user)
        for authset in auths.authorizations.all():
            userAuths.add(authset)
      except:
        pass
      context = { "admin": request.user.is_superuser, "authenticated":True, 'userAuths': userAuths }
      return render(request,self.template_name,context)



class UserAuthsView(TemplateView):
    model = UserAuths
    login_url = '/accounts/login/'
    redirect_field_name = 'login'
    template_name = 'authorizations.html'

    @method_decorator(login_required)
    def dispatch(self, *args, **kwargs):
     return super(TemplateView, self).dispatch(*args, **kwargs)

    def get_queryset(self):
      return UserAuths.objects.filter(user=request.user)


class MetadataView(StrongholdPublicMixin,TemplateView):
    login_url = '/accounts/login/'
    redirect_field_name = 'login'
    model = Query
    template_name = 'data.html'

    @method_decorator(login_required)
    def dispatch(self, *args, **kwargs):
        return super(TemplateView, self).dispatch(*args, **kwargs)

    @method_decorator(login_required)
    def get(self, request, *args, **kwargs):
      cachedVal = caches['eventcount'].get("ingestcount")
      if cachedVal is None:
          ingestcount = 0
      else:
          ingestcount = int(cachedVal)
      cachedVal = caches['eventcount'].get("ingestcomplete")
      if cachedVal is None:
          ingestcomplete = 0
      else:
          ingestcomplete = int(cachedVal)

      cachedVal = caches['eventcount'].get("useruploads")
      if cachedVal is None:
          useruploads = 0
      else:
          useruploads = int(cachedVal)

      context = {'useruploads':useruploads,'ingestcomplete': ingestcomplete,'ingestcount': ingestcount, "admin": request.user.is_superuser, "authenticated":True}
      return render(request,self.template_name,context)


class EdgeQueryView(StrongholdPublicMixin,TemplateView):
    login_url = '/accounts/login/'
    redirect_field_name = 'login'
    model = Query
    query_template = 'edge_query.html'
    result_template = 'edge_results.html'

    @method_decorator(login_required)
    def dispatch(self, *args, **kwargs):
        return super(TemplateView, self).dispatch(*args, **kwargs)

    @method_decorator(login_required)
    def get(self, request, *args, **kwargs):
      query = request.GET.get("query")
      original_query = request.GET.get("originalquery")
      query_id = request.GET.get("query_id")
      auths= request.GET.get("authstring")
      if not auths:
        auths=""
      if not query:
        if (not query_id):
          context = {"query_id" : query_id, "auths" : auths, "admin": request.user.is_superuser, "authenticated":True}
          return render(request,self.query_template,context)
        else:
          context = {"query_id" : query_id, "auths" : auths, "admin": request.user.is_superuser, "authenticated":True, "query" : original_query}
          return render(request,self.result_template,context)
      else:
        eq = EdgeQuery.objects.create(query_id=str(uuid.uuid4()), query=query, auths=auths, running=False, finished=False)
        eq.save
        sr = ScanResult.objects.create(user=request.user,query_id=eq.query_id,authstring=auths,is_finished=False)
        sr.save()
        run_edge_query.delay(eq.query_id)
        context = {"query_id" : eq.query_id, "auths" : auths, "query" : query, "admin": request.user.is_superuser, "authenticated":True}
        url = "/edge/?query_id=" + str(eq.query_id) + "&auths=" + auths + "&originalquery=" + query
        return HttpResponseRedirect(url)
       # return render(request,self.result_template,context)

    

class EdgeQueryResults(JSONResponseMixin,TemplateView):
    login_url = '/accounts/login/'
    redirect_field_name = 'login'
    model = Query

    @method_decorator(login_required)
    def dispatch(self, *args, **kwargs):
        return super(TemplateView, self).dispatch(*args, **kwargs)

    @method_decorator(login_required)
    def get(self, request, *args, **kwargs):
      query_id = request.GET.get("query_id")

      context = {}
      context["is_finished"] = False
      direction_mapping = dict()
      ## row will be the to
      ## direction will be the cf
      ## date will be cq
      ## value will be count/uuid

      if query_id:
        rez = ScanResult.objects.filter(query_id=query_id,user=request.user).first()
        if (rez):
          context["is_finished"] = rez.is_finished
          for res in rez.result_set.all():
            value_split = res.value.split("/")
            if not res.cf in direction_mapping:
              direction_mapping[ res.cf ] = dict() 
            if not res.row in direction_mapping[res.cf]:
              direction_mapping[res.cf][res.row] = list()
              direction_mapping[res.cf][res.row].append( int( value_split[0] ) )
            else:
              direction_mapping[res.cf][res.row].append( int( value_split[0] ) )
          context["results"] = direction_mapping
      return self.render_to_response(context)
  

class ComplexEncoder(json.JSONEncoder):
    """Always return JSON primitive."""

    def default(self, obj):
        try:
            return super(ComplexEncoder, obj).default(obj)
        except TypeError:
            if hasattr(obj, "pk"):
                return obj.pk
            return str(obj)




def daterange(start_date, end_date):
    for n in range(0,int ((end_date - start_date).days)+1):
        yield start_date + datetime.timedelta(n)

def getDateRange(days : int ):
  adjusted_date = datetime.datetime.now() + datetime.timedelta(days)
  now_date = datetime.datetime.now()
  if (days >= 0):
    return daterange(now_date,adjusted_date)
  else:
    return daterange(adjusted_date,now_date)


class MetadataEventCountsView(JSONResponseMixin,TemplateView):
    login_url = '/accounts/login/'
    redirect_field_name = 'login'
    model = Query
    template_name = 'data.html'

    def get_context_data(self,**kwargs):#,request,*args, **kwargs):
        context = super(MetadataEventCountsView, self).get_context_data(**kwargs)
        tpl = self.get_data()
        context.update({"labels": tpl[0], "datasets": tpl[1]})
        return context
    def get_data(self): #, request, *args, **kwargs):
      colors = ["#"+''.join([random.choice('0123456789ABCDEF') for j in range(6)]) for i in range(16)]
      counts=0
      fields = [None] * 16
      arr = [None] * 16
      for dateinrange in getDateRange(-15):
        dt = dateinrange.strftime("%Y%m%d")
        fields[counts]=dt
        cachedVal = caches['eventcount'].get(dt)
        if cachedVal is None:
          numeric = 0
        else:
          numeric = int(cachedVal)
        arr[counts] = numeric
        counts=counts+1
      returnval = [1]
      ret = {}
      ret["backgroundColor"]=colors
      ret["label"] = "Distribution of data"
      ret["data"] = arr
      returnval[0] = ret
      return (fields,returnval) #render_to_response('data.html', { 'metadata': mapping })




class MetadataChartView(JSONResponseMixin,TemplateView):
    login_url = '/accounts/login/'
    redirect_field_name = 'login'
    model = Query
    template_name = 'data.html'


    def get_context_data(self,**kwargs):#,request,*args, **kwargs):
        context = super(MetadataChartView, self).get_context_data(**kwargs)
        tpl = self.get_data()
        context.update({"labels": tpl[0], "datasets": tpl[1]})
        return context

    def get_data(self): #, request, *args, **kwargs):

      queryRanges = list()
      #last seven days
      for dateinrange in getDateRange(-7):
        queryRanges.append(dateinrange.strftime("%Y%m%d"))

      mapping = {}
      if caches['metadata'].get("fieldchart") is None:
        mapping = {}
      else:
        mapping = json.loads(caches['metadata'].get("fieldchart") )

      arr = [None] * len(mapping.keys())
      fields = [None] * len(mapping.keys())
      counts = 0
      colors = ["#"+''.join([random.choice('0123456789ABCDEF') for j in range(6)]) for i in range(len(mapping.keys()))]
      for field in mapping:
        arr[counts] = mapping[field][0]#[None] * len(mapping[field])
        fields[counts]=field
        loccount=0
        for cnt in mapping[field]:
          loccount=loccount+1
        counts=counts+1
      returnval = [1]
      ret = {}
      ret["backgroundColor"]=colors
      ret["label"] = "Distribution of data"
      ret["data"] = arr
      returnval[0] = ret
      return (fields,returnval) #render_to_response('data.html', { 'metadata': mapping })



class FieldMetadataView(StrongholdPublicMixin,TemplateView):
    login_url = '/accounts/login/'
    redirect_field_name = 'login'
    model = Query
    template_name = 'fieldmetadata.html'

    @method_decorator(login_required)
    def dispatch(self, *args, **kwargs):
        return super(TemplateView, self).dispatch(*args, **kwargs)

    @method_decorator(login_required)
    def get(self, request, *args, **kwargs):
      metadata = "{}"
      if caches['metadata'].get("field") is None:
        metadata = "{}"
      else:
        metadata = caches['metadata'].get("field")

      context={ "admin": request.user.is_superuser, "authenticated":True, 'metadata': json.loads(metadata) }
      return render(request,'fieldmetadata.html',context)


class DeleteEventView(StrongholdPublicMixin,TemplateView):
    login_url = '/accounts/login/'
    redirect_field_name = 'login'
    model = Query
    template_name = 'mutate_page.html'

    @method_decorator(login_required)
    def dispatch(self, *args, **kwargs):
        return super(TemplateView, self).dispatch(*args, **kwargs)

    @method_decorator(login_required)
    def get(self, request, *args, **kwargs):
      shard = request.GET.get('shard')
      datatype = request.GET.get('datatype')
      uid = request.GET.get('uid')
      query = request.GET.get('query')
      authstring = request.GET.get('auths')
      url = "/search/?q=" + query
      auths = sharkbite.Authorizations()
      if not authstring is None and len(authstring) > 0:
        auths.addAuthorization(authstring)
      user = sharkbite.AuthInfo(AccumuloCluster.objects.first().user,AccumuloCluster.objects.first().password, ZkInstance().get().getInstanceId())
      connector = sharkbite.AccumuloConnector(user, ZkInstance().get())
      tableOps = connector.tableOps(AccumuloCluster.objects.first().dataTable)
      scanner = tableOps.createScanner(auths,1)
      startKey = sharkbite.Key(row=shard)
      endKey = sharkbite.Key(row=shard)
      docid = datatype + "\x00" + uid;
      startKey.setColumnFamily(docid)
      endKey.setColumnFamily(docid + "\xff")
      rng = sharkbite.Range(startKey,True,endKey,True)
      scanner.addRange(rng)
      writer = tableOps.createWriter(auths,10)
      deletes = sharkbite.Mutation(shard)
      for keyValue in scanner.getResultSet():
         key = keyValue.getKey()
         deletes.putDelete( key.getColumnFamily(), key.getColumnQualifier(), key.getColumnVisibility(), key.getTimestamp())
     ## scan for the original document
      writer.addMutation(deletes)
      writer.close()
      for auth in authstring.split("|"):
        url = url + "&auths=" + auth
      return HttpResponseRedirect(url)

class MutateEventView(StrongholdPublicMixin,TemplateView):
    login_url = '/accounts/login/'
    redirect_field_name = 'login'
    model = Query
    template_name = 'mutate_page.html'

    @method_decorator(login_required)
    def dispatch(self, *args, **kwargs):
        return super(TemplateView, self).dispatch(*args, **kwargs)

    @method_decorator(login_required)
    def delete(self, request, *args, **kwargs):
      url = "/search/?q=" + query
      for auth in authstring.split("|"):
        url = url + "&auths=" + auth
      return HttpResponseRedirect(url)
    @method_decorator(login_required)
    def post(self, request, *args, **kwargs):
      query= request.POST.get('query')
      shard = request.POST.get('shard')
      authstring = request.POST.get('auths')
      datatype= request.POST.get('datatype')
      uid = request.POST.get('uid')
      originals = {}
      news = {}
      for key, value in request.POST.items():
        if key == "query":
          query = value
        elif key.startswith("original"):
           split = key.split(".")
           originals[split[1]] = value
        elif key == "shard" or key == "datatype" or key == "uid" or key == "auths":
          pass
        elif key == "csrfmiddlewaretoken":
          pass
        else:
          news[key] = value
      user = sharkbite.AuthInfo(AccumuloCluster.objects.first().user,AccumuloCluster.objects.first().password, ZkInstance().get().getInstanceId())
      connector = sharkbite.AccumuloConnector(user, ZkInstance().get())

      auths = sharkbite.Authorizations()
      #for auth in
      if not authstring is None and len(authstring) > 0:
        auths.addAuthorization(authstring)
      table = AccumuloCluster.objects.first().dataTable
      index_table= AccumuloCluster.objects.first().indexTable
      table_operations = connector.tableOps(table)
      index_table_ops = connector.tableOps(index_table)
      writer = table_operations.createWriter(auths, 10)
      indexWriter = index_table_ops.createWriter(auths,5)
      mutation = sharkbite.Mutation(shard);
      diff=0
      for key,value in news.items():
        if news[key] != originals[key]:
          import datetime;
          ts = int( datetime.datetime.now().timestamp())*1000
          mutation.putDelete(datatype + "\x00" + uid,key + "\x00" + originals[key],authstring,ts)
          ts = int( datetime.datetime.now().timestamp())*1000+100
          mutation.put(datatype + "\x00" + uid,key + "\x00" + news[key],authstring,ts)
          originalIndexMutation = sharkbite.Mutation(originals[key].lower())
          indexMutation = sharkbite.Mutation(news[key].lower())
          protobuf = Uid_pb2.List()
          protobuf.COUNT=1
          protobuf.IGNORE=False
          protobuf.UID.append( uid )
          indexMutation.put(key,shard + "\x00" + datatype,authstring,ts,protobuf.SerializeToString())
          originalprotobuf = Uid_pb2.List()
          indexWriter.addMutation(indexMutation)
          originalprotobuf.COUNT=1
          originalprotobuf.IGNORE=False
          originalprotobuf.REMOVEDUID.append( uid)
          originalIndexMutation.put(key,shard + "\x00" + datatype,authstring,ts,originalprotobuf.SerializeToString())
          indexWriter.addMutation(originalIndexMutation)
          diff=diff+1
        else:
          pass
      if diff > 0:
        writer.addMutation( mutation )
      indexWriter.close()
      writer.close()
      authy = ""
      url = "/search/?q=" + query
      for auth in authstring.split("|"):
        url = url + "&auths=" + auth
      return HttpResponseRedirect(url)
    @method_decorator(login_required)
    def get(self, request, *args, **kwargs):
      user = sharkbite.AuthInfo(AccumuloCluster.objects.first().user,AccumuloCluster.objects.first().password, ZkInstance().get().getInstanceId())
      connector = sharkbite.AccumuloConnector(user, ZkInstance().get())

      table = AccumuloCluster.objects.first().dataTable
      authstring = request.GET.get('auths')
      if not authstring is None and len(authstring) > 0 and authstring=="PROV":
        table="provenance"
      table_operations = connector.tableOps(table)
      shard = request.GET.get('shard')
      datatype = request.GET.get('dt')
      uid = request.GET.get('id')
      q = request.GET.get('query')
      auths = sharkbite.Authorizations()
      if not authstring is None and len(authstring) > 0:
        auths.addAuthorization(authstring)
      asyncQueue = queue.Queue()
      asyncQueue.put(Range(datatype,shard,uid))
      shardLookupInformation=LookupInformation(table,auths,table_operations)
      docs = queue.Queue()
      getDoc(shardLookupInformation,asyncQueue,docs)
      wanted_items=list()
      while not docs.empty():
        wanted_items.append(docs.get())
      context = {'shard':shard, 'uid' : uid, 'datatype' : datatype,'query': q , 'auths': authstring,'results' : wanted_items}
      return render(request,'mutate_page.html',context)


class FileUploadView(StrongholdPublicMixin,TemplateView):
    login_url = '/accounts/login/'
    redirect_field_name = 'login'
    model = Query
    template_name = 'fileupload.html'

    @method_decorator(login_required)
    def dispatch(self, *args, **kwargs):
        return super(TemplateView, self).dispatch(*args, **kwargs)

    @method_decorator(login_required)
    def post(self, request, *args, **kwargs):
        form = DocumentForm(request.POST, request.FILES)
        if form.is_valid():
            form.save()
            for config in IngestConfiguration.objects.all():
                if len(config.post_location) > 0:
                    ## upload the file to another location  and then
                    ## change the status
                    files = FileUpload.objects.filter(status="NEW")
                    if not files is None:
                        for file in files:
                            upl_files = {'file': open(file.document.path,'rb')}
                            requests.post(config.post_location,files=upl_files)
                            print("Posting to " + config.post_location)
                            if not config.use_provenance:
                              file.status="UPLOADED"
                            file.save()
                else:
                  print("Could not find a suitable post location")
            return HttpResponseRedirect('/files/status')

        context = {"admin": request.user.is_superuser, "authenticated":True, 'form' : form}
        return render(request, self.template_name, context)

    @method_decorator(login_required)
    def get(self, request, *args, **kwargs):
      form = DocumentForm()
      context = {"admin": request.user.is_superuser, "authenticated":True, 'form' : form}
      return render(request, self.template_name, context)

class FileStatusView(StrongholdPublicMixin,TemplateView):
    login_url = '/accounts/login/'
    redirect_field_name = 'login'
    model = Query
    template_name = 'filestatus.html'

    @method_decorator(login_required)
    def dispatch(self, *args, **kwargs):
        return super(TemplateView, self).dispatch(*args, **kwargs)

    @method_decorator(login_required)
    def get(self, request, *args, **kwargs):
      form = DocumentForm()
      objs = FileUpload.objects.all()
      
      for obj in objs:
        if len(obj.originalfile)==0:
          ind = obj.document.name.split("_")
          if len(ind) == 2:
            obj.originalfile = ind[1]
            obj.save()
      context = {"admin": request.user.is_superuser, "authenticated":True, 'uploads': objs}
      return render(request, self.template_name, context)


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
      cert = DatawaveWebservers.objects.first().cert_file
      key = DatawaveWebservers.objects.first().key_file
      cacert = DatawaveWebservers.objects.first().ca_file
      key_pass = DatawaveWebservers.objects.first().key_password
      url = DatawaveWebservers.objects.first().url

      entry = request.GET.get('q')
      selectedauths = request.GET.getlist('auths')
      try:
        skip = int(request.GET.get('s'))
      except:
        skip=0
      field = request.GET.get('f')



      isProv=False
      authlist=list()
      auths = sharkbite.Authorizations()
      for auth in selectedauths:
        if len(auth) > 0:
          if auth == "PROV":
            isProv=True
          authlist.append(auth)
          auths.addAuthorization(auth)
      if isProv is True and len(authlist) == 1:
        table="provenance"
        index_table="provenanceIndex"
     

      start=time.time()
      wanted_items = list()
      docs = queue.Queue()

      user_query = query.Query(query = entry,
            cert_path = cert, key_path = key, ca_cert=cacert, key_password=key_pass, url=url ).with_syntax("LUCENE")
      ands = []
      result = user_query.create()
      if result.events is not None:
          for event in result.events:
            #docs.put(event)
            doc = dict()
            for field in event['Fields']:
              value = field['Value']
              if value['type'] == 'xs:decimal':
                doc[ field['name']] = float(value['value'])
              else:
                doc[ field['name']] = value['value']
            docs.put(doc)

      counts = 0
      header = set()
      while not docs.empty():
        jsondoc = docs.get()
        for key in jsondoc.keys():
          if key != "ORIG_FILE" and key != "TERM_COUNT" and key != "RAW_FILE" and key != "shard" and key != "datatype" and key != "uid":
            header.add( key )
        wanted_items.append(jsondoc)
        counts=counts+1
      nxt=""
      prv=""

      userAuths = set()
      try:
        auths =  UserAuths.objects.get(name=request.user)
        user_auths = auths.authorizations.all()
        if not user_auths is None:
          for authset in user_auths:
            userAuths.add(authset)
      except:
        pass
      s="|"
      authy= s.join(authlist)
      context={'header': header,'authstring':authy, 'selectedauths':selectedauths,'results': wanted_items, 'time': (time.time() - start), 'prv': prv, 'nxt': nxt,'field': field, "admin": request.user.is_superuser, "authenticated":True,'userAuths':userAuths,'query': entry}
      return render(request,'search_results.html',context)
