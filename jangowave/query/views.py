from django.utils.decorators import method_decorator
import uuid
from django.core import serializers
from django.contrib.auth.decorators import user_passes_test
from django.template import RequestContext
from django.core.cache import caches
import random
from django.contrib.auth.decorators import login_required
from django.shortcuts import render
from django import http
from django.http import HttpResponseRedirect
from django.shortcuts import render_to_response
from ctypes import cdll
from argparse import ArgumentParser
import heapq
from sortedcontainers import SortedList, SortedSet, SortedDict
import ctypes
import itertools
import os
import traceback
import sys
import Uid_pb2
import time
import datetime
import json
from django.http import JsonResponse
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
from django.views.generic import TemplateView, ListView, View

from stronghold.views import StrongholdPublicMixin
import threading

from .models import FileUpload
from .models import AccumuloCluster
from .forms import DocumentForm
from .models import Query
from .models import UserAuths
from .models import Auth
from  .WritableUtils import *
from .rangebuilder import *
from luqum.parser import lexer, parser, ParseError
from luqum.pretty import prettify
from luqum.utils import UnknownOperationResolver, LuceneTreeVisitorV2
from luqum.exceptions import OrAndAndOnSameLevel
from luqum.tree import OrOperation, AndOperation, UnknownOperation
from luqum.tree import Word  # noqa: F401
import asyncio
from collections import deque
import queue
import concurrent.futures

resolver = UnknownOperationResolver()

import faulthandler
faulthandler.enable()

#### **** import jnius_config
#### **** jnius_config.set_classpath('.', '/home/centos/datawave-dev-3.1.0-SNAPSHOT/lib/*')
#### **** import jnius



import pysharkbite


def getOrSetZookeeper():
  
  return zk

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
        self.zk = pysharkbite.ZookeeperInstance(AccumuloCluster.objects.first().instance, AccumuloCluster.objects.first().zookeeper, 1000, conf)

    def get(self):
        return self.zk

def runningWorkers(workers):
  for worker in workers:
    if not worker.done():
      return True
  return False

class CancellationToken:
   def __init__(self):
       self._is_cancelled = threading.Event()
       self._is_cancelled.clear()

   def cancel(self):
       self._is_cancelled.set()

   def running(self):
       return self._is_cancelled.is_set() == False

   def cancelled(self):
       return self._is_cancelled.is_set()

def lookupRange(lookupInformation : LookupInformation, range : RangeLookup, output ) -> None:
    indexTableOps = lookupInformation.getTableOps()

    indexScanner = indexTableOps.createScanner(lookupInformation.getAuths(),1)

    indexrange = pysharkbite.Range(range.getValue())

    indexScanner.addRange(indexrange)
    if not  range.getField() is None:
      indexScanner.fetchColumn(range.getField().upper(),"")
    indexSet = indexScanner.getResultSet()

    for indexKeyValue in indexSet:
       value = indexKeyValue.getValue()
       protobuf = Uid_pb2.List()
       protobuf.ParseFromString(value.get().encode())
       for uidvalue in protobuf.UID:
            shard = indexKeyValue.getKey().getColumnQualifier().split("\u0000")[0]
            datatype = indexKeyValue.getKey().getColumnQualifier().split("\u0000")[1]            
            output.put( Range(datatype,shard,uidvalue))
    indexScanner.close()



def executeIterator(indexLookupInformation : LookupInformation,iterator : LookupIterator, output ) -> None:
    iterator.getRanges(indexLookupInformation,output)
    print("executor finished")


class OrIterator(LookupIterator):
    def __init__(self,rng : RangeLookup):
        self._rangeQueue=queue.SimpleQueue()
        self._rangeQueue.put(rng)

    def __init__(self,rng,lookupInfo):
      super(LookupIterator,self).__init__(rng,lookupInfo)

    def __init__(self):
        self._rangeQueue=queue.SimpleQueue()

    def addRange(self, rng):
        self._rangeQueue.put(rng)

    def combineScanners(self,scanners):
      if not seqs: return   # No items
      return itertools.chain(scanner)


    def getRanges(self,indexLookupInformation : LookupInformation, queue : queue.SimpleQueue):
        loop = asyncio.new_event_loop()

        with concurrent.futures.ThreadPoolExecutor() as pool:
            while not self._rangeQueue.empty():
                rng = self._rangeQueue.get()
                if isinstance(rng,RangeLookup):
                  result = loop.run_in_executor(pool, lookupRange,indexLookupInformation, rng, queue)
                elif isinstance(rng,LookupIterator):
                  rng.getRanges(indexLookupInformation,queue)  
        #loop.close()
       


def intersect_sets(seqs):
   if not seqs: return   # No items
   iterators =  [ForwardIterator(seq) for seq in seqs]
   first, rest = iterators[0], iterators[1:]
   for item in first: 
       candidates = list(rest)
       while candidates:
           if any(c.peek() is EndOfIter for c in candidates):   
            return  # Exhausted an iterator
           candidates = [c for c in candidates if c.peek() < item]
           for c in candidates:
            c.__next__()
       # Out of loop if first item in remaining iterator are all >= item.
       if all(it.peek() == item for it in rest):
           yield item
def lookupRanges(lookupInformation : LookupInformation, ranges : list, output ) -> None:
    indexTableOps = lookupInformation.getTableOps()

    rngs = [None] * len(ranges)
    scnrs = [None] * len(ranges)
    itrs = [None] * len(ranges)
    count=0
    for rng in ranges:
      if isinstance(rng,LookupIterator):
        itrs[count] = ForwardIterator(rng)
        scnrs[count]=None
        count=count+1
      else:  
        scnrs[count] = indexTableOps.createScanner(lookupInformation.getAuths(),1)
        indexrange = pysharkbite.Range(rng.getValue())
        scnrs[count].addRange(indexrange)
        if not  rng.getField() is None:
          scnrs[count].fetchColumn(rng.getField().upper(),"")
        itrs[count]=scnrs[count].getResultSet()
        count=count+1
    print("need to intersect " + str(count)) 
    try:
      for indexKeyValue in intersect_sets(itrs):
         print(" *** got somthin " + indexKeyValue.getShard())
         output.put( indexKeyValue)
    except StopIteration:
      print("ohstop")
    except:
      print("oh fail")
      traceback.print_exc()
      raise
    print("finitio")
    for scnr in scnrs:
      if not scnr is None:
        scnr.close()

class AndIterator(LookupIterator):
    def __init__(self,rng : RangeLookup):
        self._rangeQueue=list()
        self._rangeQueue.append(rng)

    def __init__(self,rng,lookupInfo):
      super(LookupIterator,self).__init__(rng,lookupInfo)
  
    def __init__(self):
        self._rangeQueue=list()

    def addRange(self, rng):
        self._rangeQueue.append(rng)

    def combineScanners(self,scanners):
      if not seqs: return   # No items
      iterators = [seq for seq in seqs]
      first, rest = iterators[0], iterators[1:]
      for item in first:
       candidates = list(rest)
       while candidates:
           if any(c.peek() is EndOfIter for c in candidates): return  # Exhausted an iterator
           candidates = [c for c in candidates if c.peek() < item]
           for c in candidates: c.next()
       # Out of loop if first item in remaining iterator are all >= item.
       if all(it.peek() == item for it in rest):
           yield item      

    def getRanges(self,indexLookupInformation : LookupInformation, queue : queue.SimpleQueue):
        if len(self._rangeQueue)==0:
          return
        queueitem = 0
        loop = asyncio.new_event_loop()
        with concurrent.futures.ThreadPoolExecutor() as pool:
          result = loop.run_in_executor(pool, lookupRanges,indexLookupInformation, self._rangeQueue, queue)
         # while not result.done():
          #  time.sleep(.5)
class IndexLookup(LuceneTreeVisitorV2):
    def __init__(self):
        pass
    def visit_and_operation(self, *args, **kwargs):
        return self._binary_operation("AND", *args, **kwargs)
    
    def visit_or_operation(self, *args, **kwargs):
        return self._binary_operation("OR", *args, **kwargs)


    def _binary_operation(self, op_type_name, node, parents, context):
        child_context = dict(context) if context is not None else {}
        operation="OR"
        iter = OrIterator()
        if op_type_name == "AND":
            print("encoutered and")
            iter = AndIterator()
        else:
            print ("encountered or")
            iter = OrIterator()

        children = self.simplify_if_same(node.children, node)
        children = self._yield_nested_children(node, children)
        if child_context.get("need_in", False):
            child_context["in"] = True
        #children = node.children
        items = [self.visit(child, parents + [node], child_context) for child in
                 children]
        #We are selecting columns
  
        for lookup in items:
            ## add iterators
            if isinstance(lookup, LookupIterator):
               print("adding lookp as range")
               iter.addRange(lookup)
            elif lookup.getValue() == "or":
                pass    
        #    iter = OrIterator()
            elif lookup.getValue() == "and":
                pass # :witer = AndIterator()
            else:
                print("value is " + lookup.getValue())
                iter.addRange(lookup)

        return iter

    def _is_must(self, operation):
        """
        Returns True if the node is a AndOperation or an UnknownOperation when
        the default operator is MUST
        :param node: to check
        :return: Boolean
        ::
            >>> ElasticsearchQueryBuilder(
            ...     default_operator=ElasticsearchQueryBuilder.MUST
            ... )._is_must(AndOperation(Word('Monty'), Word('Python')))
            True
        """
        return (
            isinstance(operation, AndOperation) or
            isinstance(operation, UnknownOperation) and
            self.default_operator == ElasticsearchQueryBuilder.MUST
        )

    def _is_should(self, operation):
        """
        Returns True if the node is a OrOperation or an UnknownOperation when
        the default operator is SHOULD
        ::
            >>> ElasticsearchQueryBuilder(
            ...     default_operator=ElasticsearchQueryBuilder.MUST
            ... )._is_should(OrOperation(Word('Monty'), Word('Python')))
            True
        """
        return (
            isinstance(operation, OrOperation) or
            isinstance(operation, UnknownOperation) and
            self.default_operator == ElasticsearchQueryBuilder.SHOULD
        )

    def _yield_nested_children(self, parent, children):
        """
        Raise if a OR (should) is in a AND (must) without being in parenthesis::
            >>> builder = ElasticsearchQueryBuilder()
            >>> op = OrOperation(Word('yo'), OrOperation(Word('lo'), Word('py'))
            >>> list(builder._yield_nested_children(op, op.children))
            [Word('yo'), OrOperation(Word('lo'), Word('py'))]
            >>> op = OrOperation(Word('yo'), AndOperation(Word('lo'), Word('py')))
            >>> list(builder._yield_nested_children(op, op.children))
            Traceback (most recent call last):
                ...
            luqum.exceptions.OrAndAndOnSameLevel: lo AND py
        """

        for child in children:
            if (self._is_should(parent) and self._is_must(child) or
               self._is_must(parent) and self._is_should(child)):
                raise OrAndAndOnSameLevel(
                    self._get_operator_extract(child)
                )
            else:
                yield child

    def simplify_if_same(self, children, current_node):
        """
        If two same operation are nested, then simplify
        Should be use only with should and must operations because Not(Not(x))
        can't be simplified as Not(x)
        :param children:
        :param current_node:
        :return:
        """
        for child in children:
            
            if type(child) is type(current_node):
                print("same")
                yield from self.simplify_if_same(child.children, current_node)
            else:
                yield child

    def visit_search_field(self, node, parents, context):
        #child_context = dict(context) if context is not None else {}
        #enode = self.visit(node.children[0], parents + [node], child_context)
        
        field = node.name
        value = node.expr.value

        if value == "*":
            raise Exception("Do not support unlimited range queries")

        return RangeLookup(field,value) 


    def visit_word(self, node, parents, context):
        # we've arrived here because of an unfielded query
        # or because of invalid syntax in lucene query
        
        value = node.value

        if value == "*":
            raise Exception("Do not support unlimited range queries")

        

        return RangeLookup(None,value) 


def produceShardRanges(cancellationtoken : CancellationToken,indexLookupInformation : LookupInformation,output : queue.SimpleQueue, iterator: LookupIterator):
        ranges = queue.SimpleQueue()
        executeIterator(indexLookupInformation,iterator,ranges)
        print ("Producing shard ranges")
        while not ranges.empty() and not cancellationtoken.cancelled():
            try:
              rng = ranges.get(False)
              output.put(rng,timeout=2)
              print("Size is " + str(output.qsize()))
            except Queue.Empty:
              continue
            except:
              break
        cancellationtoken.cancel()
        print("Exiting producer")
        

def scanDoc(scanner, outputQueue):
    resultset = scanner.getResultSet()
    count = 0
    for keyvalue in resultset:
        key = keyvalue.getKey()
        value = keyvalue.getValue()
        if len(value.get()) == 0:
          continue
        print("Received one of length" + str(len(value.get())))
        jsonpayload = json.loads(value.get())
        outputQueue.put( jsonpayload )
    
        count=count+1
    print("Exiting scan")

    scanner.close()

    return count

def getDocuments(cancellationtoken : CancellationToken, name : int , lookupInformation : LookupInformation,input : queue.SimpleQueue, outputQueue : queue.SimpleQueue):
  count=0
  while cancellationtoken.running():
    docInfo = None
    try:
        try:
            if input.empty():
              pass
            else:
              docInfo = input.get(timeout=1)
        except:
            print("Continuing")
            # Handle empty queue here
        if not docInfo is None:
          print("Scanning shard from "+ str(name))
          tableOps = lookupInformation.getTableOps()
          scanner = tableOps.createScanner(lookupInformation.getAuths(),5)
          startKey = pysharkbite.Key()
          endKey = pysharkbite.Key()
          startKey.setRow(docInfo.getShard())
          docid = docInfo.getDataType() + "\x00" + docInfo.getDocId();
          startKey.setColumnFamily(docid)
          endKey.setRow(docInfo.getShard())
          endKey.setColumnFamily(docid + "\xff")
          rng = pysharkbite.Range(startKey,True,endKey,True)

          scanner.addRange(rng)

          rangecount=1

          while rangecount < 10:
            try:
              print("getting " + str(name))
              docInfo = input.get(False)
              print("oh goody " + str(rangecount))
              startKey = pysharkbite.Key()
              endKey = pysharkbite.Key()
              startKey.setRow(docInfo.getShard())
              docid = docInfo.getDataType() + "\x00" + docInfo.getDocId();
              startKey.setColumnFamily(docid)
              endKey.setRow(docInfo.getShard())
              endKey.setColumnFamily(docid + "\xff")
              rng = pysharkbite.Range(startKey,True,endKey,True)
  
              scanner.addRange(rng)
              rangecount=rangecount+1
      
            except:
              print("oh")
              rangecount=11

          print("attempting scan")

          with open('jsoncombiner.py', 'r') as file:
            combinertxt = file.read()
            combiner=pysharkbite.PythonIterator("PythonCombiner",combinertxt,100)
            scanner.addIterator(combiner)
          print ("Launching from " + str(name))
          count = count + scanDoc(scanner,outputQueue)
          print("gotdoc")
        else:
          time.sleep(0.5)
      
    except:
      print("**ERror**",flush=True)
      e = sys.exc_info()[0]
      print("**Error occurred" + e,flush=True)  
  print("*Exiting " + str(name),flush=True) 
  print("Count is " + str(count),flush=True)
  if cancellationToken.cancelled():
    print("*Exiting " + str(name) + " due to cancellation", flush=True)
  return True
#    while True:
 #     docInfo = input.get()
  #    input.task_done()

def getDoc(docLookupInformation : LookupInformation,asyncQueue : queue.SimpleQueue, documents : queue.SimpleQueue):

    intermediateQueue = queue.SimpleQueue()

    isrunning = CancellationToken()
    workers = list()


    executor = ThreadPoolExecutor(max_workers=2)
    future = executor.submit(getDocuments,isrunning,0,docLookupInformation,asyncQueue,intermediateQueue)
    workers.append(future)

    counts = 0
    while counts < 10 and (runningWorkers(workers) or not asyncQueue.empty()):
          if asyncQueue.empty():
             isrunning.cancel()
          try:
           if not intermediateQueue.empty():
            documents.put(intermediateQueue.get())
            counts=counts+1
           else:
            time.sleep(1)
          except Queue.Empty:
            pass

    while counts < 10 and not intermediateQueue.empty():
      try:
         if not intermediateQueue.empty():
          doc = intermediateQueue.get()

          documents.put(doc)
          counts=counts+1
         else:
          time.sleep(1)
      except :
          pass

    print("found about " + str(documents.qsize()))

    isrunning.cancel()

    for worker in workers:
      worker.cancel()

    executor.shutdown()

def lookup(indexLookupInformation : LookupInformation, docLookupInformation : LookupInformation,iterator: LookupIterator, documents : queue.SimpleQueue):

    asyncQueue = queue.SimpleQueue()

    intermediateQueue = queue.SimpleQueue()

    isrunning = CancellationToken()
    producerrunning = CancellationToken()
    workers = list()


    executor = ThreadPoolExecutor(max_workers=5)
    producer = executor.submit(produceShardRanges,producerrunning,indexLookupInformation,asyncQueue,iterator)
    for i in range(2):
      future = executor.submit(getDocuments,isrunning,i,docLookupInformation,asyncQueue,intermediateQueue)
      workers.append(future)

    counts = 0
    while not producerrunning.cancelled():
      if intermediateQueue.qsize() > 10:
        producerrunning.cancel()
        break;
      time.sleep(.5)

    while counts < 10 and (runningWorkers(workers) or not asyncQueue.empty()):
          if asyncQueue.empty() and producerrunning.cancelled():
             isrunning.cancel()
          try:
           if not intermediateQueue.empty():
            documents.put(intermediateQueue.get())
            counts=counts+1
           else:
            time.sleep(1)
          except Queue.Empty:
            pass

    while counts < 10 and not intermediateQueue.empty():
      try:
         if not intermediateQueue.empty():
          doc = intermediateQueue.get()
          
          documents.put(doc)
          counts=counts+1
         else:
          time.sleep(1)
      except :
          pass

    print("found about " + str(documents.qsize()))

    isrunning.cancel()
    producerrunning.cancel()

    for worker in workers:
      worker.cancel()

    executor.shutdown()


conf = pysharkbite.Configuration()

conf.set ("FILE_SYSTEM_ROOT", "/accumulo");

#zk = pysharkbite.ZookeeperInstance(AccumuloCluster.objects.first().instance, AccumuloCluster.objects.first().zookeeper, 1000, conf)

#pysharkbite.LoggingConfiguration.enableTraceLogger()

class HomePageView(StrongholdPublicMixin,TemplateView):
    login_url = '/accounts/login/'
    redirect_field_name = 'login'
    template_name = 'home.html'

    @method_decorator(login_required)
    def dispatch(self, *args, **kwargs):
      return super(TemplateView, self).dispatch(*args, **kwargs)

    @method_decorator(login_required)
    def get(self, request, *args, **kwargs):    
      #auths =  UserAuths.objects.get(name=request.user)
      userAuths = set()
      try:
        auths =  UserAuths.objects.get(name=request.user)

        for authset in auths.authorizations.all():
            userAuths.add(authset)
      except:
        pass
      context = { 'admin': request.user.is_superuser, 'authenticated':True, 'userAuths': userAuths }
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
      context = {'admin': request.user.is_superuser, 'authenticated':True}
      return render(request,self.template_name,context)

class ComplexEncoder(json.JSONEncoder):
    """Always return JSON primitive."""

    def default(self, obj):
        try:
            return super(ComplexEncoder, obj).default(obj)
        except TypeError:
            if hasattr(obj, "pk"):
                return obj.pk
            return str(obj)

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


def daterange(start_date, end_date):
    for n in range(0,int ((end_date - start_date).days)+1):
        yield start_date + datetime.timedelta(n)

def getDateRange(days : int ):
  adjusted_date = datetime.datetime.now() + datetime.timedelta(days)
  now_date = datetime.datetime.now()
#strftime("%Y%m%d")
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
        print("oh boy")
        context = super(MetadataEventCountsView, self).get_context_data(**kwargs)
        tpl = self.get_data()
        context.update({"labels": tpl[0], "datasets": tpl[1]})
        return context
    def get_data(self): #, request, *args, **kwargs):
      colors = ["#"+''.join([random.choice('0123456789ABCDEF') for j in range(6)]) for i in range(16)]
      counts=0
      fields = [None] * 16
      print("here")
      arr = [None] * 16
      for dateinrange in getDateRange(-15):
        dt = dateinrange.strftime("%Y%m%d")
        fields[counts]=dt
        cachedVal = caches['eventcount'].get(dt)
        if cachedVal is None:
          numeric = 0
        else:
          numeric = int(cachedVal)
        print("Cached value for " + dt + " is " + str( numeric))
        arr[counts] = numeric
        counts=counts+1
      print("image")
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
        print("oh boy")
        context = super(MetadataChartView, self).get_context_data(**kwargs)
        tpl = self.get_data()
        context.update({"labels": tpl[0], "datasets": tpl[1]})
        return context

    def get_data(self): #, request, *args, **kwargs):

      queryRanges = list()
      #last seven days
      for dateinrange in getDateRange(-7):
        print( dateinrange.strftime("%Y%m%d") )     
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
        #  arr[counts][loccount] = cnt
          loccount=loccount+1
        counts=counts+1
      print("image")
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
      metadata = {}
      if caches['metadata'].get("field") is None:
        metadata = {}
      else:
        metadata = caches['metadata'].get("field")

      context={ 'admin': request.user.is_superuser, 'authenticated':True, 'metadata': json.loads(metadata) }
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
      auths = pysharkbite.Authorizations()
      auths.addAuthorization(authstring)
      user = pysharkbite.AuthInfo(AccumuloCluster.objects.first().user,AccumuloCluster.objects.first().password, ZkInstance().get().getInstanceId())
      connector = pysharkbite.AccumuloConnector(user, ZkInstance().get()) 
      tableOps = connector.tableOps("shard")
      scanner = tableOps.createScanner(auths,1)
      startKey = pysharkbite.Key(row=shard)
      endKey = pysharkbite.Key(row=shard)
      docid = datatype + "\x00" + uid;
      startKey.setColumnFamily(docid)
      endKey.setColumnFamily(docid + "\xff")
      rng = pysharkbite.Range(startKey,True,endKey,True)
      scanner.addRange(rng)
      writer = tableOps.createWriter(auths,10)
      deletes = pysharkbite.Mutation(shard)
      for keyValue in scanner.getResultSet():
         key = keyValue.getKey()
         deletes.putDelete( key.getColumnFamily(), key.getColumnQualifier(), key.getColumnVisibility(), key.getTimestamp())
     ## scan for the original document
      writer.addMutation(deletes)
      writer.close()
      ## add the deletes. Can leave the index hanging
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
      user = pysharkbite.AuthInfo(AccumuloCluster.objects.first().user,AccumuloCluster.objects.first().password, ZkInstance().get().getInstanceId())
      connector = pysharkbite.AccumuloConnector(user, ZkInstance().get())

      auths = pysharkbite.Authorizations()
      #for auth in 
      if not authstring is None and len(authstring) > 0:
        print("got " + authstring)
        auths.addAuthorization(authstring)

      table = "shard"
      indexTable= "shardIndex"
      tableOperations = connector.tableOps(table)
      indexTableOps = connector.tableOps(indexTable)
      writer = tableOperations.createWriter(auths, 10)
      indexWriter = indexTableOps.createWriter(auths,5)
      mutation = pysharkbite.Mutation(shard);    
      diff=0
      for key,value in news.items():

        if news[key] != originals[key]:
          import datetime;
          ts = int( datetime.datetime.now().timestamp())*1000
          mutation.putDelete(datatype + "\x00" + uid,key + "\x00" + originals[key],authstring,ts)
          ts = int( datetime.datetime.now().timestamp())*1000+100
          print ("time stamp is " + str(ts))
          mutation.put(datatype + "\x00" + uid,key + "\x00" + news[key],authstring,ts)
          originalIndexMutation = pysharkbite.Mutation(originals[key])
          indexMutation = pysharkbite.Mutation(news[key])
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
          print(news[key] + " is the same as " + originals[key])
      if diff > 0:
        print("Adding mudation")
        writer.addMutation( mutation )
#        deletewriter.addMutation( deletemutation )
      indexWriter.close()
      writer.close()
 #     deletewriter.close()
      authy = ""
      url = "/search/?q=" + query
      for auth in authstring.split("|"):
        url = url + "&auths=" + auth
      return HttpResponseRedirect(url)
    @method_decorator(login_required)
    def get(self, request, *args, **kwargs):
      user = pysharkbite.AuthInfo(AccumuloCluster.objects.first().user,AccumuloCluster.objects.first().password, ZkInstance().get().getInstanceId())
      connector = pysharkbite.AccumuloConnector(user, ZkInstance().get())

      table = "shard"

      tableOperations = connector.tableOps(table)
      shard = request.GET.get('shard')
      datatype = request.GET.get('dt')
      uid = request.GET.get('id')
      q = request.GET.get('query')
      authstring = request.GET.get('auths')
      auths = pysharkbite.Authorizations()
      if not authstring is None and len(authstring) > 0:
        auths.addAuthorization(authstring)
      asyncQueue = queue.SimpleQueue()
      asyncQueue.put(Range(datatype,shard,uid))
 #     for auth in selectedauths:
  #      auths.addAuthorization(auth)
   #   start=time.time()
      shardLookupInformation=LookupInformation(table,auths,tableOperations)
      docs = queue.SimpleQueue() 
      getDoc(shardLookupInformation,asyncQueue,docs)  
      wanted_items=list()
      while not docs.empty():
        wanted_items.append(docs.get())
      # def getDoc(docLookupInformation : LookupInformation,asyncQueue : queue.SimpleQueue, documents : queue.SimpleQueue):
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
            return HttpResponseRedirect('/files/status')
        context = {'admin': request.user.is_superuser, 'authenticated':True, 'form' : form}
        return render(request, self.template_name, context)

    @method_decorator(login_required)
    def get(self, request, *args, **kwargs):
      form = DocumentForm()
      context = {'admin': request.user.is_superuser, 'authenticated':True, 'form' : form}
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
      #objs = FileUpload.objects.all().order_by("status") 
      for obj in objs:
        if len(obj.originalfile)==0:
          print("have to parse originalfile")
          ind = obj.document.name.split("_")
          print("ind size is " + str(len(ind)))
          if len(ind) == 2:
            obj.originalfile = ind[1]
            obj.save()   
      context = {'admin': request.user.is_superuser, 'authenticated':True, 'uploads': objs}
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
      user = pysharkbite.AuthInfo(AccumuloCluster.objects.first().user,AccumuloCluster.objects.first().password, ZkInstance().get().getInstanceId())  
      connector = pysharkbite.AccumuloConnector(user, ZkInstance().get())

      entry = request.GET.get('q')
      selectedauths = request.GET.getlist('auths')
      try:
        skip = int(request.GET.get('s'))
      except:
        skip=0
      field = request.GET.get('f')

      print("query is " + entry)
        

     # try:
        #LuceneToJexlQueryParser  = jnius.autoclass('datawave.query.language.parser.jexl.LuceneToJexlQueryParser')
  
        #luceneparser = LuceneToJexlQueryParser()

       # node = luceneparser.parse(entry)

        #jexl = node.getOriginalQuery()
        #print("Jexl is " + jexl)
     # except:
      #  pass
      indexLookup = 1

      table = "shard"

      tableOperations = connector.tableOps(table)

      indexTableOps = connector.tableOps("shardIndex")

      auths = pysharkbite.Authorizations()
      authlist = list()
      for auth in selectedauths:
        authlist.append(auth)
        auths.addAuthorization(auth)
      start=time.time()
      indexLookupInformation=LookupInformation("shardIndex",auths,indexTableOps)
      shardLookupInformation=LookupInformation(table,auths,tableOperations)
      wanted_items = list()
      tree = parser.parse(entry)
      tree = resolver(tree)
      visitor = IndexLookup()
      iterator = visitor.visit(tree)
      if isinstance(iterator, RangeLookup):
        rng = iterator
        iterator = OrIterator()
        iterator.addRange(rng)
      docs = queue.SimpleQueue()
      lookup(indexLookupInformation,shardLookupInformation,iterator,docs)

      counts = 0
      while not docs.empty():
        wanted_items.append(docs.get())
        counts=counts+1

      nxt=""
      prv=""
      auths =  UserAuths.objects.get(name=request.user)
      userAuths = set()
      for authset in auths.authorizations.all():
          userAuths.add(authset)
      s="|"
      authy= s.join(authlist)
      context={'authstring':authy, 'selectedauths':selectedauths,'results': wanted_items, 'time': (time.time() - start), 'prv': prv, 'nxt': nxt,'field': field, 'admin': request.user.is_superuser, 'authenticated':True,'userAuths':userAuths,'query': entry}
      return render(request,'search_results.html',context) 
      #return render_to_response('search_results.html', {'selectedauths':selectedauths,'results': wanted_items, 'time': (time.time() - start), 'prv': prv, 'nxt': nxt,'field': field, 'admin': request.user.is_superuser, 'authenticated':True,'userAuths':userAuths,'query': entry})
# Create your views here.