from django.utils.decorators import method_decorator
from django.contrib.auth.decorators import login_required
from django.shortcuts import render
from django.shortcuts import render_to_response
from ctypes import cdll
from argparse import ArgumentParser
import heapq
from sortedcontainers import SortedList, SortedSet, SortedDict
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
from concurrent.futures import ThreadPoolExecutor
#from multiprocessing import Queue
import multiprocessing
# cities/views.py
from django.views.generic import TemplateView, ListView, View

from stronghold.views import StrongholdPublicMixin
import threading

from .models import Query
from .models import UserAuths
from .models import Auth
from  .WritableUtils import *
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



import pysharkbite



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


class LookupInformation(object):
    def __init__(self,lookupTable,auths, tableOps):
        self._lookupTable=lookupTable
        self._auths = auths
        self._tableOps = tableOps

    def getLookupTable(self):
        return self._lookupTable

    def getAuths(self):
        return self._auths

    def getTableOps(self):
        return self._tableOps

class Range:
    def __init__(self,datatype,shard,docid):
        self._datatype=datatype
        self._shard=shard
        self._docid=docid

    def getDataType(self):
        return self._datatype

    def getShard(self):
        return self._shard

    def getDocId(self):
        return self._docid

    def __eq__(self, other):
        return self._shard == other._shard and self._docid == other._docid

    def __gt__(self, other):
       return self._shard > other._shard and self._docid > other._docid 

    def __lt__(self, other):
       return self._shard < other._shard and self._docid < other._docid  

class RangeLookup:
    def __init__(self,field,value):
        self._field=field
        self._value=value

    def getValue(self):
        return self._value

    def getField(self):
        return self._field

class LookupIterator(object):
    def __init__(self,rangeQueue):
        self._rangeQueue=rangeQueue
    def getRanges(self,indexLookupInformation : LookupInformation, queue):
      pass

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

    def __init__(self):
        self._rangeQueue=queue.SimpleQueue()

    def addRange(self, rng):
        self._rangeQueue.put(rng)

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
       
EndOfIter = object() # Sentinel value

class ForwardIterator(object):
    def __init__(self, it):
        self._indexset = it
        self.it = it.__iter__()
        self._peek = None
        self.__next__() # pump iterator to get first value

    def __iter__(self): 
        return self

    def __next__(self):
        cur = self._peek
        if cur is EndOfIter:
            raise StopIteration()

        try:
            indexKeyValue = self.it.__next__()
            value = indexKeyValue.getValue()
            protobuf = Uid_pb2.List()
            protobuf.ParseFromString(value.get().encode())
            for uidvalue in protobuf.UID:
              shard = indexKeyValue.getKey().getColumnQualifier().split("\u0000")[0]
              datatype = indexKeyValue.getKey().getColumnQualifier().split("\u0000")[1]
              self._peek=Range(datatype,shard,uidvalue)
        except StopIteration:
            self._peek = EndOfIter
        except:    
            self._peek = EndOfIter
        return cur

    def peek(self): 
        return self._peek


def intersct_sets(seqs):
   if not seqs: return   # No items
   iterators = [ForwardIterator(seq.getResultSet()) for seq in seqs]
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
def lookupRanges(lookupInformation : LookupInformation, ranges : list, output ) -> None:
    indexTableOps = lookupInformation.getTableOps()

    rngs = [None] * len(ranges)
    scnrs = [None] * len(ranges)
    count=0
    for rng in ranges:
      if isinstance(rng,LookupIterator):
        pass
      else:  
        scnrs[count] = indexTableOps.createScanner(lookupInformation.getAuths(),1)
        indexrange = pysharkbite.Range(rng.getValue())
        scnrs[count].addRange(indexrange)
        if not  rng.getField() is None:
          scnrs[count].fetchColumn(rng.getField().upper(),"")
        count=count+1
  
    for indexKeyValue in intersct_sets(scnrs):
       output.put( indexKeyValue)
    for scnr in scnrs:
      if not isinstance(scnr,LookupIterator):
        scnr.close()

class AndIterator(LookupIterator):
    def __init__(self,rng : RangeLookup):
        self._rangeQueue=list()
        self._rangeQueue.append(rng)

    def __init__(self):
        self._rangeQueue=list()

    def addRange(self, rng):
        self._rangeQueue.append(rng)


    def getRanges(self,indexLookupInformation : LookupInformation, queue : queue.SimpleQueue):
        if len(self._rangeQueue)==0:
          return
        queueitem = 0
        loop = asyncio.new_event_loop()
        with concurrent.futures.ThreadPoolExecutor() as pool:
          result = loop.run_in_executor(pool, lookupRanges,indexLookupInformation, self._rangeQueue, queue)

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
               iter.addRange(item)
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
        outputQueue.put( value.get() )
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
             print("Requesting cancellation of workers",flush=True)
             isrunning.cancel()
          try:
           if not intermediateQueue.empty():
            print("adding doc")
            documents.put(intermediateQueue.get())
            counts=counts+1
           else:
            print("Wating" + str(asyncQueue.qsize()))
            time.sleep(1)
          except Queue.Empty:
            pass
    print("Exited main loop " + str(counts) + " " + str(runningWorkers(workers)))

    while counts < 10 and not intermediateQueue.empty():
      print ("adding more docs")
      try:
         if not intermediateQueue.empty():
          doc = intermediateQueue.get()
          print("adding doc" + str(len(doc)))
          
          documents.put(doc)
          counts=counts+1
         else:
          print("Wating")
          time.sleep(1)
      except :
          pass

    print("found about " + str(documents.qsize()))

    isrunning.cancel()
    producerrunning.cancel()

    print("Canceling tasks")

    for worker in workers:
      worker.cancel()

    print("awaiting shutdown")
    executor.shutdown()


conf = pysharkbite.Configuration()

conf.set ("FILE_SYSTEM_ROOT", "/accumulo");

zk = pysharkbite.ZookeeperInstance("muchos", "mycluster-LeaderZK-1:2181,mycluster-LeaderZK-3:2181", 1000, conf)

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
      auths =  UserAuths.objects.get(name=request.user)
      userAuths = set()
      for authset in auths.authorizations.all():
          userAuths.add(authset)
      return render_to_response(self.template_name,{ 'authenticated':True, 'userAuths': userAuths })



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
    template_name = 'metadata.html'

    @method_decorator(login_required)
    def dispatch(self, *args, **kwargs):
        return super(TemplateView, self).dispatch(*args, **kwargs)

    @method_decorator(login_required)
    def get(self, request, *args, **kwargs):
      user = pysharkbite.AuthInfo("root","secret", zk.getInstanceId())
      connector = pysharkbite.AccumuloConnector(user, zk)

      indexTableOps = connector.tableOps("DatawaveMetadata")

      auths = pysharkbite.Authorizations()


      indexScanner = indexTableOps.createScanner(auths,20)
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
         if day in mapping:
           if key.getRow() in mapping[day]:
            print(dt + " " + day + " " + key.getRow() + " " + value.get() + " " + str(mapping[day][key.getRow()])) 
            mapping[day][key.getRow()] += int( value.get() )
           else:
            print(dt + " " + day + " " + key.getRow() + " " + value.get() + " ")
            mapping[day][key.getRow()] = int( value.get() )
         else:
           mapping[day]={}
           mapping[day][key.getRow()] = int( value.get() ) 
  
      return render_to_response('metadata.html', { 'metadata': mapping })

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
      user = pysharkbite.AuthInfo("root","secret", zk.getInstanceId())  
      connector = pysharkbite.AccumuloConnector(user, zk)

      entry = request.GET.get('q')
      selectedauths = request.GET.getlist('auths')
      try:
        skip = int(request.GET.get('s'))
      except:
        skip=0
      print("skipping " + str(skip))
      field = request.GET.get('f')

      print("query is " + entry)
        

      indexLookup = 1

      table = "shard"

      tableOperations = connector.tableOps(table)

      indexTableOps = connector.tableOps("shardIndex")

      auths = pysharkbite.Authorizations()
      for auth in selectedauths:
        print("Adding auth " + auth)
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
        print("wearerangelookup")
        rng = iterator
        iterator = OrIterator()
        iterator.addRange(rng)
      docs = queue.SimpleQueue()
      lookup(indexLookupInformation,shardLookupInformation,iterator,docs)

      counts = 0
      while not docs.empty():
        wanted_items.append(docs.get())
        counts=counts+1
      print ("technically finished with " + str(counts))
      #return Query.objects.filter(pk__in = wanted_items)
#      return JsonResponse(events)
      nxt=""
      prv=""
      auths =  UserAuths.objects.get(name=request.user)
      userAuths = set()
      for authset in auths.authorizations.all():
          userAuths.add(authset)
      return render_to_response('search_results.html', {'selectedauths':selectedauths,'results': wanted_items, 'time': (time.time() - start), 'prv': prv, 'nxt': nxt,'field': field, 'authenticated':True,'userAuths':userAuths,'query': entry})
# Create your views here.
