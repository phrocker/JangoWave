from django.utils.decorators import method_decorator
from django.contrib.auth.decorators import login_required
from django.shortcuts import render
from django.shortcuts import render_to_response
from ctypes import cdll
from argparse import ArgumentParser
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





import pysharkbite


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
    async def getRanges(self,indexLookupInformation : LookupInformation, queue):
        print("lbase")

def lookupRange(lookupInformation : LookupInformation, range : RangeLookup, output : queue.Queue ) -> None:
    indexTableOps = lookupInformation.getTableOps()

    indexScanner = indexTableOps.createScanner(lookupInformation.getAuths(),1)

    indexrange = pysharkbite.Range(range.getValue())

    indexScanner.addRange(indexrange)
    indexScanner.fetchColumn(range.getField().upper(),"")
    indexSet = indexScanner.getResultSet()

    print ("looking up " + range.getField() + " " + range.getValue())
    for indexKeyValue in indexSet:
       value = indexKeyValue.getValue()
       protobuf = Uid_pb2.List()
       protobuf.ParseFromString(value.get().encode())
       for uidvalue in protobuf.UID:
            shard = indexKeyValue.getKey().getColumnQualifier().split("\u0000")[0]
            datatype = indexKeyValue.getKey().getColumnQualifier().split("\u0000")[1]            
            output.put( Range(datatype,shard,uidvalue))
    indexScanner.close()
    print("producer finished")

async def executeIterator(indexLookupInformation : LookupInformation,iterator : LookupIterator, output : queue.Queue ) -> None:
    await iterator.getRanges(indexLookupInformation,output)
    print("executor finished")

class OrIterator(LookupIterator):
    def __init__(self,rng : RangeLookup):
        self._rangeQueue=queue.Queue()
        self._rangeQueue.put(rng)

    def __init__(self):
        self._rangeQueue=queue.Queue()

    def addRange(self, rng):
        self._rangeQueue.put(rng)

    async def getRanges(self,indexLookupInformation : LookupInformation, queue : queue.Queue):
        loop = asyncio.get_running_loop()

        with concurrent.futures.ThreadPoolExecutor() as pool:
            while not self._rangeQueue.empty():
                rng = self._rangeQueue.get()
                result = await loop.run_in_executor(pool, lookupRange,indexLookupInformation, rng, queue)

        #loop.close()
        



class IndexLookup(LuceneTreeVisitorV2):
    def __init__(self):
        print("here")

    def visit_and_operation(self, *args, **kwargs):
        return self._binary_operation("AND", *args, **kwargs)
    
    def visit_or_operation(self, *args, **kwargs):
        return self._binary_operation("OR", *args, **kwargs)


    def _binary_operation(self, op_type_name, node, parents, context):
        child_context = dict(context) if context is not None else {}
        operation="OR"
        if op_type_name == "AND":
            operation="AND"
        else:
            operation="OR"

        children = self.simplify_if_same(node.children, node)
        children = self._yield_nested_children(node, children)
        if child_context.get("need_in", False):
            child_context["in"] = True
        #children = node.children
        items = [self.visit(child, parents + [node], child_context) for child in
                 children]
        #We are selecting columns
        iter = OrIterator()
  
        for lookup in items:
            if lookup.getValue() == "or":
                operation="OR"
            elif lookup.getValue() == "and":
                operation="AND"
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
            >>> op = OrOperation(Word('yo'), OrOperation(Word('lo'), Word('py')))
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


async def produceShardRanges(indexLookupInformation : LookupInformation,output : asyncio.Queue, iterator: LookupIterator):
        ranges = queue.Queue()
        await executeIterator(indexLookupInformation,iterator,ranges)
        while not ranges.empty():
            rng = ranges.get()
            print("Produced shard range")
            await output.put(rng)
        

async def getDocuments(lookupInformation : LookupInformation,input : asyncio.Queue, outputQueue : queue.Queue):
    while True:
        docInfo = await input.get()
        print ("Looking up doc " + docInfo.getShard())
        tableOps = lookupInformation.getTableOps()
        print("wut")
        scanner = tableOps.createScanner(lookupInformation.getAuths(),1)
        print("okay")
        startKey = pysharkbite.Key()
        endKey = pysharkbite.Key()
        print("3")
        startKey.setRow(docInfo.getShard())
        print("Found " + docInfo.getShard())
        docid = docInfo.getDataType() + "\x00" + docInfo.getDocId();
        startKey.setColumnFamily(docid)
        endKey.setRow(docInfo.getShard())
        endKey.setColumnFamily(docid + "\xff")
        rng = pysharkbite.Range(startKey,True,endKey,True)

        scanner.addRange(rng)

        with open('jsoncombiner.py', 'r') as file:
            combinertxt = file.read()
            combiner=pysharkbite.PythonIterator("PythonCombiner",combinertxt,100)
            scanner.addIterator(combiner)
        print("start") 
        resultset = scanner.getResultSet()
        for keyvalue in resultset:
            key = keyvalue.getKey()
            value = keyvalue.getValue()
            outputQueue.put( value.get() )
        print("done")
        scanner.close()

        input.task_done()

async def lookup(indexLookupInformation : LookupInformation, docLookupInformation : LookupInformation,iterator: LookupIterator, documents : queue.Queue):
    print ("looking up")

    asyncQueue = asyncio.Queue()

    # fire up the both producers and consumers
    producers = [asyncio.create_task(produceShardRanges(indexLookupInformation,asyncQueue,iterator))
                 for _ in range(2)]
    consumers = [asyncio.create_task(getDocuments(docLookupInformation,asyncQueue,documents))
                 for _ in range(10)]


    await asyncio.gather(*producers)

    await asyncQueue.join()

    for c in consumers:
        c.cancel()




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
#         print(day + " " + key.getRow() + " " + value.get())
#         binstream = ByteArrayInputStream(value.get().encode("latin1"))i
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

      entry = request.GET.get('q').lower()
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
      wanted_items = set()
      tree = parser.parse(entry)
      tree = resolver(tree)
      visitor = IndexLookup()
      iterator = visitor.visit(tree)
      if isinstance(iterator, RangeLookup):
        print("wearerangelookup")
        iterator = OrIterator(iterator)
      docs = queue.Queue()
      asyncio.run( lookup(indexLookupInformation,shardLookupInformation,iterator,docs))

      counts = 0
      while not docs.empty():
        wanted_items.add(docs.get())
        counts=counts+1
      print ("technically finished")
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
