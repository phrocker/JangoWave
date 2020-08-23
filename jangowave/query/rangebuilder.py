from django.utils.decorators import method_decorator
from django.contrib.auth.decorators import login_required
from django.shortcuts import render
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
    def __init__(self,datatype,shard, docid, field=None, value=None):
        self._datatype=datatype
        self._shard=shard
        if not field is None:
          self._ignore=True
        else:
          self._ignore=False
        self._docid=docid
        self._field=field
        self._ivalue=value

    def getDataType(self):
        return self._datatype

    def getVal(self):
        return self._ivalue

    def getField(self):
        return self._field

    def getShard(self):
        return self._shard

    def isIgnore(self):
        return self._ignore

    def getDocId(self):
        return self._docid

    def __eq__(self, other):
        if other is None:
         return False
        return self._shard == other._shard and self._docid == other._docid

    def __gt__(self, other):
       if other is None:
         return True
       return self._shard > other._shard and self._docid > other._docid

    def __lt__(self, other):
       if other is None:
         return False
       return self._shard < other._shard and self._docid < other._docid

class RangeLookup:
    def __init__(self,field,value, upperbound=None):
        self._field=field
        self._value=value
        self._upperbound=upperbound
    
    def getValue(self):
        return self._value

    def upperbound(self):
        return self._upperbound

    def getField(self):
        return self._field

EndOfIter=object()

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
            print("for " + str(indexKeyValue.getKey()))
            try:
              protobuf.ParseFromString(value.get().encode())
              print("gotcha')")
              if (protobuf.IGNORE or len(protobuf.UID) == 0):
                print("oh no")
                shard = indexKeyValue.getKey().getColumnQualifier().split("\u0000")[0]
                datatype = indexKeyValue.getKey().getColumnQualifier().split("\u0000")[1]
                self._peek=Range(datatype,shard,"",indexKeyValue.getKey().getColumnFamily(),indexKeyValue.getKey().getRow())
              else:
                for uidvalue in protobuf.UID:
                  shard = indexKeyValue.getKey().getColumnQualifier().split("\u0000")[0]
                  datatype = indexKeyValue.getKey().getColumnQualifier().split("\u0000")[1]
                  self._peek=Range(datatype,shard,uidvalue)
            except:
              print("oh fhi")
              shard = indexKeyValue.getKey().getColumnQualifier().split("\u0000")[0]
              datatype = indexKeyValue.getKey().getColumnQualifier().split("\u0000")[1]
              self._peek=Range(datatype,shard,"",indexKeyValue.getKey().getColumnFamily(),indexKeyValue.getKey().getRow())
        except StopIteration:
            self._peek = EndOfIter
        except:
            traceback.print_exc()
            print("error?")
            self._peek = EndOfIter
        return cur

    def peek(self):
        return self._peek

class LookupIterator(object):
    def __init__(self,rangeQueue):
        self._rangeQueue=rangeQueue
        self._indexLookupInformation=None
        self._tableOps=None
        self._scnrs=None
        self._rngs=None
        self._iter=None

    def __init__(self,rngs,indexLookupInformation):
        self._rangeQueue=rngs
        self._indexLookupInformation=indexLookupInformation
        self._tableOps=indexLookupInformation.getTableOps()
        self._scnrs=None
        self._rngs=None
        self._iter=None

    def __next__(self):
      if self._iter is None:
        raise StopIteration;
      cur = None
      try:
        cur = self._iter.__next__()
      except StopIteration:
        for scnr in self._scnrs:
          if not scnr is None:
            scnr.close()
        raise StopIteration
      return cur

    def combineScanners(self,scanners):
      return None

    def __iter__(self):
      if self._indexLookupInformation is None:
        return self

      indexTableOps = lookupInformation.getTableOps()

      self._rngs = [None] * len(rangeQueue)
      self._scnrs = [None] * len(rangeQueue)
      self._iters = [None] * len(rangeQueue)
      count=0
      for rng in rangeQueue:
        if isinstance(rng,LookupIterator):
          self._iters[count]=rng
          self._scnrs=None
        else:
          print("o9hh")
          scnrs[count] = indexTableOps.createScanner(lookupInformation.getAuths(),1)
          indexrange = pysharkbite.Range(rng.getValue())
          scnrs[count].addRange(indexrange)
          if not  rng.getField() is None:
            scnrs[count].fetchColumn(rng.getField().upper(),"")
          self._iters=ForwardIterator(scnrs[count].getResultSet())
          count=count+1

      self._iter=combineScanners(self._iters)

      return self

    def getRanges(self,indexLookupInformation : LookupInformation, queue):
      pass


