
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
            protobuf.ParseFromString(value.get().encode())
            for uidvalue in protobuf.UID:
              shard = indexKeyValue.getKey().getColumnQualifier().split("\u0000")[0]
              datatype = indexKeyValue.getKey().getColumnQualifier().split("\u0000")[1]
              self._peek=Range(datatype,shard,uidvalue)
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

