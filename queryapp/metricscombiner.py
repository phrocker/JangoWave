from org.poma.accumulo import Key,KeyValue
from org.apache.accumulo.core.data import Range,Value
from org.apache.hadoop.io import WritableUtils
from java.io import ByteArrayInputStream, DataInputStream
from java.lang import Long
import json

class MetadataCounter: 
## This example iterator assumes the CQ contains a field name
## and value pair separated by a null delimiter
  def onNext(self,iterator):
    if (iterator.hasTop()):
        mapping = {}
        retkey = iterator.getTopKey()
        size=0
        row = retkey.getRow()
        day = row.split("_")[0]
        while iterator.hasTop():
          key = iterator.getTopKey()
          if key.getRow().split("_")[0] != day:
            break
          if key.getColumnFamily() == "EVENT_COUNT":
            value = iterator.getTopValue() 
            if value.getSize() > 0:
              try:
                size += int(value.get())
              except:
                pass
            retkey=key
          iterator.next()
        newval = Value(str(size))
        kv = KeyValue(retkey,newval)
        if retkey.getColumnFamily() == "EVENT_COUNT":
          return kv
        else:
          return None
    else: 
       return None
