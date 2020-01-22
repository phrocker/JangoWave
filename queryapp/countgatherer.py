from org.poma.accumulo import Key,KeyValue
from org.apache.accumulo.core.data import Range,Value
from org.apache.hadoop.io import WritableUtils
from java.io import ByteArrayInputStream, DataInputStream
import struct
import json


#def swap32(i):
#    return struct.unpack("<L", struct.pack(">L", i))[0]

def swap32(x):
    return (((x << 24) & 0xFF000000) |
            ((x <<  8) & 0x00FF0000) |
            ((x >>  8) & 0x0000FF00) |
            ((x >> 24) & 0x000000FF))

class MetadataCounter: 
## This example iterator assumes the CQ contains a field name
## and value pair separated by a null delimiter
  def onNext(self,iterator):
    if (iterator.hasTop()):
        mapping = {}
        key = iterator.getTopKey()
        cf = key.getColumnFamily()
        if cf != "f":
          while (iterator.hasTop() and "f" != key.getColumnFamily()):
            iterator.next()
            key = iterator.getTopKey()
        if not iterator.hasTop():
          return None
        value = iterator.getTopValue()
        size = "0"
        if value.getSize() > 0:
          bs = ByteArrayInputStream(value.get())
          ds = DataInputStream(bs)   
          size = str( swap32( WritableUtils.readVLong( ds ) ))
#          size = str( WritableUtils.readVLong( ds ) )
        newval = Value(size)
        kv = KeyValue(key,newval)
        iterator.next()
        return kv
    else: 
       return None
