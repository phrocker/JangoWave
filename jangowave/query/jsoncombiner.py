from org.poma.accumulo import Key,KeyValue
from org.apache.accumulo.core.data import Range,Value
import json
class PythonCombiner: 
## This example iterator assumes the CQ contains a field name
## and value pair separated by a null delimiter
  def onNext(self,iterator):
    if (iterator.hasTop()):
        mapping = {}
        key = iterator.getTopKey()
        cf = key.getColumnFamily()
        while (iterator.hasTop() and cf == key.getColumnFamily()):
            ## FN and FV in cq
            split = key.getColumnQualifier().split('\x00');
            fieldvalue="EMPTY"
            if len(split) >= 1:
              fieldname = split[0]
            if len(split) >= 2:
              fieldvalue = split[1];
            mapping[fieldname]=fieldvalue
            iterator.next();
            if (iterator.hasTop()):
                key = iterator.getTopKey()
        json_data = json.dumps(mapping,indent=4, sort_keys=True);
        value = Value(json_data);
        kv = KeyValue(key,value)
        return kv
    else: 
       return None
