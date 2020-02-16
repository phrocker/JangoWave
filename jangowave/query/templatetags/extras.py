import json

from django import template

register = template.Library()

def remove_if_present(field,in_dict):
  if field in in_dict:
    del in_dict[field]


@register.filter
def remove_fields(json):
  jayson=list()
  for js in json:
    remove_if_present('uid',js)
    remove_if_present('shard',js)
    remove_if_present('ORIG_FILE',js)
    remove_if_present('LOAD_DATE',js)
    remove_if_present('datatype',js)
    jayson.append(js)
  return jayson 

@register.filter
def get_item(dictionary, key):
   if not dictionary.get(key) is None: 
     return dictionary.get(key)
   else:
     return ""

@register.filter
def pretty_json(js):
    remove_if_present('uid',js)
    remove_if_present('shard',js)
    remove_if_present('datatype',js)
    return json.dumps(js, indent=4)
