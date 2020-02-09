import json

from django import template

register = template.Library()


@register.filter
def remove_fields(json):
  jayson=list()
  for js in json:
    del js['uid']
    del js['shard']
    del js['ORIG_FILE']
    del js['LOAD_DATE']
    del js['datatype']
    jayson.append(js)
  return jayson 

@register.filter
def pretty_json(js):
    del js['uid']
    del js['shard']
    del js['datatype']
    return json.dumps(js, indent=4)
