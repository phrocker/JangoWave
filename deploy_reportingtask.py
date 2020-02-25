import nipyapi
from nipyapi import canvas, config, nifi
from random import randrange

import argparse

parser = argparse.ArgumentParser(description='Deploy the sample flow.')
parser.add_argument('--host', dest='nifi_host',
                   help='nifi host name')
parser.add_argument('--zookeepers', dest='zookeeper_list', default="localhost:2181",
                   help='nifi host name')
parser.add_argument('--instance', dest='instance', default="uno",
                    help='instance name')
args = parser.parse_args()

nifi_host = args.nifi_host
instance = args.instance
zookeeper_list = args.zookeeper_list


if not nifi_host.endswith("/"):
   nifi_host = nifi_host + "/"

nipyapi.config.nifi_config.host = nifi_host + "nifi-api"

root_pg_id = nipyapi.canvas.get_root_pg_id()

#print(root_pg_id)
import requests

post_path = nifi_host + "nifi-api/controller/reporting-tasks"
import json
outy = {
    'revision': {
    'clientId': 'NiPyAPI',
    'version': 0,
    'lastModifier': 'value'
},
'component': {
'state' : 'RUNNING',
'type':'org.apache.nifi.accumulo.reporting.AccumuloReportingTask',
'schedulingPeriod': '5s',
'bundle': {'artifact':'nifi-accumulo-nar','group':'org.apache.nifi','version':'1.11.2'},
              'name':'AccumuloReportingTask',
              'properties': {'Table Name': 'provenance','Accumulo Password':'secret',
                             'Accumulo User':'root',
                             'Instance Name':instance,
                             'ZooKeeper Quorum': zookeeper_list}
               }}
headers = {'Content-Type': 'application/json' }
print(json.dumps(outy))
print("Posting to " + post_path)
output = requests.post(url = post_path, data=json.dumps(outy),headers=headers)
json_parsed = json.loads(output.text)
uri = json_parsed['uri'] + "/run-status"
update_state = {
    'state' : 'RUNNING','revision': {
    'clientId': 'NiPyAPI',
    'version': 1,
    'lastModifier': 'status'
}
}
print("Sending to " + uri)
output = requests.put(url = uri, data=json.dumps(update_state),headers=headers)
