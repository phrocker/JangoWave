import nipyapi
from nipyapi import canvas, config, nifi
from random import randrange


import argparse

parser = argparse.ArgumentParser(description='Deploy the sample flow.')
parser.add_argument('--host', dest='nifi_host',
                   help='nifi host name')

args = parser.parse_args()

nifi_host = args.nifi_host
zookeeper_list = args.zookeeper_list

if not nifi_host.endswith("/"):
   nifi_host = nifi_host + "/"

nipyapi.config.nifi_config.host = nifi_host + "nifi-api"

root_pg_id = nipyapi.canvas.get_root_pg_id()

print(root_pg_id)

template_entity = nipyapi.templates.get_template_by_name("jangowave_demo")
nipyapi.templates.delete_template(template_entity.id)
