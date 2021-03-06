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

print(root_pg_id)

root_pg = canvas.get_process_group(root_pg_id, 'id')

location_x = 2000
location_y = 2000

location = (location_x, location_y)



template_entity = nipyapi.templates.upload_template(root_pg_id,'jangowave_demo.xml')

flow = nipyapi.templates.deploy_template(root_pg_id,template_entity.id,2000,2000)


jd = canvas.get_process_group('jangowave_demo')
for cs in canvas.list_all_processors(jd.id):       
    if cs.status.run_status != "ENABLED" and cs.component.name == "RecordIngest":
        config_update = nifi.models.processor_config_dto.ProcessorConfigDTO(properties={"Accumulo User": "root", "Instance Name": instance, "Accumulo Password": "secret", "ZooKeeper Quorum": zookeeper_list })
        canvas.update_processor(cs,config_update)
for cs in canvas.list_all_controllers(jd.id):
    canvas.schedule_controller(cs,True)
for cs in canvas.list_all_processors(jd.id):
    canvas.schedule_processor(cs,True)
canvas.schedule_process_group(jd.id,True)

