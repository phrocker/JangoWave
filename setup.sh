#!/bin/bash

#setup.sh<accumulo instanceid> <zookeeperlist>
status=1
ACC_INST=$1
ZK_LIST=$2

if [ -z "$ACC_INST" ] || [ -z "$ZK_LIST" ]; then
   echo " Must provide accumulo instance and zookeeper"
   exit
fi
echo "Setting up Apache NiFi"
while [ $status -ne 0 ]
do
	cmd="python3.7 deploy_flow.py --host http://localhost:8080/ --zookeepers ${ZK_LIST}"
	$cmd 2>/dev/null
	status=$?
	if [ $status -ne 0 ]; then
		echo "NiFi not available, sleeping 10 seconds..."
		cmd="python3.7 delete_template.py --host http://localhost:8080/ --zookeepers ${ZK_LIST}"
		$cmd 2>/dev/null
		sleep 10
	fi
done

## deploy and start the reporting task

python3.7 deploy_reportingtask.py  --host http://localhost:8080/ --zookeepers ${ZK_LIST}

echo "Apache NiFi setup complete"

echo "Setting up Jangowave App. You will be prompted for the initial admin username and password"

docker-compose run web python3.7 manage.py createsuperuser

docker-compose run web python3.7 manage.py runscript startdemo --script-args instance=${ACC_INST} zookeepers=${ZK_LIST}
