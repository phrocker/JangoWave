#!/bin/bash
docker-compose build --force-rm --no-cache && docker-compose up --detach

echo "Performing migrations"

docker-compose run web python3.7 manage.py makemigrations

docker-compose run web python3.7 manage.py migrate

docker-compose run web sysctl net.ipv4.conf.all.forwarding=1
docker-compose run celery sysctl net.ipv4.conf.all.forwarding=1
