#!/bin/bash

docker-compose run web python3.7 manage.py makemigrations

docker-compose run web python3.7 manage.py migrate
echo "complete..."

