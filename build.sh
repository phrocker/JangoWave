#!/bin/bash
docker-compose build --force-rm --no-cache && docker-compose up --detach && docker-compose run web migrate.py makemigrations && docker-compose run web migrate.py migrate
echo "complete..."
