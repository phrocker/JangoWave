# JangoWave
Apache Accumulo Query application built in python. 

## Requirements

    celery
    protobuf
    sharkbite
    google-cloud
    django==2.2.10
    pyparsing
    sortedcontainers
    django-adminlte3
    django-slick-admin
    django-stronghold
    luqum
    django_redis

### Building

Run ./build.sh from root or the following commands:

    docker-compose build --force-rm --no-cache && docker-compose up --detach
    docker-compose run web migrate.py makemigrations
    docker-compose run web migrate.py migrate

Please note that you will need to run docker-compose run web createsuperuser to create your first
user
    
    docker-compose run web migrate.py createsuperuser