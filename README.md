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
    django-extensions
    pyjnius

Jangowave works with a variety of iterators. This project requires python iterators, which 
can be found in [sharkbite](https://github.com/phrocker/sharkbite/tree/master/native-iterators-jni)

The demo, which can be created from two scripts, sets up the containers with a defined flow. Note
that this is a base flow. It is expected that you can either deploy the NiFi container on your own
or augment the flow defined in the jangowave_demo.xml template. 

## General points

While this app has been tested with some additional query iterators on production environments, the
repo doesn't contain these references yet. That code will be provided soon ( upon first release ).

There are some assumptions made currently. Namely that searches are synchronous. This will be changed very soon
to facilitate asynchronous searches that use caching to speed up time to first result.

### Building Demo

You can quickly and easily build a demo using the scripts provided.  If you wish to set up the demo
on a single node you can use [fluo-uno](https://github.com/apache/fluo-uno)

Run ./build.sh from root or the following commands:

    docker-compose build --force-rm --no-cache && docker-compose up --detach
    docker-compose run web migrate.py makemigrations
    docker-compose run web migrate.py migrate

Once this completes you will see containers running for Apache NiFi, celery, redis, and django.

The setup.py script will set up NiFi and Djano with the appropriate data to begin the demo.

Note that the demo will not use provenance to track ingest. Instead, a flow will be setup in NiFi
that receives data from your Jangowave app. Data will be ingested and queryable within a second. 

    setup.sh <accumulo instanceid> <zookeeperlist>
