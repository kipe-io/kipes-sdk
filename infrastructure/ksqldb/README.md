# ksqlDB Setup

## Prerequisites

- Kafka is running (see `../kafka/README.md`)

## Startup ksqlDB

    $ docker-compose up

## Open ksql commandline

    $ docker exec -it ksqldb-cli /bin/ksql http://ksqldb:8088
    