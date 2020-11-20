# Stage connector - stg-connect

## Preconditions

- ideally, start from `../README.md`
- kafka is running locally (see `../infrastructure/kafka/README.md`)
- stage.sourcedata.service is running, or ran at least once and the sourcedata topics are available

## Setup Kafka Connect

The IEXCloud Connector is based on Kafka Connect, an application which can be enhanced by connectors (plugins). The IEXCloud Connector is such connector. 

### 1) Setup distributed.docker.properties

    $ cp distributed.docker.local.example.properties distributed.docker.properties
    
Adjust the settings to match your setup.

### 2) Create the container

    $ mvn clean install
    
### 3) Run docker container (after packaging)

    $ docker run -p 8083:8083 --network=tradingpulse ageworks/iexcloud

## Setup the IEXCloud Connector

### 4) Setup distributed.iexcloudconnector.json

    $ cp distributed.iexcloudconnector.example.json distributed.iexcloudconnector.json

Adjust the settings to match your setup. Make sure, the configured `topic` is available at the kafka cluster.

### 5) Install the connector

    $ curl -X PUT -H "Content-Type: application/json" -d @distributed.iexcloudconnector.json http://127.0.0.1:8083/connectors/iex-cloud-ohlcv/config

## Helpful Tools

**Show last offsets**

    $ kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic stg-connect-offsets --from-beginning --property print.key=true
    
**Show the current connect configuration**

    $ kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic stg-connect-configs --from-beginning --property print.key=true

**Update the IEXCloud Connector configuration**

- adjust the `distributed.iexcloudconnector.json`
- re-run step 5
