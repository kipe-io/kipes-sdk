# de.tradingpulse

Repository for all tradingpulse micro-services.

## Preconditions

- Java 11
- Maven 3.6.3
- bash, kafka cmdline tools
- docker including post-installation steps 'docker as non-root', 'start on boot'
- docker-compose

## Setup local environment

### 1) Get Kafka running locally

Follow the steps outlined at `infrastructure/kafka/README.md`.

### 2) Prepare all application-local.yml files

- copy all `stage.*.service/src/main/resources/application-local.example.yml` to `application-local.yml` and adjust the files to match your setup

## Run Stage Services

*Note: In the following sections we assume you are running a default kafka setup without further hardening like passwords etc.*

### 1) build everything local
	
    $ mvn clean install
	
### 2) run the sourcedata stage

    $ cd stage.sourcedata.service
    $ mvn exec:exec

### 3) generate some test data

    $ cat stage.sourcedata.service/exampledata/20200831_AAPL_1y.json | kafka-console-producer.sh --bootstrap-server 127.0.0.1:9092 --topic stg-sourcedata-ohlcv_daily_raw 

### 4) validate the sourcedata stage topics 

- list all sourcedata stage topics (*WARNING, don't delete them, it's just to list them now*)

    $ ./delete-topics.sh sourcedata

- you should see (amongst others)
  - stg-sourcedata-ohlcv_daily_raw
  - stg-sourcedata-ohlcv_daily
  - stg-sourcedata-ohlcv_weekly

- check the test records you have send to `stg-sourcedata-ohlcv_daily_raw` at point three.

    $ kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic stg-sourcedata-ohlcv_daily_raw --from-beginning --property print.key=true
	
- check the normalized records at `stg-sourcedata-ohlcv_daily`

    $ kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic stg-sourcedata-ohlcv_daily --from-beginning --property print.key=true
	
- check the derived weekly records at `stg-sourcedata-ohlcv_weekly`

    $ kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic stg-sourcedata-ohlcv_weekly --from-beginning --property print.key=true

Please note an important concept here: all records are so-called incremental aggregates. The basic, atomic change to an asset would be a TICK. All other records are essentially aggregations of multiple TICKs. That's the reason we are handling two timestamps for each record: the key timestamp at which the change happened (the TICK timestamp), and the record's timerange timestamp at which the record's data describe the current (incremental) aggregation values.

### 5) run & verify the other stages

There are currently these stages (in order):
- `sourcedata` (from above) - provides OHLCV records, currently in daily and weekly resolution
- `indicators` - provides EMAs, MACDs
- `systems` - provides Impulse system records
- `tradingscreens` - provides trading screen records and entry signals

Run each of the stage services like in point two and verify by listing the topics and digging through the records. Make sure you run the services at least once orderly to have all topics filled.

Finally, check whether you see entry signals as described at `stage.tradingscreens.service/README.md`

## Shutdown & Restart Stage Services

To shutdown a service simply CTRL+C. To resume the work simply restart the service. 

If you want a service to redo it's work, to re-process records there are multiple ways to do it. The easiest way is - without going into the details - to delete the stage's topics and reset the stage's offsets by using the `delete-topics.sh` script.

    $ ./delete-topics.sh <STAGENAME>
    ...
    Are you sure to delete those topics? [y/N]: y
    ...
    Do you want to reset related offsets for these group(s)? [Y/n]: y
    ...

Once you restart the stage service the topics will be recreated and the input records get processed again. Note: the <STAGENAME> parameter is a regexp, actually. 

### 6) Cleanup everything and prepare real data

Since we added example data at step 3 and we now want to connect to a real data provider we need to cleanup all topics and reset the offsets. We are going to do this by simply deleting all topics.

Make sure, all services are stopped.

    $ ./delete-topics.sh stg-*

At this point your Kafka cluster has no topics and data from this project left. 

### 7) Setup, run and verify the IEXCloud Connector

- Make sure you have access to iexcloud.io api.
- Run the sourcedata stage service once to setup the topics (see step 2).

The IEXCloud Connector is based on Kafka Connect, an application which can be enhanced by connectors (plugins). The IEXCloud Connector is such connector. Since we need to start Kafka Connect, the steps to get the connector working differ from the stage service.

- Follow the steps outlined at `connector.iexcloud/README.md`

### 8) Run everything with docker-compose

- Make sure the Kafka service are already running (see `infrastructure/kafka/README.md`)
- Make sure all other services are *not* running.

    $ mvn clean install
    $ docker-compose up
    
