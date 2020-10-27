# de.tradingpulse

Repository for all tradingpulse micro-services.

## Preconditions

- Java 11
- Maven 3.6.3
- access to confluent cloud
- (bash, kafka cmdline tools for our commandline tools)

## Setup local environment

- copy `stage.*.service/src/main/resources/application-confluent.example.yml` to `application.yml` and adjust the file to match your setup

## Run

1) build everything local
	
	> mvn clean install
	
2) run the sourcdata stage

	> cd stage.sourcedata.service
	> mvn exec:exec

3) generate some test data

	> cat <t.b.s> | kafka-console-producer.sh -- <todo parameters>

4) validate the sourcedata stage topics

