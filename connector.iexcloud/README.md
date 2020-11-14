# Stage connector - stg-connect

## Show last offsets

    $ kafka-console-consumer.sh --bootstrap-server pkc-lq8v7.eu-central-1.aws.confluent.cloud:9092 --consumer.config ~/.kafka/client.config.confluent --topic stg-connect-offsets --from-beginning --property print.key=true

