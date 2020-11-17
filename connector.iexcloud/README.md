# Stage connector - stg-connect

## Run docker container (after packaging)

    $ docker run -p 8083:8083 ageworks/iexcloud

## Show last offsets

    $ kafka-console-consumer.sh --bootstrap-server pkc-lq8v7.eu-central-1.aws.confluent.cloud:9092 --consumer.config ~/.kafka/client.config.confluent --topic stg-connect-offsets --from-beginning --property print.key=true

