###############################################################################
# indicators.service container
# -----------------------------------------------------------------------------
#
# Build
# -----------------------------------------------------------------------------
# mvn package # will build the image
#
#
# Run
# -----------------------------------------------------------------------------
# docker run \
#     -d
#     -p 48200:48200
#     -p 58200:58200
#     --network=tradingpulse
#     ageworks/indicators.service
#
###############################################################################

# rocksdb doesn't play well with alpine 
FROM openjdk:16

###############################################################################
# PARAMETERS
###############################################################################

# instance id used as base for port numbers
ENV IID=8200

# the application environment
ENV MICRONAUT_ENV local

# Kafka bootstrap servers
ENV KAFKA_BOOTSTRAP_SERVERS kafka:29092
# workaround for https://github.com/micronaut-projects/micronaut-kafka/issues/179
ENV kafka.bootstrap.servers kafka:29092 

# RMI host to be used when answering JMX connection requests
ENV RMI_HOST 127.0.0.1 

# additional java options
ENV JAVA_OPTS -Djava.rmi.server.hostname=$RMI_HOST -Dcom.sun.management.jmxremote.port=4$IID -Dcom.sun.management.jmxremote.rmi.port=5$IID -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false

###############################################################################
# SETUP
###############################################################################

ARG JAR_FILE
ADD target/${JAR_FILE} stage.jar
RUN sh -c 'touch /stage.jar'

###############################################################################
# START
###############################################################################
CMD exec java $JAVA_OPTS -Djava.security.egd=file:/dev/./urandom -Dmicronaut.environments=${MICRONAUT_ENV} -jar /stage.jar
