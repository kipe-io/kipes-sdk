###############################################################################
# iexcloud container
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
#     -p 8083:8083
#     --network=tradingpulse
#     ageworks/iexcloud
#
###############################################################################

# rocksdb doesn't play well with alpine 
FROM openjdk:16

# install kafka
RUN curl -o /tmp/kafka.tgz https://downloads.apache.org/kafka/3.1.0/kafka_2.13-3.1.0.tgz && \
    tar xzf /tmp/kafka.tgz -C /opt/ && \
    rm -f /tmp/kafka.tgz
    
ENV KAFKA_BASE /opt/kafka_2.13-3.1.0

ENV PATH="${KAFKA_BASE}/bin:${PATH}"

###############################################################################
# PARAMETERS
###############################################################################

# instance id used as base for port numbers
ENV KAFKA_CFG_PLUGIN_PATH /plugins

###############################################################################
# SETUP
###############################################################################

ARG JAR_FILE
ADD target/${JAR_FILE} ${KAFKA_CFG_PLUGIN_PATH}/connector.jar

ADD target/classes/log4j.properties ${KAFKA_BASE}/config/connect-log4j.properties

ADD distributed.docker.properties /connect.properties

###############################################################################
# START
###############################################################################
CMD connect-distributed.sh connect.properties 
