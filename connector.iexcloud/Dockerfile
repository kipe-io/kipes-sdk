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
#     ageworks/iexcloud
#
###############################################################################

# rocksdb doesn't play well with alpine 
FROM openjdk:16

# install kafka
RUN curl -o /tmp/kafka.tgz https://downloads.apache.org/kafka/2.6.0/kafka_2.13-2.6.0.tgz && \
    tar xzf /tmp/kafka.tgz -C /opt/ && \
    rm -f /tmp/kafka.tgz
ENV PATH="/opt/kafka_2.13-2.6.0/bin:$PATH"

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

ADD distributed.docker.confluent.properties /connect.properties

###############################################################################
# START
###############################################################################
CMD connect-distributed.sh connect.properties 
