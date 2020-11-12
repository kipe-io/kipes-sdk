###############################################################################
# sourcedata.service container
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
#     -p 38900:38900
#     -p 48900:48900
#     -p 58900:58900
#     ageworks/sourcedata.service
#
###############################################################################

# rocksdb doesn't play well with alpine 
FROM openjdk:16

###############################################################################
# PARAMETERS
###############################################################################

# instance id used as base for port numbers
ENV IID 8900

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
CMD exec java $JAVA_OPTS -Djava.security.egd=file:/dev/./urandom -jar /stage.jar
