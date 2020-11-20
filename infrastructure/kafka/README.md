# Setup Kafka for Local Development

## Preconditions

- docker installed
- docker-compose installed


## Prepare local data persistence volumes 

*
Note: This step is only needed if you want to preserve the data of kafka (topics, records, etc.pp) in a local folder (you could also create volumes)

If you skip this step you will need to adjust the docker-compose.yml and env files in this directory to NOT reference the local folders.
*

**Create following two folders (use whatever location suites you):**

    root@...$ mkdir -p /opt/lib/docker/kafka/kafka-data
    root@...$ mkdir -p /opt/lib/docker/kafka/zookeeper-data

**Adjust permissions:**

We are using the non-root bitnami/kafka container. It uses the UID 1001 to run all services. Depending on your system your user (which we assume is the one you use to run docker containers is member of the docker group) your user id might **not** be the 1001.

Hence, we are changing the ownership of the above created folders to that UID.
 
    root@...$ chown -R 1001:docker /opt/lib/docker/kafka

*
Note: you might ask why we don't use the `user: ${UID}:${GID}` feature instead. Answer is simple: we didn't got that working. The containers pick that up but complain later about permission problems although the folders were correctly set up. 

If you find the solution, let us know. It would be better to tell the container which UID to use instead of using that predefined one.
*

## Prepare env.local

**Setup env.local**

- copy the default file

    $ cp env.local.example env.local

- adjust the parameters

## Create the application network

We create an network to be able to add services later, started from other compose files.

    $ docker network create tradingpulse
    
## Startup Service

    $ docker-compose --env-file=env.local up

*Note: you may want to add `-d` as a parameter to start the services in daemon mode.*

## Stop Services

- just CTRL-C if not started in daemon mode
- if daemon mode 

    $ docker-compose --env-file=env.local stop
    
    