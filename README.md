***
# NOTICE:
 
## This repository has been archived and is not supported.
 
[![No Maintenance Intended](http://unmaintained.tech/badge.svg)](http://unmaintained.tech/)
***
NOTICE: SUPPORT FOR THIS PROJECT HAS ENDED 

This projected was owned and maintained by Walmart. This project has reached its end of life and Walmart no longer supports this project.

We will no longer be monitoring the issues for this project or reviewing pull requests. You are free to continue using this project under the license terms or forks of this project at your own risk. This project is no longer subject to Walmart's bug bounty program or other security monitoring.


## Actions you can take

We recommend you take the following action:

  * Review any configuration files used for build automation and make appropriate updates to remove or replace this project
  * Notify other members of your team and/or organization of this change
  * Notify your security team to help you evaluate alternative options

## Forking and transition of ownership

For [security reasons](https://www.theregister.co.uk/2018/11/26/npm_repo_bitcoin_stealer/), Walmart does not transfer the ownership of our primary repos on Github or other platforms to other individuals/organizations. Further, we do not transfer ownership of packages for public package management systems.

If you would like to fork this package and continue development, you should choose a new name for the project and create your own packages, build automation, etc.

Please review the licensing terms of this project, which continue to be in effect even after decommission.

# BigBen
`BigBen` is a generic, multi-tenant, time-based event scheduler and cron 
scheduling framework based on `Cassandra` and `Hazelcast`

It has following features:
* **Distributed** - `BigBen` uses a distributed design and can be deployed on 10's or 100's of machines and can be dc-local or cross-dc
* **Horizontally scalable** - `BigBen` scales linearly with the number of machines. 
* **Fault tolerant** - `BigBen` employs a number of failure protection modes and 
can withstand arbitrary prolonged down times
* **Performant** - `BigBen` can easily scale to 10,000's or even millions's of event triggers with a 
very small cluster of machines. It can also easily manage million's of crons running in a distributed manner
* **Highly Available** - As long as a single machine is available in the cluster, `BigBen` will guarantee the 
execution of events (albeit with a lower throughput)
* **Extremely consistent** - `BigBen` employs a single master design (the master itself is highly available with 
`n-1` masters on standby in an `n` cluster machine) to ensure that no two nodes fire the same event or execute 
the same cron.  
* **NoSql based** - `BigBen` comes with default implementation with `Cassandra` but can be easily extended
to support other `NoSql` or even `RDBMS` data stores  
* **Auditable** - `BigBen` keeps a track of all the events fired and crons executed with a configurable 
retention 
* **Portable, cloud friendly** - `BigBen` comes as application bundled as `war` or an embedded lib as `jar`, 
and can be deployed on any cloud, `on-prem` or `public`   

## Use cases
`BigBen` can be used for a variety of time based workloads, both single trigger based or repeating crons. 
Some of the use cases can be
* **Delayed execution** - E.g. if a job is to be executed 30 mins from now
* **System retries** - E.g. if a service A wants to call service B and service B is down at the moment, then 
service A can schedule an exponential backoff retry strategy with retry intervals of 
1 min, 10 mins, 1 hour, 12 hours, and so on.
* **Timeout tickers** - E.g. if service A sends a message to service B via `Kafka` and expects a response in 1 min, 
then it can schedule a `timeout check` event to be executed after 1 min
* **Polling services** - E.g. if service A wants to poll service B at some frequency, it can schedule a cron 
to be executed at some specified frequency
* **Notification Engine** - `BigBen` can be used to implement `notification engine` with scheduled deliveries, 
scheduled polls, etc
* **Workflow state machine** - `BigBen` can be used to implement a distributed `workflow` with state suspensions, 
alerts and monitoring of those suspensions.

## Architectural Goals
`BigBen` was designed to achieve the following goals:
* Uniformly distributed storage model
    * Resilient to hot spotting due to sudden surge in traffic
* Uniform execution load profile in the cluster
    * Ensure that all nodes have similar load profiles to minimize misfires
* Linear Horizontal Scaling 
* Lock-free execution
    * Avoid resource contentions
* Plugin based architecture to support variety of data bases like `Cassandra, Couchbase, Solr Cloud, Redis, RDBMS`, etc
* Low maintenance, elastic scaling   

## Design and architecture
See the blog published at [Medium](https://medium.com/walmartlabs/an-approach-to-designing-distributed-fault-tolerant-horizontally-scalable-event-scheduler-278c9c380637)
for a full description of various design elements of `BigBen`

## Events Inflow
`BigBen` can receive events in two modes:
* **kafka** - inbound and outbound Kafka topics to consume event requests and publish event triggers
* **http** - HTTP APIs to send event requests and HTTP APIs to receive event triggers.

*It is strongly recommended to use `kafka` for better scalability*

### Event Inflow diagram
![inflow](/docs/assets/inflow.png "Events Inflow diagram")

*Request and Response channels can be mixed. For example, the event requests can be sent through HTTP APIs but 
the event triggers (response) can be received through a Kafka Topic.*

## Event processing guarantees
`BigBen` has a robust event processing guarantees to survive various failures. 
However, `event-processing` is not same as `event-acknowledgement`. 
`BigBen` works in a no-acknowledgement mode (*at least for now*). 
Once an event is triggered, it is either published to `Kafka` or 
sent through an `HTTP API`. Once the `Kafka` producer returns success, or `HTTP API` returns non-500 status code, 
the event is **assumed** to be processed and marked as such in the system. 
However, for whatever reason if the event was not processed and resulted in an error 
(e.g. `Kafka` producer timing out, or `HTTP API` throwing `503`), 
then the event will be retried multiple times as per the strategies discussed below

### Event misfire strategy
Multiple scenarios can cause `BigBen` to be not able to trigger an event on time. Such scenarios are called 
misfires. Some of them are:
* `BigBen`'s internal components are down during event trigger. 
E.g. 
    * `BigBen`'s data store is down and events could not be fetched
    * `VMs` are down

* `Kafka` Producer could not publish due to loss of partitions / brokers or any other reasons
* `HTTP API` returned a 500 error code
* Any other unexpected failure

In any of these cases, the event is first retried in memory using an exponential back-off strategy. 

Following parameters control the retry behavior:

* _event.processor.max.retries_ - how many in-memory retries will be made before declaring the event as error, default is 3
* _event.processor.initial.delay_ - how long in seconds the system should wait before kicking in the retry, default is 1 second
* _event.processor.backoff.multiplier_ - the back off multiplier factor, default is 2. E.g. the intervals would be 1 second, 2 seconds, 4 seconds.

If the event still is not processed, then the event is marked as `ERROR`. 
All the events marked `ERROR` are retried up to a configured limit called `events.backlog.check.limit`. 
This value can be an arbitrary amount of time, e.g. 1 day, 1 week, or even 1 year. E.g. if the the limit
is set at `1 week` then any event failures will be retried for `1 week` after which, they will be permanently 
marked as `ERROR` and ignored. The `events.backlog.check.limit` can be changed at any time by changing the 
value in `bigben.yaml` file and bouncing the servers.

### Event bucketing and shard size
`BigBen` shards events by minutes. However, since it's not known in advance how many events will be 
scheduled in a given minute, the buckets are further sharded by a pre defined shard size. The shard size is a 
design choice that needs to be made before deployment. Currently, it's not possible to 
change the shard size once defined. 

An undersized shard value has minimal performance impact, however an oversized shard value may 
keep some machines idling. The default value of `1000` is good enough for most practical purposes as long as 
number of events to be scheduled per minute exceed `1000 x n`, where `n` is the number of machines in the cluster.
If the events to be scheduled are much less than `1000` then a smaller shard size may be chosen.      

### Multi shard parallel processing
Each bucket with all its shards is distributed across the cluster for execution with an algorithm that ensures a 
random and uniform distribution. The following diagram shows the execution flow.  
![shard design](https://cdn-images-1.medium.com/max/1600/1*euaHLOnw6G96SigfXxWhtA.png "BigBen processing flow")

### Multi-tenancy
Multiple tenants can use `BigBen` in parallel. Each one can configure how the events will be delivered once triggered.
Tenant 1 can configure the events to be delivered in `kafka` topic `t1`, where as tenant 2 can have them delivered
via a specific `http` url. The usage of tenants will become more clearer with the below explanation of `BigBen` APIs

## Docker support
BigBen is dockerized and image (`bigben`) is available on docker hub. The code also contains 
scripts, which start `cassandra`, `hazelcast` and `app`.
To quickly set up the application for local dev testing, do the following steps:
1. `git clone $repo`
2. `cd bigben/build/docker`
3. execute `./docker_build.sh`
4. start cassandra container by executing `./cassandra_run.sh`
5. start app by executing `./app_run.sh`
6. To run multiple app nodes `export NUM_INSTANCES=3 && ./app_run.sh`
6. wait for application to start on port `8080`
7. verify that `curl http://localhost:8080/ping` returns `200`
8. Use `./cleanup.sh` to stop and remove all `BigBen` related containers 

## Non-docker execution
`BigBen` can be run without docker as well. Following are the steps
1. `git clone $repo`
2. `cd bigben/build/exec`
3. execute `./build.sh`
4. execute `./app_run.sh`

## Env properties
You can set the following environment properties
1. `APP_CONTAINER_NAME` (default bigben_app)
2. `SERVER_PORT` (default 8080)
3. `HZ_PORT` (default 5701)
4. `NUM_INSTANCES` (default 1)
5. `LOGS_DIR` (default bigben/../bigben_logs)
6. `CASSANDRA_SEED_IPS` (default $HOST_IP)
7. `HZ_MEMBER_IPS` (default $HOST_IP)
8. `JAVA_OPTS`

#How to override default config values?
`BigBen` employs an extensive override system to allow someone to override 
the default properties. The order of priority is system properties > system env variables >
overrides > defaults
The overrides can be defined in `config/overrides.yaml` file.
The `log4j.xml` can also be changed to change log behavior without 
recompiling binaries

## How to setup `Cassandra` for `BigBen`?
Following are the steps to set up `Cassandra`:
1. git clone the `master` branch
2. Set up a Cassandra cluster
3. create a keyspace `bigben` in `Cassandra` cluster with desired replication
4. Open the file `bigben-schema.cql` and execute `cqlsh -f bigben-schema.cql`

## APIs

### cluster
`GET /events/cluster`
* response sample (a 3 node cluster running on single machine and three different ports (5701, 5702, 5703)):
```json
{
    "[127.0.0.1]:5702": "Master",
    "[127.0.0.1]:5701": "Slave",
    "[127.0.0.1]:5703": "Slave"
}
``` 
The node marked `Master` is the master node that does the scheduling.

### tenant registration
A tenant can be registered by calling the following API

`POST /events/tenant/register`
* payload schema
```json
{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "type": "object",
  "properties": {
    "tenant": {
      "type": "string"
    },
    "type": {
      "type": "string"
    },
    "props": {
      "type": "object"
    }
  },
  "required": [
    "tenant",
    "type",
    "props"
  ]
}
```
* `tenant` - specifies a tenant and can be any arbitrary value.
* `type` - specifies the type of `tenant`. One of the three types can be used
    * MESSAGING - specifies that `tenant` wants events delivered via a messaging queue. Currently, `kafka` 
    is the only supported messaging system.
    * HTTP - specifies that `tenant` wants events delivered via an http callback URL. 
    * CUSTOM_CLASS - specifies a custom event processor implemented for custom processing of events
* `props` - A bag of properties needed for each type of tenant. 

* kafka sample:
```json
{
    "tenant": "TenantA/ProgramB/EnvC",
    "type": "MESSAGING",
    "props": {
        "topic": "some topic name",
        "bootstrap.servers": "node1:9092,node2:9092"
    }
}
```
* http sample
```json
{
     "tenant": "TenantB/ProgramB/EnvC",
     "type": "HTTP",
     "props": {
          "url": "http://someurl",
          "headers": {
            "header1": "value1",
            "header2": "value2"
          }
     }
}
```     

### fetch all tenants:
`GET /events/tenants`

### event scheduling
`POST /events/schedule`

`Payload - List<EventRequest>`

`EventRequest` schema:
 
```json
{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "type": "object",
  "properties": {
    "id": {
      "type": "string"
    },
    "eventTime": {
      "type": "string",
      "description": "An ISO-8601 formatted timestamp e.g. 2018-01-31T04:00.00Z"
    },
    "tenant": {
      "type": "string"
    },
    "payload": {
      "type": "string",
      "description": "an optional event payload, must NOT be null with deliveryOption = PAYLOAD_ONLY"
    },
    "mode": { 
      "type": "string",
      "enum": ["UPSERT", "REMOVE"],
      "default": "UPSERT",
      "description": "Use REMOVE to delete an event, UPSERT to add/update an event"
    },
    "deliveryOption": {
      "type": "string",
      "enum": ["FULL_EVENT", "PAYLOAD_ONLY"],
      "default": "FULL_EVENT",
      "description": "Use FULL_EVENT to have full event delivered via kafka/http, PAYLOAD_ONLY to have only the payload delivered"
    }
  },
  "required": [
    "id",
    "eventTime",
    "tenant"
  ]
}
```

### find an event
`GET /events/find?id=?&tenant=?`

### dry run
`POST /events/dryrun?id=?&tenant=?`

fires an event without changing its final status

## Cron support
`BigBen` provides full support for scheduling of cron expressions in a 
distributed, fault tolerant manner. `BigBen` uses [cron-utils](https://github.com/jmrozanec/cron-utils) 
open source package to handle cron parsing and calculating execution times.  
Please see more details on various types of crons supported by the `cron-utils` package. 
The coverage is quite exhaustive (QUARTZ, UNIX, Cron4J, and Spring).

### How are crons executed?
`BigBen` uses `hazelcast` to create a lock-free distributed cron execution system.
`hazelcast` partitions its data in `271` partitions and it takes care of distributing 
these partitions equally among the cluster nodes. All the cron expressions are hashed to 
these partitions, which means crons get distributed across the cluster.

Each node then spawns a thread (pool) called `cron-runner` executes every second and 
checks which **local** crons are ready to execute. Note that there's no cross 
node communication or locking involved in executing these crons. 
Each cron requires a `tenant` which dictates how the cron is to be triggered 
(much like any other event in `BigBen`).

## cron execution guarantees
`BigBen` aims to guarantee that
1. As long as at least one node is available, the cron will execute
2. A cron will always be executed on one node only (If the node goes down then the 
subsequent executions will happen on another node)
3. Each cron trigger is tried four times (like other events in `BigBen`) 
(default is now, 1 second later, 2 seconds later, 4 seconds later)
4. If all tries result in failure (e.g. if tenant's http service is not 
responding or kafka cluster is down) and if cron log events support is enabled
 (see below) then the event is stored in log table with the (last) associated failure. 
 All intermediate failures are also logged in the configured `log4j` appenders as well.
5. The minimum execution interval supported is 1 second.      

## cron event log support
`BigBen` can optionally record each execution of cron trigger in a table called `cron-events`
(see `bigben-schema.cql` for table details).

The `cron-events` uses fully qualified cron-id (a combination of user
supplied cron-id, cron-type (e.g. QUARTZ, UNIX, etc), tenant) and a `logGranularity`
time unit as partition keys and the cron `executionTime` as the event time.
The table also stores what event is triggered at the trigger time. The `cron-events`
also supports log event retention as well.

E.g. you can set up a cron top execute every 1 minute and keep records 
grouped together at DAILY level with retention of 1 week for each event.

This support is optional. By default, the log events are turned off.

_the cron log events are stored with consistency ONE_. You can use a different 
consistency by providing `cron.log.events.write.consistency` in `bigben.yaml`
        
## cron APIs
`POST /cron`

Sets up a cron. (**_requires tenant to be set up first_**)

Sample request payload with event log enabled:
```json
{
	"tenant": "tenant1",
	"id": "cronId1",
	"expression": "*/5 * * * * ? *",
	"type": "QUARTZ",
	"logGranularity": "MINUTES",
	"retention": 2,
	"retentionUnits": "MINUTES"
}
```     
Sample request payload with event log disabled:
```json
{
	"tenant": "tenant1",
	"id": "cronId1",
	"expression": "*/5 * * * * ? *",
	"type": "QUARTZ"
}
```  
The response comes back with a `description` that tells how the cron 
will be executed.

Sample response payload:
```json
{
  "cron": {
    "id": "cronId1",
    "expression": "*/5 * * * * ? *",
    "type": "QUARTZ",
    "tenant": "tenant1",
    "logGranularity": "MINUTES",
    "retention": 2,
    "retentionUnits": "MINUTES"
  },
  "description": "every 5 seconds"
}
```      
That's it! This cron will execute every 5 seconds as long as any node in cluster 
is alive.

`GET /cron/{tenant}/{id}`

Returns all crons (if multiple types) identified by this tenant and 
cronId combination. 

`DELETE /cron/{tenant}/{id}/{type}`

Deletes a cron with specific combination. 

