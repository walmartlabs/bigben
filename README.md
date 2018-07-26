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
* **http** - HTTP APIs to send event requests and and HTTP APIs to receive event triggers.

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

### What constitutes an event processing failure?
Multiple scenarios can cause failures. Some of them are:
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

## Shard
![shard design](https://cdn-images-1.medium.com/max/1600/1*euaHLOnw6G96SigfXxWhtA.png "BigBen processing flow")

            
    
    
