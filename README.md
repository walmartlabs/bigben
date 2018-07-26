# BigBen
`BigBen` is a generic, multi-tenant, time-based event scheduler and cron 
scheduling framework based on `Cassandra` and `Hazelcast` is designed to be:
* **Distributed** - `BigBen` uses a distributed design and can be deployed on 10's or 100's of machines and can be dc-local or cross-dc
* **Horizontally scalable** -  Need more throughput? Just add more machines without any downtime.
* **Fault tolerant** - `BigBen` employs a number of failure protection modes and 
can withstand arbitrary prolonged down times
* **Performant** - `BigBen` can easily scale to 10,000's or even millions's of event triggers with a 
very small cluster of machines. It can also easily manage million's of crons running in a distributed manner
* **Highly Available** - As long as a single machine is available in the cluster, `BigBen` will guarantee the 
execution of events (albeit with a lower throughput)
* **Extremely consistent** - `BigBen` employs a single master design (the master itself is highly available with 
`n-1` masters on standby in an `n` cluster machine) to ensure that no two nodes fire the same event or execute 
the same cron.  
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

## Design and architecture
See the blog published at [Medium](https://medium.com/walmartlabs/an-approach-to-designing-distributed-fault-tolerant-horizontally-scalable-event-scheduler-278c9c380637)
for a full description of various design elements of `BigBen`

## Events Inflow
`BigBen` can receive events in two modes:
* **kafka** - inbound and outbound Kafka topics to consume event requests and publish event triggers
* **http** - HTTP APIs to send event requests and and HTTP APIs to receive event triggers.

*It is strongly recommended to use `kafka` for better scalability*

![inflow](/docs/assets/inflow.png "Events Inflow diagram")

## Shard
![shard design](https://cdn-images-1.medium.com/max/1600/1*euaHLOnw6G96SigfXxWhtA.png "BigBen processing flow")

            
    
    
