# Message Broker
This message broker can
* distribute text-based messages between multiple clients
* via separate topics
* with tolerance for temporary connection interruptions to clients
* with load balancing on consumer groups
.

## Feature Backlog
- [x] Topics
- [x] Consumer Groups
- [x] In-memory message queueing
  - After connecting, clients will receive messages of their subscriptions that arrived while the clients were disconnected 
- [ ] Persistent queueing
  - During operation, the broker can be restarted and the processing continues without loss of messages
- [x] Cancel Subscriptions 

## Quality Backlog
- [x] Introduce logging Library
- [x] Unit-Test ConsumerGroup.getNextClientProxyWithSocket
- [ ] Asynchronous message replication for higher throughput of message ingestion 
  - No majority quorum, because strong consistency is not required
- [ ] Leaderless replication
    - This drops strict ordering, because each node will construct its own message order
- [ ] High Availability with Active Redundancy
  - [ ] Failures of single node do not block the overall operation
    - [ ] On failure, clients connect to any node and continue
      - [ ] State (Queues + Subscriptions) have to be replicated
