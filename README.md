# Message Broker
This message broker can
* distribute text-based messages between multiple clients
* via separate topics
* with tolerance for temporary connection interruptions to clients
* with load balancing on consumer groups
.

## Feature Backlog
- [x] Multiple topics
- [x] Cleanup on disconnect
- [x] 1:n message delivery
- [x] In-memory message queueing
  - After connecting, clients will receive messages of their subscriptions that arrived while the clients were disconnected 
- [ ] Persistent queueing
  - During operation, the broker can be restarted and the processing continues without loss of messages
- [x] For load-balancing on the consumer side, clients can be grouped into consumer groups
- [x] Cancel subscriptions 

## Quality Backlog
- [x] Introduce logging Library
- [x] Unit-Test ConsumerGroup.getNextClientProxyWithSocket
- [ ] High Availability with Active Redundancy
  - [ ] Failures of single instances do not block the overall operation
    - [ ] On failure, clients connect to any instance and continue
      - [ ] State (Queues + Subscriptions) have to be replicated
