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
- [ ] HA via Leaderless Replication of Messages and Load Balancing
    - This drops strict ordering, because each node will construct its own message order
    - [ ] Replication of all messages to all nodes
      - [ ] On startup, nodes send replication requests to each other
      - [ ] When a message is received from a consumer, the message will be forwarded to replication receivers
      - [ ] The replication receiver will feed all messages into its own queues
    - [ ] (On failure) clients connect to any node and message processing continues
      - [ ] Subscriptions have to be replicated