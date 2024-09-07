# Message Broker

This message broker can

* distribute text-based messages between multiple clients
* via separate topics
* with tolerance for temporary connection interruptions to clients
* with load balancing on Consumer Groups
* with high availability with Leaderless Replication

.

## Feature Backlog

- [x] Topics
- [x] Consumer Groups
- [x] In-memory message queueing
    - After connecting, clients will receive messages of their subscriptions that arrived while the clients were
      disconnected
- [ ] Persistent queueing
    - During operation, the broker can be restarted and the processing continues without loss of messages
- [x] Cancel Subscriptions

## Quality Backlog

- [x] Introduce logging Library
- [x] Unit-Test ConsumerGroup.getNextClientProxyWithSocket
- [ ] HA via Leaderless Replication of Messages and Load Balancing
    - This drops strict ordering, because each node will construct its own message order
    - [x] Replication of all messages to all nodes
        - [x] Nodes know about each other via IP:PORT config parameters
        - [x] On startup, nodes send replication requests to each other
            - [ ] Consider migrating to a push-based instead of subscription-based model
        - [x] When a message is received from a consumer, the message will be forwarded to replication receivers
        - [x] The replication receiver feeds all messages into its own topics
        - [?] Delivered messages are deleted cluster-wide
    - [ ] Decoupling of replication and distribution 
    - [ ] Cluster organizes the division of labour automatically
      - No manual configuration by admin needed
      - [ ] Responsibilities are negotiated when a node becomes or unbecomes the role of a message distributor 
    - [ ] (On failure) clients connect to any node and message processing continues
        - [ ] Subscriptions have to be replicated
