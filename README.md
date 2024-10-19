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
        - [x] Delivered messages are deleted cluster-wide
    - [ ] Nodes without clients continue receiving replicated messages (for HA) but delegate their message distribution
      responsibilities to other nodes. (Otherwise these messages would get stuck in the node)
    - [ ] Cluster organizes the division of labour automatically
      - No manual configuration by admin needed
      - [ ] Responsibilities are negotiated when a node becomes or unbecomes the role of a message distributor 
    - [x] (On failure) clients connect to any node and message processing continues
        - [x] Subscription requests have to be replicated
          - [ ] fail-safe (retries on network failure)
        - [ ] Unsubscribe requests have to be replicated

## Lessons Learned
* Networking and concurrency increase the complexity (yes, we all knew this before, but I _felt_ it during development :-))
* My development approach "High-Level Unit Testing with TDD" is great for the cases that which you (can) think up upfront.
    * However, there are many cases which are not the leading cases for the happy path, but which have to be covered aswell
* Changing cross-cutting concerns or redistributing responsibilities across classes **on demand** (the opposite strategy of designing them upfront) is possible
  * if you're the single author of a codebase. If multiple persons are working on the same codebase in parallel, things might work differently
* Interfaces on the level of the programming language can be expressed good enough with types and code comments.
  * For replication, network communication gets involved... How and where should this interface be documented?