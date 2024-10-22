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
      - Config v1 (links are uni directional, every node has to know every node via config params)
          - [x] Nodes know about each other via IP:PORT config parameters
            - [x] On startup, nodes send replication requests to each other
              - [ ] Consider migrating to a push-based instead of subscription-based model
      - Config v2 (links are bidirectional, a joining node has to know only a single cluster node, can connect anytime)
        - [ ] Topology changes during runtime
          - [ ] Nodes can join the cluster by connecting to any cluster node
            - [ ] it gets a list of all known nodes and establishes replication links
            - [ ] concurrency control
      - Distribution
          - [x] When a consumer group receives a message, it will be forwarded to replication receivers
          - [x] The replication receiver feeds all messages into its own replicated consumer groups
          - [x] Delivered messages are deleted cluster-wide
    - [ ] Nodes without clients continue receiving replicated messages (for HA) but delegate their message distribution
      responsibilities to other nodes. (Otherwise these messages would get stuck in the node)
    - [ ] Cluster organizes the division of labour automatically
      - No manual configuration by admin needed
      - [ ] Topology changes for the distribution cluster
        - [ ] A node gets a first client of a consumer group
        - [ ] A node looses its last client of a consumer group
        - [ ] A new node joins the cluster
        - [ ] A node leaves the cluster
    - [ ] (On failure) clients connect to any node and message processing continues
        - [x] Subscription requests (consumer group to topic) have to be replicated
        - [ ] Subscription requests (client to consumer group) have to be replicated for recognition on connect
        - [ ] Unsubscribe requests (client to consumer group) have to be replicated

## Lessons Learned
* Networking and concurrency increase the complexity (yes, we all knew this before, but I _felt_ it during development :-))
* My development approach "High-Level Unit Testing with TDD" is great for the cases that which you (can) think up upfront.
    * However, there are many cases which are not the leading cases for the happy path, but which have to be covered aswell
* Changing cross-cutting concerns or redistributing responsibilities across classes **on demand** (the opposite strategy of designing them upfront) is possible
  * if you're the single author of a codebase. If multiple persons are working on the same codebase in parallel, things might work differently
* Interfaces on the level of the programming language can be expressed good enough with types and code comments.
  * For replication, network communication gets involved... How and where should this interface be documented?
