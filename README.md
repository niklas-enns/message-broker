# Message Broker
This message broker can
* distribute text-based messages between multiple clients
* via separate topics
.

Message _queueing_ is not implemented yet, so messages are only delivered to _connected_ clients.


## Feature Backlog
- [x] Introduce logging Library
- [x] Multiple topics
- [x] Cleanup on disconnect
- [x] 1:n message delivery
- [x] In-memory message queueing
  - After connecting, clients will receive messages of their subscriptions that arrived while the clients were disconnected 
- [ ] Persistent queueing
  - During operation, the broker can be restarted and the processing continues without loss of messages
- [ ] HA
  - Clients can connect to any instance and perform any operation
