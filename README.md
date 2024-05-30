# Message Broker
This message broker can
* distribute text-based messages between multiple clients
* via separate topics
.

Message _queueing_ is not implemented yet, so messages are only delivered to _connected_ clients.


## Feature Backlog
- [x] Introduce logging Library
- [x] multiple topics
- [x] cleanup on disconnect
- [x] 1:n message delivery
- [ ] in-memory message queueing
  - after connecting, clients will receive messages of their subscriptions that arrived while the clients were disconnected 
- [ ] persistent queueing
  - during operation, the broker can be restarted and the processing continues without loss of messages
- [ ] HA
  - clients can connect to any instance and perform any operation
