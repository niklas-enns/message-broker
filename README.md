# Message Broker
For better understanding how message brokers work, I'm building my own one 🦫.


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
