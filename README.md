# Message Broker
For better understanding how message brokers work, I'm building my own one 🦫.


## Feature Backlog
- [x] Introduce Logging Library
- [ ] in-memory message queueing
- [ ] persistent queueing
- [x] multiple topics
- [x] cleanup on disconnect
- [x] 1:n message delivery
- [ ] HA (but connect to any instance)
  - incoming messages have to be distributed to all nodes
  - and the nodes then distribute the messages to their local subscriptions
