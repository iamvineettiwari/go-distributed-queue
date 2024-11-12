### Introduction

In this project, I aimed to explore and understand distributed systems by building a distributed queue. The system is designed to manage topics, where each topic’s partition is distributed across multiple servers. To ensure coordination and consistency across these servers, I used etcd as a key component. Client - Server communication is TCP base RPC calls.


**TODOS**
- [ ] Implement replication
- [ ] Implement replica leader election
- [ ] Implement WAL
- [ ] Implement offset management at client
- [ ] Improve client with more configurations 
- [ ] Refactor Server and Partitions for modularity
- [ ] Improve test coverage

### Start Command

This command will fetch docker image of etcd, run it locally on port `PORT_2379` and `PORT_2380`, then build the client and server and bind server to etcd server.

```
make start NO_SERVER=3 SERVER_PORTS="8000 8001 8002"
```

### Stop Command
```
make stop
```

### Client as producer
```
cd build/ && ./client -mode=producer
```

### Client as consumer
```
cd build/ && ./client -mode=consumer
```
