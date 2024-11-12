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

