# Rough Sketch

### Queue Configurations

1. maxPollRecord (default 100) : how many messages to return, when poll is done from the consumer
2. maxPollBytes (default 50MB) : how many bytes to return, keep no of records to respect this.


### Topic configuration

1. name (required) : Name of queue topic
2. noOfPartition (default 1) : No of partition in topic

### Partition configuration

1. id : ID of partition
2. consumerOffset : Track the commited offset of consumer
for this partition

### How data will be stored ?

1. Each partition will have its own append only file (partitionName).db 
2. Once a message is recieved on a topic, if partition id is not defined, round robin is used for message storage, else message is stored in the defined partition number.
3. Before appending the message, we assign the offset of message in current partition

- strucutre
    ```
    {
        topic: "",
        partition: "",
        timestamp: "",
        key: "",
        value: "",
        offset: "",
    }
    ```

### TODOS 

- [ ] Each topic will have partition attached to id (treat both as one)
- [ ] Each server share topic-partition 
- [ ] Each server knows about topic-partition address
