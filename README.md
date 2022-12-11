# Miruvor

This repository contains an implementation of a distributed key-value store in Elixir using the RocksDB key-value store and the Raft consensus protocol. It is implemented using the GenStateMachine and Phoenix Framework libraries.

## Features

- Distributed key-value store with support for multiple nodes
- Uses the Raft consensus protocol to ensure data consistency and availability
- Implements the GenStateMachine library for managing the state of the key-value store
- Built using the Phoenix Framework for fast and scalable performance

## Planned Features

- Shards
- Deployment to Cloud

## Design Choices

In designing this distributed key-value store, several important choices were made with the goal of achieving high performance and reliability.

One key decision was to use the RocksDB key-value store as the underlying storage engine. RocksDB is a high-performance, embedded database engine that is widely used in distributed systems and is known for its fast read and write speeds. This makes it well-suited for use in a distributed key-value store, where high throughput and low latency are critical.

Another important design choice was to use the Raft consensus protocol for managing the distributed key-value store. Raft is a well-known and widely-used consensus algorithm that ensures data consistency and availability across multiple nodes. This makes it ideal for use in a distributed key-value store, where data must be synchronized and consistent across all nodes. The goal was to implement strong consistency, however upon implementation there were significant hurdles. Raft provides a weaker form of consistency known as linearizability out-of-the-box, which guarantees that the system will appear to be in a consistent state from the perspective of any individual node. However, it does not guarantee that all nodes will always have the same data at the same time, which is what strong consistency guarantees. To implement strong, consistency, generally Paxos is used. There are other methods such as using a strongly consistent data-store or adding syncronization mechanisms on top of the the consensus layer. These methods required significant development effor beyond the scope of this project.

To further improve performance and reliability, it was decided to implement the key-value store using the GenStateMachine and Phoenix Framework libraries. GenStateMachine is a powerful Elixir library that allows developers to easily implement state machines, which are useful for managing the state of complex systems like a distributed key-value store. Phoenix Framework is a high-performance web framework that provides a fast and scalable platform for building web applications in Elixir.

Finally, the decision was made to keep the logs for the key-value store in memory for increased performance. This allows the system to quickly access and update the logs without having to read from and write to disk, which can be slower and more resource-intensive. This design choice ensures that the key-value store can operate at high speeds and handle large volumes of data without sacrificing performance.


## Results

### 100 Requests

Total time: 2.8973729610443115
Average time: 0.028973729610443116
Throughput: 34.51402402953212

### 500 Requests

Total time: 16.166029691696167
Average time: 0.032332059383392335
Throughput: 30.929053672147447

### 1000 Requests

Total time: 32.564454793930054
Average time: 0.03256445479393005
Throughput: 30.70832926047937
