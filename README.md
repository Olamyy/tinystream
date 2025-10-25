# TinyStream

TinyStream is a lightweight in Python. It’s designed to demonstrate the internal mechanics of modern event streaming:
append-only logs, partitioned storage, replication, and streaming processing — all in a small, readable codebase.

## Features

* Append-only partitioned log storage
* Durable, segment-based storage on disk
* Producer / Consumer APIs (Kafka-style)
* Controller node for broker and topic metadata
* Broker cluster simulation (multi-node)
* Replication and leader election (WIP)
* Retention policies for segment cleanup
* Local cluster harness for distributed testing

## Architecture Overview

TinyStream is split into five layers:

```
┌────────────────────────────────────────────┐
│        Producers / Consumers               │
│  • Send and fetch records from topics      │
│  • Commit offsets                          │
└────────────────────────────────────────────┘
                 │
                 ▼
┌────────────────────────────────────────────┐
│                 Broker                     │
│  • Accepts produce/fetch requests          │
│  • Manages topic partitions                │
│  • Serves leader/follower replicas         │
└────────────────────────────────────────────┘
                 │
                 ▼
┌────────────────────────────────────────────┐
│                Partition                   │
│  • Manages append-only log segments        │
│  • Handles retention and compaction        │
│  • Stores offset and time indexes          │
└────────────────────────────────────────────┘
                 │
                 ▼
┌────────────────────────────────────────────┐
│                Segment                     │
│  • File-based, append-only structure       │
│  • Supports batch reads via index lookups  │
└────────────────────────────────────────────┘
                 │
                 ▼
┌────────────────────────────────────────────┐
│                Storage                     │
│  • Local disk or tiered storage backend    │
│  • Retention + compaction policy engine    │
└────────────────────────────────────────────┘
```

Each topic is split into partitions, and each partition is an append-only log.
Brokers host one or more partitions; producers and consumers talk to brokers via lightweight RPC.

## Partition Storage Layout

Each partition is stored on disk as a directory containing segment and index files:

```
data/
└── topics/
    └── user_clicks/
        └── partition-0/
            ├── 00000000000000000000.log
            ├── 00000000000000000000.index
            ├── 00000000000001000000.log
            ├── 00000000000001000000.index
            ├── partition.metadata
            └── lock
```

Messages are never deleted after consumption — instead, TinyStream enforces a retention policy (by time or size) to delete or compact old segments.

## Local Cluster Simulation

You can run a multi-broker cluster locally to test replication and leader election:

```
localhost
└── TinyStream Cluster
    ├── broker-1 (port 5001)
    ├── broker-2 (port 5002)
    ├── broker-3 (port 5003)
    └── controller (port 6000)
```

Each broker stores its own data in a separate directory (e.g., `/tmp/tinystream/b1`).

## Example Usage

### Start a local controller and brokers

```bash
python -m tinystream.controller --port 6000
python -m tinystream.broker --id 1 --port 5001 --controller localhost:6000
python -m tinystream.broker --id 2 --port 5002 --controller localhost:6000
```

### Produce messages

```python
from tinystream.client import Producer

producer = Producer(controller="localhost:6000")
for i in range(10):
    producer.send("events", f"msg-{i}".encode())
```

### Consume messages

```python
from tinystream.client import Consumer

consumer = Consumer(controller="localhost:6000")
for msg in consumer.read("events"):
    print(msg.value)
```

## Local Cluster Testing

TinyStream includes a built-in harness for testing multi-broker setups:

```python
from tinystream.testing import LocalCluster

def test_cluster_replication():
    cluster = LocalCluster(brokers=3)
    cluster.start()

    p = cluster.new_producer()
    c = cluster.new_consumer()

    for i in range(100):
        p.send("demo", f"event-{i}".encode())

    msgs = c.read("demo", from_beginning=True)
    assert len(msgs) == 100

    cluster.stop()
```

## What to Test

| Category        | Example                                              |
| --------------- | ---------------------------------------------------- |
| Replication     | Write to leader → restart follower → verify catch-up |
| Leader Election | Kill leader → ensure controller reassigns            |
| Retention       | Configure short TTL → check old segment deletion     |
| Consistency     | Compare offsets after recovery                       |
| Consumer Groups | Add/remove consumers → verify rebalancing            |

## Roadmap

* [ ] Controller-based leader election
* [ ] Replication between brokers
* [ ] Topic compaction policies
* [ ] Async I/O for log reads
* [ ] REST/gRPC API layer
* [ ] Stream processing DSL (TinyFlink)

## Design Philosophy

TinyStream is not a production system. It's primarily to reproduce core concepts from event streaming systems in a minimal codebase.

* How Kafka’s storage engine works internally
* How distributed log replication operates
* How producers and consumers interact via offsets
* How checkpointing, retention, and recovery behave in real streaming systems

It aims to provide readable, hackable code that models the essence of stream storage.
