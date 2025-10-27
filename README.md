# TinyStream

TinyStream is a lightweight streaming system in Python, inspired by Apache Kafka.
It’s designed to demonstrate the internal mechanics of a modern event streaming platform: append-only logs, partitioned storage, replication, and service discovery — all in a small, readable codebase.

## Design Philosophy:

**This is not a production system.** It is a "glass box" designed to reproduce the core concepts from systems like Kafka in a minimal, hackable codebase.

It exists to help answer questions like:
* How does Kafka’s storage engine work internally?
* How does distributed log replication operate?
* How do producers and consumers interact via offsets?
* How do components discover each other and handle leader election?

It aims to provide readable code that models the essence of distributed stream storage.

## Features

* Append-only partitioned log storage
* Durable, segment-based storage on disk
* Producer / Consumer client APIs
* Controller node for cluster metadata and leader election
* Broker cluster simulation
* Local cluster harness for distributed testing
* **[WIP]** Replication and leader election
* **[WIP]** Retention policies for segment cleanup

## Installation

The project uses `uv` for dependency management and execution.

1.  Clone the repository:
    ```bash
    git clone [https://github.com/Olamyy/tinystream](https://github.com/Olamyy/tinystream)
    cd tinystream
    ```
2.  Install the required dependencies:
    ```bash
    uv install
    ```

## Quick Start: Running Locally

Here is how to run a minimal "cluster" (one controller, one broker) on your machine.

### Step 1: Start the Controller

The Controller manages cluster metadata (topics, brokers, partition leaders).

```bash
uv run python -m tinystream.controller --port 6000
```

### Step 2: Start a Broker

The Broker stores data. It registers itself with the Controller.
```bash
uv run python -m tinystream.broker --mode cluster --broker-number=2
```

### Step 4: Create a Topic

Topics must be created before you can produce to them. Use the admin client to ask the Controller to create a topic.

```bash
uv run python -m tinystream.admin create-topic \
    --topic "events" \
    --partitions 3 \
    --replication-factor 1 \
    --controller "localhost:6000"
```


### Step 5: Produce Messages

```python
from tinystream.client.producer import Producer
from tinystream.config.parser import TinyStreamConfig

config = TinyStreamConfig.from_default_config_file()
config.mode = "cluster"
producer = Producer(config=config)

print("Sending 10 messages...")

for i in range(10):
    msg = f"hello-tinystream-{i}".encode('utf-8')
    producer.send("events", msg)
    print(f"Sent: {msg.decode()}")

print("Done.")
```

### Step 6: Consume Messages

```python

from tinystream.client.consumer import Consumer
from tinystream.config.parser import TinyStreamConfig

config = TinyStreamConfig.from_default_config_file()

config.mode = "cluster"
consumer = Consumer(config=config, group_id="test-group")

consumer.connect()

consumer.assign(topic="events", partition=0, start_offset=0)

for _message in consumer.poll():
    print(f"Received: {_message.value.decode()}")

```

### Running Components in Isolation

For quick testing, each core component can be run in isolation directly as a module:

- Controller: `uv run python -m tinystream.controller`
- Broker: `uv run python -m tinystream.broker`
- Producer: `uv run python -m tinystream.client.producer`
- Consumer: `uv run python -m tinystream.client.consumer`

## Architecture Overview

TinyStream is split into five layers:

```
┌────────────────────────────────────────────┐
│          Producers / Consumers             │
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
│  • Manages append-only log segments        |
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


## What to Test

| Category        | Example                                              |
| --------------- | ---------------------------------------------------- |
| Replication     | Write to leader → restart follower → verify catch-up |
| Leader Election | Kill leader → ensure controller reassigns            |
| Retention       | Configure short TTL → check old segment deletion     |
| Consistency     | Compare offsets after recovery                       |
| Consumer Groups | Add/remove consumers → verify rebalancing
