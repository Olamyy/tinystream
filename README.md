# TinyStream

TinyStream is a `lightweight` streaming engine in Python, inspired by Apache Kafka.

## Features

- Append-only Partitioned Log: The core storage mechanism.
- Segment-Based Storage: Logs are broken into segments with sparse .index files for fast, efficient reads.
- Pluggable Storage: Supports SingleLogStorage (one file) or SegmentedLogStorage (retention-ready).
- Controller Cluster: A controller-based architecture (no "single mode") manages cluster state.
- Metadata & Liveness: The Controller tracks broker liveness (via heartbeats) and partition assignments.
- Leader Election: The Controller automatically elects new leaders when brokers fail [cite: controller.py, _handle_broker_failure].
- Producer/Consumer APIs: Asynchronous clients for producing and consuming data.
- Log Retention: Supports per-topic log retention by time (retention_ms) or size (retention_bytes) [cite: topic_manager.py, create_topic].
- HTTP Admin Dashboard: A built-in, lightweight web UI (via FastAPI) to view cluster status.


## Configuration Management

TinyStream uses a 4-layer "override" system for configuration, managed by the `ConfigManager`.

The final value for any setting is chosen in this order of priority:

- CLI Arguments: (e.g., `--controller-uri`)
- Environment Variables: (e.g., `TINYSTREAM_CONTROLLER_URI`)
- User-Provided Config File: (e.g., `--config test_confs/broker-1.ini`)
- Default Component Config File: (e.g., `tinystream/config/controller.ini`)

This allows you to have a set of default configs and override them at runtime. For example, you can start a broker and tell it where the controller is via an environment variable, rather than modifying its config file.


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

#### Step 1: Start the Controller

The Controller manages cluster metadata (topics, brokers, partition leaders).

```bash
uv run python -m tinystream.controller
```

The controller will start:

- Its RPC Server on `localhost:9093` (for Brokers to connect).
- The Metastore API / Dashboard on http://localhost:3200.

#### Step 2: Start a Broker

The Broker stores data. This command will load ``tinystream/config/broker.ini`` (which knows the controller's address) and start the broker with ID 1.
```bash
uv run python -m tinystream.broker --broker-id 1
```

The broker will start:

- Its RPC Server on ``localhost:9095`` (from broker.ini, for clients).
- It will then connect to the Controller at ``localhost:9093`` to register itself.

#### Step 4: Create a Topic

Use the admin client to tell the Controller to create a new topic.

```bash
uv run python -m tinystream.admin create-topic \
    --topic "events" \
    --partitions 3 \
    --replication-factor 1 \
    --controller-uri "localhost:6000"
```


#### Step 5: Produce Messages

```python
import asyncio
import argparse
from tinystream.client.producer import Producer
from tinystream.config.manager import ConfigManager

async def run():
    args = argparse.Namespace(config=None, controller_uri=None, metastore_uri=None)
    config = ConfigManager(args, component_type="broker")

    producer = Producer(config=config)

    try:
        await producer.connect()
        print("Producer connected. Sending 10 messages...")

        for i in range(10):
            msg = f"hello-tinystream-{i}"
            key = f"user-{i % 2}"
            print(f"Sending: {msg} (key: {key})")

            response = await producer.send("events", msg.encode('utf-8'), key=key)
            print(f"-> Broker response: {response}")
            await asyncio.sleep(0.5)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        await producer.close()

if __name__ == "__main__":
    asyncio.run(run())
```

#### Step 6: Consume Messages

```python

import asyncio
import argparse
from tinystream.client.consumer import Consumer
from tinystream.config.manager import ConfigManager

async def run():
    args = argparse.Namespace(config=None, controller_uri=None, metastore_uri=None)
    config = ConfigManager(args, component_type="broker")

    consumer = Consumer(config=config, group_id="my-test-group")

    try:
        await consumer.connect()
        print("Consumer connected.")

        consumer.assign(topic="events", partition=0, start_offset=0)
        print("Consuming from 'events-0'. Press Ctrl+C to stop.")

        while True:
            messages = await consumer.poll(max_messages=5)
            if messages:
                for msg in messages:
                    print(f"Received: {msg.decode('utf-8')}")
                await consumer.commit()

            await asyncio.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        await consumer.close()

if __name__ == "__main__":
    asyncio.run(run())

```

## Load Testing

A load test script is included in `load_test.py`. It uses spawns worker tasks to send data in parallel.

Before running, ensure the topic exists:

```bash
uv run python -m tinystream.admin create-topic --topic "load_test" --partitions 3 --replication-factor 1
```

To run the test for 60 seconds with 50 concurrent workers:

```bash
uv run python load_test.py \
    --topic "load_test" \
    --num-workers 50 \
    --message-size 1024 \
    --run-time 60
```

## Load Test Results

TODO: Add results from a benchmark run (e.g., on an M4 Mac with 36GB memory) here.


#### Running Components in Isolation

For quick testing, each core component can be run in isolation directly as a module:

- Controller: `uv run python -m tinystream.controller`
- Broker: `uv run python -m tinystream.broker`
- Producer: `uv run python -m tinystream.client.producer`
- Consumer: `uv run python -m tinystream.client.consumer`
- Admin: `uv run python -m tinystream.client.admin`

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
