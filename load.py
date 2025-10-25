import asyncio
import time
import uuid
from typing import List

from client.producer import Producer

NUM_PRODUCERS = 50
MESSAGES_PER_PRODUCER = 5000000
TOPIC = "load_test_topic"
TOTAL_MESSAGES = NUM_PRODUCERS * MESSAGES_PER_PRODUCER


async def producer_worker(worker_id: int):
    """
    A single producer task that sends a batch of messages.
    """
    print(f"[Worker {worker_id}] Starting...")
    producer = Producer()
    await producer.connect()

    messages_sent = 0
    for i in range(MESSAGES_PER_PRODUCER):
        msg = {
            "worker_id": worker_id,
            "message_id": i,
            "payload": str(uuid.uuid4())  # A semi-realistic payload
        }
        try:
            await producer.send(TOPIC, msg, key=str(worker_id))
            messages_sent += 1
        except Exception as e:
            print(f"[Worker {worker_id}] Error sending message: {e}")
            break  # Stop this worker on error

    await producer.close()
    print(f"[Worker {worker_id}] Finished. Sent {messages_sent} messages.")
    return messages_sent


async def main():
    print("--- Starting TinyStream Load Test ---")
    print(f"Configuration:")
    print(f"  Concurrent Producers: {NUM_PRODUCERS}")
    print(f"  Messages per Producer: {MESSAGES_PER_PRODUCER}")
    print(f"  Total Messages: {TOTAL_MESSAGES}")
    print("---------------------------------------")

    try:
        p = Producer()
        await p.connect()
        await p.close()
        print("Broker connection test successful.")
    except ConnectionRefusedError:
        print("\n[FATAL ERROR] Could not connect to broker.")
        print("Please ensure the broker is running in another terminal.")
        return
    except Exception as e:
        print(f"Broker connection test failed: {e}")
        return

    start_time = time.monotonic()

    tasks = [
        producer_worker(i)
        for i in range(NUM_PRODUCERS)
    ]

    results: List[int] = await asyncio.gather(*tasks)

    end_time = time.monotonic()

    total_time = end_time - start_time
    total_sent = sum(results)
    messages_per_second = total_sent / total_time

    print("\n--- Load Test Results ---")
    print(f"Total messages sent: {total_sent} / {TOTAL_MESSAGES}")
    print(f"Total time taken: {total_time:.2f} seconds")
    print(f"Ingestion Throughput: {messages_per_second:,.2f} messages/sec")
    print("-------------------------")


if __name__ == "__main__":
    print("NOTE: Clear your log directory (e.g., ./data/tinystream_logs) for a clean run.")
    asyncio.run(main())