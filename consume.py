import asyncio
from tinystream.client import Consumer

TOPIC = "clicks"
PARTITION = 0
START_OFFSET = 0


async def main():
    print("Initializing consumer...")
    consumer = Consumer(host="localhost", port=9092)

    try:
        await consumer.connect()
        print("Consumer connected.")

        consumer.assign(TOPIC, partition=PARTITION, start_offset=START_OFFSET)
        print(
            f"Assigned to {TOPIC}-{PARTITION} at offset {START_OFFSET}. Polling... (Press Ctrl+C to stop)"
        )

        while True:
            batch = await consumer.poll(max_messages=10)

            if batch:
                print("--- Received batch ---")
                for msg in batch:
                    print(f"Received: {msg}")
                print("----------------------")
            else:
                await asyncio.sleep(1)

    except ConnectionRefusedError:
        print("\n[ERROR] Could not connect to broker.")
        print("Please ensure the broker is running in another terminal:")
        print("  python -m tinystream.broker")

    except KeyboardInterrupt:
        # --- Handle Ctrl+C gracefully ---
        print("\n\nStopping consumer... (Ctrl+C pressed)")

    except Exception as e:
        print(f"\nAn error occurred: {e}")

    finally:
        # --- Ensure connection is always closed ---
        if consumer._connection.is_connected:
            await consumer.close()
            print("Consumer connection closed.")
        else:
            print("Consumer was not connected.")


if __name__ == "__main__":
    asyncio.run(main())
