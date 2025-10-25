import asyncio
import random
import uuid

from tinystream.client.producer import Producer


async def main():
    print("Initializing producer...")
    producer = Producer(broker_host="localhost", broker_port=9092)

    # --- Data for generating random messages ---
    users = ["alice", "bob", "carlos", "denise"]
    actions = ["click", "view", "purchase", "scroll"]
    items = ["item_A", "item_B", "page_X", "button_Y"]
    # ---

    try:
        await producer.connect()
        print("Producer connected. Sending messages... (Press Ctrl+C to stop)")

        message_count = 0
        # --- Infinite loop ---
        while True:
            user = random.choice(users)
            msg = {
                "user": user,
                "action": random.choice(actions),
                "item": random.choice(items),
                "message_id": str(uuid.uuid4()),
                "count": message_count,
            }

            # 2. Send the message
            print(f"Sending: {msg}")
            response = await producer.send(topic="clicks", data=msg, key=user)
            print(f"Broker response: {response}")

            message_count += 1

            await asyncio.sleep(1)

    except ConnectionRefusedError:
        print("\n[ERROR] Could not connect to broker.")
        print("Please ensure the broker is running in another terminal:")
        print("  python -m tinystream.broker")

    except KeyboardInterrupt:
        print("\n\nStopping producer... (Ctrl+C pressed)")

    except Exception as e:
        print(f"\nAn error occurred: {e}")

    finally:
        if producer._connection.is_connected:
            await producer.close()
            print("Producer connection closed.")
        else:
            print("Producer was not connected.")


if __name__ == "__main__":
    asyncio.run(main())
