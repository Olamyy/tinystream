import asyncio
import hashlib
import random
import string
import argparse
from typing import Any, Optional, Dict

from tinystream.cluster_manager import ClusterManager
from tinystream.config.manager import ConfigManager
from tinystream.serializer.base import AbstractSerializer
from tinystream.utils.serlializer import init_serializer
from tinystream.utils.env import env_default


class Producer:
    def __init__(self, config: ConfigManager) -> None:
        self.config = config
        self.serializer_config = config.serialization

        self.serializer: AbstractSerializer = init_serializer(
            self.serializer_config.get("type", "messagepack")
        )

        self.cluster_manager = ClusterManager(
            config=config,
            serializer=self.serializer,
        )

    async def is_connected(self) -> bool:
        return await self.cluster_manager.is_connected()

    async def connect(self) -> None:
        """Explicitly connects to the controller."""
        print("[Producer]: Connecting...")
        await self.cluster_manager.connect()

    async def close(self) -> None:
        """Closes all active connections."""
        print("Closing producer connections...")
        await self.cluster_manager.close()

    @staticmethod
    def _get_partition_id(key: Optional[bytes], partition_count: int) -> int:
        """
        Determines the partition for a given key.
        """
        if key is None:
            return 0

        hash_bytes = hashlib.md5(key).digest()
        hash_int = int.from_bytes(hash_bytes, "little")

        return hash_int % partition_count

    async def _get_partition_count(self, topic: str) -> int:
        """Gets partition count for a topic from the cluster."""
        topic_partitions = await self.cluster_manager.get_topic_metadata(topic)
        return len(topic_partitions)

    async def send(
        self, topic: str, data: Any, key: Optional[str] = None, retries: int = 3
    ) -> Dict[str, Any]:
        """
        Sends a message to a topic.
        """
        key_bytes = key.encode("utf-8") if key else None

        partition_count = await self._get_partition_count(topic)
        partition_id = self._get_partition_id(key_bytes, partition_count)
        request = {
            "command": "append",
            "topic": topic,
            "partition": partition_id,
            "data": data,
        }

        return await self._send_cluster(request, retries)

    async def create_topic(
        self, topic: str, partition_count: int, replication_factor: int = 1
    ) -> Dict[str, Any]:
        """
        Creates a new topic in the cluster.
        """
        return await self.cluster_manager.create_topic(
            topic, partition_count, replication_factor
        )

    async def _send_cluster(
        self, request: Dict[str, Any], retries: int
    ) -> Dict[str, Any]:
        """Handles sending in cluster mode with leader discovery and retries."""
        topic = request["topic"]
        partition_id = request["partition"]

        for attempt in range(retries):
            connection = None
            try:
                connection = await self.cluster_manager.get_leader_connection(
                    topic, partition_id
                )

                response = await connection.send_request(request)

                if response.get("status") == "ok":
                    return response

                print(f"[Producer]: Broker error: {response}. Retrying...")
                await self.cluster_manager.invalidate_caches(connection)

            except (ConnectionError, asyncio.TimeoutError) as e:
                print(f"[Producer]: Connection error: {e}. Retrying...")
                await self.cluster_manager.invalidate_caches(connection)

            except Exception as e:
                print(f"[Producer]: Unexpected error: {e}")
                raise e

            await asyncio.sleep(0.5 * (attempt + 1))

        raise ConnectionError(
            f"Failed to send message to topic '{topic}' after {retries} retries."
        )


async def main(config: ConfigManager) -> None:
    producer = Producer(config=config)

    def _random_payload(size_bytes: int = 5_000) -> str:
        return "".join(
            random.choices(string.ascii_letters + string.digits, k=size_bytes)
        )

    dummy_events = [
        {
            "user": f"user_{i}",
            "action": "clicks",
            "item": "item_A",
            # "payload": _random_payload(),
        }
        for i in range(1)
    ]

    try:
        await producer.connect()
        print("Producer connected in cluster mode. Sending... (Press Ctrl+C to stop)")

        message_count = 0
        while True:
            event = dummy_events[message_count % len(dummy_events)]
            event["message_id"] = str(message_count)

            print(f"Sending: {event['message_id']}")
            response = await producer.send(
                topic=event["action"], data=event, key=event["user"]
            )
            print(f"Broker response: {response}")

            message_count += 1
            await asyncio.sleep(1)
    except ConnectionRefusedError:
        print("\n[ERROR] Could not connect to controller.")
    except KeyboardInterrupt:
        print("\n\nStopping producer... (Ctrl+C pressed)")
    except Exception as e:
        print(f"\nAn error occurred: {e}")
    finally:
        if await producer.is_connected():
            await producer.close()
            print("Producer connection closed.")
        else:
            print("Producer was not connected.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TinyStream Producer")

    parser.add_argument(
        "--controller-uri",
        type=str,
        default=env_default("TINYSTREAM_CONTROLLER_URI")(),
        help="Controller RPC URI (e.g., localhost:9093). Overrides config.",
    )
    parser.add_argument(
        "--metastore-uri",
        type=str,
        default=env_default("TINYSTREAM_METASTORE_URI")(),
        help="Metastore HTTP API URI. Overrides config.",
    )

    parser.add_argument(
        "--config",
        type=str,
        default=env_default("TINYSTREAM_CONFIG")(),
        help="Path to a user config file. Overrides default config.",
    )

    args = parser.parse_args()

    config_manager = ConfigManager(args, component_type="broker")

    asyncio.run(main(config=config_manager))
