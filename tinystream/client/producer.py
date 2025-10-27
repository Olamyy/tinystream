import asyncio
import hashlib
from typing import Any, Optional, Dict, Tuple

from tinystream import DEFAULT_CONFIG_PATH
from tinystream.client.connection import TinyStreamAPI
from tinystream.cluster_manager import ClusterManager
from tinystream.config.parser import TinyStreamConfig
from tinystream.serializer.base import AbstractSerializer
from tinystream.utils.serlializer import init_serializer


class Producer:
    """
    Manages connections and provides a `send` method.
    Supports two modes:
    1.  "single": Connects directly to a single broker.
    2.  "cluster": Connects to a controller to discover leader brokers.
    """

    def __init__(self, config: TinyStreamConfig) -> None:
        self.config = config
        self.mode = config.mode

        self.serializer_config = config.get_serialization_config()

        self.serializer: AbstractSerializer = init_serializer(
            self.serializer_config.get("type", "messagepack")
        )

        if self.mode == "cluster":
            controller_config = config.get_controller_config()
            self.controller_host = controller_config.get("host")
            self.controller_port = controller_config.get("port")
            self._controller_connection: Optional[TinyStreamAPI] = TinyStreamAPI(
                self.controller_host,  # type: ignore
                self.controller_port,  # type: ignore
                serializer=self.serializer,  # type: ignore
            )

            self._topic_metadata_cache: Dict[str, Dict[int, Any]] = {}
            self._broker_info_cache: Dict[int, Any] = {}
            self._broker_connections: Dict[Tuple[str, int], TinyStreamAPI] = {}
            self._metadata_lock = asyncio.Lock()
            self.cluster_manager = ClusterManager(
                self.mode,
                self._topic_metadata_cache,
                self._broker_info_cache,
                self._broker_connections,
                self.serializer,
            )

        else:
            self.mode = "single"
            broker_config = config.get_broker_config()
            broker_host = broker_config.get("host")
            broker_port = broker_config.get("port")
            self._single_broker_connection = TinyStreamAPI(
                broker_host,  # type: ignore
                broker_port,  # type: ignore
                serializer=self.serializer,  # type: ignore
            )
            self._default_partition_count = 1

    async def is_connected(self) -> bool:
        if self.mode == "cluster":
            if (
                not self._controller_connection
                or not self._controller_connection.is_connected
            ):
                return False
            for conn in self._broker_connections.values():
                if not conn.is_connected:
                    return False
            return True
        else:
            return self._single_broker_connection.is_connected

    async def connect(self) -> None:
        """Explicitly connects to the controller or the single broker."""
        if self.mode == "cluster":
            print("[Producer] (Cluster Mode): Connecting to controller...")
            await self._controller_connection.ensure_connected()  # type: ignore
            await self._refresh_cluster_metadata()

        elif self.mode == "single":
            print("[Producer] (Single Mode): Connecting to broker...")
            await self._single_broker_connection.ensure_connected()

    async def close(self) -> None:
        """Closes all active connections."""
        print("Closing producer connections...")
        if self.mode == "cluster":
            if self._controller_connection:
                await self._controller_connection.close()
            for conn in self._broker_connections.values():
                await conn.close()
            self._broker_connections.clear()
            self._topic_metadata_cache.clear()
            self._broker_info_cache.clear()

        elif self.mode == "single":
            if self._single_broker_connection:
                await self._single_broker_connection.close()

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
        """Gets partition count for a topic based on mode."""
        if self.mode == "cluster":
            if topic not in self._topic_metadata_cache:
                await self._refresh_cluster_metadata()

            topic_partitions = self._topic_metadata_cache.get(topic)
            if not topic_partitions:
                raise ValueError(f"Topic '{topic}' not found in cluster metadata.")
            return len(topic_partitions)
        else:
            return self._default_partition_count

    async def send(
        self, topic: str, data: Any, key: Optional[str] = None, retries: int = 3
    ) -> Dict[str, Any]:
        """
        Sends a message to a topic.
        Behavior depends on the producer's mode (single vs. cluster).
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

        if self.mode == "cluster":
            return await self._send_cluster(request, retries)
        else:
            return await self._send_single(request)

    async def _send_single(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Handles sending in single-broker mode."""
        try:
            await self._single_broker_connection.ensure_connected()
            response = await self._single_broker_connection.send_request(request)
            return response
        except (ConnectionError, asyncio.TimeoutError) as e:
            print(f"[Producer] (Single Mode): Connection error: {e}")
            raise e
        except Exception as e:
            print(f"[Producer] (Single Mode): Unexpected error: {e}")
            raise e

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

                print(
                    f"[Producer]: Broker error: {response.get('message')}. Retrying..."
                )
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

    async def _refresh_cluster_metadata(self) -> None:
        """Fetches the latest cluster state from the controller."""
        if self.mode != "cluster":
            return

        async with self._metadata_lock:
            try:
                await self._controller_connection.ensure_connected()  # type: ignore
                response = await self._controller_connection.send_request(  # type: ignore
                    {"command": "get_cluster_metadata"}
                )

                if response.get("status") == "ok":
                    metadata = response["metadata"]
                    self._broker_info_cache = metadata.get("brokers", {})
                    self._topic_metadata_cache = metadata.get("partitions", {})
                    print("[Producer]: Metadata refreshed.")
                else:
                    print(
                        f"[Producer]: Failed to refresh metadata: {response.get('message')}"
                    )
            except Exception as e:
                print(f"[Producer]: Error refreshing metadata: {e}")


async def main(
    config: Optional[str] = DEFAULT_CONFIG_PATH, mode: str = "single"
) -> None:
    config = TinyStreamConfig.from_ini(config or DEFAULT_CONFIG_PATH)  # type: ignore
    config.mode = mode  # type: ignore
    producer = Producer(config=config)  # type: ignore

    dummy_events = [
        {"user": "alice", "action": "click", "item": "item_A"},
        {"user": "bob", "action": "view", "item": "page_X"},
        {"user": "carlos", "action": "purchase", "item": "item_B"},
        {"user": "denise", "action": "scroll", "item": "button_Y"},
    ]

    try:
        await producer.connect()
        print("Producer connected. Sending messages... (Press Ctrl+C to stop)")

        message_count = 0
        while True:
            event = dummy_events[message_count % len(dummy_events)]
            event["message_id"] = str(message_count)

            print(f"Sending: {event}")
            response = await producer.send(
                topic=f"{event["action"]}s", data=event, key=event["user"]
            )
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
        if await producer.is_connected():
            await producer.close()
            print("Producer connection closed.")
        else:
            print("Producer was not connected.")


if __name__ == "__main__":
    import sys
    import argparse

    def print_usage():
        print(
            "Usage: python -m tinystream.client.producer --config <config_file_path> --mode <single|cluster>"
        )
        sys.exit(1)

    parser = argparse.ArgumentParser(description="TinyStream Producer")
    parser.add_argument(
        "--config",
        type=str,
        default=DEFAULT_CONFIG_PATH,
        help="Path to TinyStream configuration file",
    )
    parser.add_argument(
        "--mode", type=str, default="single", choices=["single", "cluster"]
    )
    args = parser.parse_args()
    config_path = args.config
    asyncio.run(main(config=config_path, mode=args.mode))
