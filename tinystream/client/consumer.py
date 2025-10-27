import asyncio
import random
import uuid
from typing import Any, Dict, List, Tuple, Optional

from tinystream import DEFAULT_CONFIG_PATH
from tinystream.client.connection import TinyStreamAPI
from tinystream.cluster_manager import ClusterManager
from tinystream.config.parser import TinyStreamConfig
from tinystream.serializer.base import AbstractSerializer
from tinystream.utils.serlializer import init_serializer


class Consumer:
    """
    A stateful consumer that tracks its own offsets for assigned partitions.
    Supports both "single" broker and "cluster" (controller-aware) modes.
    """

    def __init__(
        self,
        group_id: str,
        config: TinyStreamConfig,
    ) -> None:
        self.group_id = group_id
        self.config = config
        self.mode = self.config.mode

        serializer_config = config.get_serialization_config()
        self.serializer: AbstractSerializer = init_serializer(
            serializer_config.get("type", "messagepack")
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

        # { (topic, partition_id): next_offset }
        self._assignments: Dict[Tuple[str, int], int] = {}

        # Caches high-watermarks to avoid polling empty partitions
        self._hwms: Dict[Tuple[str, int], int] = {}

    async def connect(self) -> None:
        """Explicitly connects to the controller or the single broker."""
        if self.mode == "cluster":
            print("[Consumer] (Cluster Mode): Connecting to controller...")
            await self._controller_connection.ensure_connected()  # type: ignore
            await self._refresh_cluster_metadata()

        elif self.mode == "single":
            print("[Consumer] (Single Mode): Connecting to broker...")
            await self._single_broker_connection.ensure_connected()

    async def close(self) -> None:
        """Closes all active connections."""
        print("Closing consumer connections...")
        if self.mode == "cluster":
            if self._controller_connection:
                await self._controller_connection.close()
            for conn in self._broker_connections.values():
                await conn.close()

        elif self.mode == "single":
            if self._single_broker_connection:
                await self._single_broker_connection.close()

    async def is_connected(self) -> bool:
        """Checks if the consumer is connected."""
        if self.mode == "cluster":
            if not self._controller_connection.is_connected:  # type: ignore
                return False
            for conn in self._broker_connections.values():
                if not conn.is_connected:
                    return False
            return True

        elif self.mode == "single":
            return self._single_broker_connection.is_connected

        return False

    def assign(self, topic: str, partition: int = 0, start_offset: int = 0) -> None:
        """
        Assigns this consumer to a specific partition, starting from a given
        offset. Call this *before* polling.
        """
        key = (topic, partition)
        self._assignments[key] = start_offset
        self._hwms[key] = 0
        print(f"[Consumer] assigned to {topic}-{partition} at offset {start_offset}")

    async def _get_connection_for_partition(
        self, topic: str, partition: int
    ) -> TinyStreamAPI:
        """
        Gets the correct broker connection for a partition based on mode.
        """
        if self.mode == "single":
            await self._single_broker_connection.ensure_connected()  # type: ignore
            return self._single_broker_connection
        else:
            return await self.cluster_manager.get_leader_connection(topic, partition)

    async def _update_high_watermarks(self) -> None:
        """
        Asks the correct broker for the latest HWM for all assigned partitions.
        """
        for topic, part in self._assignments.keys():
            try:
                conn = await self._get_connection_for_partition(topic, part)

                resp = await conn.send_request(
                    {"command": "get_hwm", "topic": topic, "partition": part}
                )
                if resp.get("status") == "ok":
                    self._hwms[(topic, part)] = resp["high_watermark"]
                else:
                    print(
                        f"Failed to get HWM for {topic}-{part}: {resp.get('message')}"
                    )
                    if self.mode == "cluster":
                        await self.cluster_manager.invalidate_caches(conn)

            except Exception as e:
                print(f"Failed to get HWM for {topic}-{part}: {e}")
                if self.mode == "cluster":
                    await self.cluster_manager.invalidate_caches()

    async def poll(self, max_messages: int = 100) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []

        await self._update_high_watermarks()

        while len(results) < max_messages:
            messages_polled_this_round = 0

            for (topic, part), next_offset in self._assignments.items():
                if len(results) >= max_messages:
                    break

                hwm = self._hwms.get((topic, part), next_offset)

                if next_offset < hwm:
                    try:
                        conn = await self._get_connection_for_partition(topic, part)

                        resp = await conn.send_request(
                            {
                                "command": "read",
                                "topic": topic,
                                "partition": part,
                                "offset": next_offset,
                            }
                        )

                        if resp.get("status") == "ok":
                            results.append(resp["data"])
                            self._assignments[(topic, part)] = next_offset + 1
                            messages_polled_this_round += 1
                        else:
                            print(
                                f"Read error on {topic}-{part} at {next_offset}: {resp.get('message')}"
                            )
                            self._hwms[(topic, part)] = 0
                            if self.mode == "cluster":
                                await self.cluster_manager.invalidate_caches(conn)

                    except Exception as e:
                        print(f"Failed to read from {topic}-{part}: {e}")
                        self._hwms[(topic, part)] = 0
                        if self.mode == "cluster":
                            await self.cluster_manager.invalidate_caches()

            if messages_polled_this_round == 0:
                break

        return results

    async def commit(self) -> List[Dict[str, Any]]:
        """
        Commits the current tracked offsets for all assigned partitions
        to the correct broker (leader or single).
        """
        print(f"Committing offsets for group '{self.group_id}'...")
        commit_tasks = []

        for (topic, part), offset in self._assignments.items():
            request = {
                "command": "commit_offset",
                "group_id": self.group_id,
                "topic": topic,
                "partition": part,
                "offset": offset,
            }

            commit_tasks.append(self._send_commit_request(topic, part, request))

        responses = await asyncio.gather(*commit_tasks)
        print(f"Commit finished. Responses: {responses}")
        return responses

    async def _send_commit_request(
        self, topic: str, partition: int, request: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Helper to send a commit request to the correct broker."""
        try:
            conn = await self._get_connection_for_partition(topic, partition)
            return await conn.send_request(request)
        except Exception as e:
            print(f"Failed to commit for {topic}-{partition}: {e}")
            if self.mode == "cluster":
                await self.cluster_manager.invalidate_caches()
            return {"status": "error", "message": str(e)}

    async def _refresh_cluster_metadata(self) -> None:
        """Fetches the latest cluster state from the controller."""
        if self.mode != "cluster":
            return

        async with self._metadata_lock:
            print("[Consumer]: Refreshing cluster metadata from controller...")
            try:
                await self._controller_connection.ensure_connected()  # type: ignore
                response = await self._controller_connection.send_request(  # type: ignore
                    {"command": "get_cluster_metadata"}
                )

                if response.get("status") == "ok":
                    metadata = response["metadata"]
                    self._broker_info_cache = metadata.get("brokers", {})
                    self._topic_metadata_cache = metadata.get("partitions", {})
                    print("[Consumer]: Metadata refreshed.")
                else:
                    print(
                        f"[Consumer]: Failed to refresh metadata: {response.get('message')}"
                    )
            except Exception as e:
                print(f"[Consumer]: Error refreshing metadata: {e}")


async def main(
    config: Optional[str] = DEFAULT_CONFIG_PATH,
    topic: Optional[str] = None,
    mode: str = "single",
    group_id: Optional[str] = None,
) -> None:
    config = TinyStreamConfig.from_ini(config or DEFAULT_CONFIG_PATH)  # type: ignore
    config.mode = mode  # type: ignore

    if not group_id:
        print("[Consumer] No group_id provided, generating a random one.")
        group_id = f"consumer-{uuid.uuid4()}"

    if not topic:
        print(
            "[Consumer] No topic provided. Will randomly pick between available test topics."
        )
        topic = random.choices(["click", "view", "purchase", "scroll"], k=1)[0]

    consumer = Consumer(
        config=config,  # type: ignore
        group_id=group_id,
    )

    start_offset = 0
    partition = 0

    try:
        await consumer.connect()
        print("Consumer connected.")

        consumer.assign(topic=topic, partition=partition, start_offset=start_offset)
        print(
            f"Assigned to {topic}-{partition} at offset {start_offset}. Polling... (Press Ctrl+C to stop)"
        )

        while True:
            batch = await consumer.poll(max_messages=10)

            if batch:
                print("--- Received batch ---")
                for msg in batch:
                    print(f"Received: {msg}")
                print("----------------------")
                await consumer.commit()
            else:
                await asyncio.sleep(1)

    except ConnectionRefusedError:
        print("\n[ERROR] Could not connect to broker.")
        print("Please ensure the broker is running in another terminal:")
        print("  python -m tinystream.broker")

    except KeyboardInterrupt:
        print("\n\nStopping consumer... (Ctrl+C pressed)")

    except Exception as e:
        print(f"\nAn error occurred: {e}")

    finally:
        if consumer.is_connected():  # type: ignore
            await consumer.close()
            print("Consumer connection closed.")
        else:
            print("Consumer was not connected.")


if __name__ == "__main__":
    import sys
    import argparse

    def print_usage():
        print(
            "Usage: python consumer.py [--config CONFIG_PATH] [--mode single|cluster] [--group_id GROUP_ID]"
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
    parser.add_argument(
        "--topic", required=True, type=str, help="Topic to consume from"
    )
    parser.add_argument("--group_id", type=str, help="Consumer group ID")
    args = parser.parse_args()
    config_path = args.config
    asyncio.run(
        main(
            config=config_path,
            mode=args.mode,
            group_id=args.group_id,
            topic=args.topic,
        )
    )
