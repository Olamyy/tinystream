import asyncio
import uuid
from typing import Any, Dict, List, Tuple, Optional
import argparse
from tinystream.cluster_manager import ClusterManager
from tinystream.config.manager import ConfigManager
from tinystream.serializer.base import AbstractSerializer
from tinystream.utils.serlializer import init_serializer
from tinystream.utils.env import env_default


class Consumer:
    def __init__(
        self,
        group_id: str,
        config: ConfigManager,
    ) -> None:
        self.group_id = group_id
        self.config = config

        serializer_config = config.serialization
        self.serializer: AbstractSerializer = init_serializer(
            serializer_config.get("type", "messagepack")
        )

        self.cluster_manager = ClusterManager(
            config=config,
            serializer=self.serializer,
        )

        self._assignments: Dict[Tuple[str, int], int] = {}
        self._hwms: Dict[Tuple[str, int], int] = {}

    async def connect(self) -> None:
        """Explicitly connects to the controller."""
        print("[Consumer]: Connecting...")
        await self.cluster_manager.connect()

    async def close(self) -> None:
        """Closes all active connections."""
        print("Closing consumer connections...")
        await self.cluster_manager.close()

    async def is_connected(self) -> bool:
        """Checks if the consumer is connected."""
        return await self.cluster_manager.is_connected()

    def assign(self, topic: str, partition: int = 0, start_offset: int = 0) -> None:
        key = (topic, partition)
        self._assignments[key] = start_offset
        self._hwms[key] = 0
        print(f"[Consumer] assigned to {topic}-{partition} at offset {start_offset}")

    async def _update_high_watermarks(self) -> None:
        """
        Asks the correct broker for the latest HWM for all assigned partitions.
        """
        for topic, part in self._assignments.keys():
            try:
                conn = await self.cluster_manager.get_leader_connection(topic, part)

                resp = await conn.send_request(
                    {"command": "get_hwm", "topic": topic, "partition": part}
                )
                if resp.get("status") == "ok":
                    self._hwms[(topic, part)] = resp["high_watermark"]
                else:
                    print(
                        f"Failed to get HWM for {topic}-{part}: {resp.get('message')}"
                    )
                    await self.cluster_manager.invalidate_caches(conn)

            except Exception as e:
                print(f"Failed to get HWM for {topic}-{part}: {e}")
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
                        conn = await self.cluster_manager.get_leader_connection(
                            topic, part
                        )

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
                            await self.cluster_manager.invalidate_caches(conn)

                    except Exception as e:
                        print(f"Failed to read from {topic}-{part}: {e}")
                        self._hwms[(topic, part)] = 0
                        await self.cluster_manager.invalidate_caches()

            if messages_polled_this_round == 0:
                break

        return results

    async def commit(self) -> List[Dict[str, Any]]:
        """
        Commits the current tracked offsets for all assigned partitions.
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
            conn = await self.cluster_manager.get_leader_connection(topic, partition)
            return await conn.send_request(request)
        except Exception as e:
            print(f"Failed to commit for {topic}-{partition}: {e}")
            await self.cluster_manager.invalidate_caches()
            return {"status": "error", "message": str(e)}


async def main(
    config: ConfigManager,
    topic: str,
    group_id: Optional[str] = None,
) -> None:
    print("[Consumer] Starting in cluster mode (from config).")

    if not group_id:
        print("[Consumer] No group_id provided, generating a random one.")
        group_id = f"consumer-{uuid.uuid4()}"

    consumer = Consumer(
        config=config,
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
                    try:
                        print(f"Received: {msg.decode('utf-8')}")
                    except:
                        print(f"Received (raw): {msg}")
                print("----------------------")
                await consumer.commit()
            else:
                await asyncio.sleep(1)

    except ConnectionRefusedError:
        print("\n[ERROR] Could not connect to broker or controller.")
        print("Please ensure the broker/controller is running.")
    except KeyboardInterrupt:
        print("\n\nStopping consumer... (Ctrl+C pressed)")
    except Exception as e:
        print(f"\nAn error occurred: {e}")
    finally:
        if await consumer.is_connected():
            await consumer.close()
            print("Consumer connection closed.")
        else:
            print("Consumer was not connected.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TinyStream Consumer")

    parser.add_argument(
        "--controller-uri",
        type=str,
        default=env_default("TINYSTREAM_CONTROLLER_URI"),
        help="Controller RPC URI (e.g., localhost:9093). Overrides config.",
    )
    parser.add_argument(
        "--metastore-uri",
        type=str,
        default=env_default("TINYSTREAM_METASTORE_URI"),
        help="Metastore HTTP API URI. Overrides config.",
    )

    parser.add_argument(
        "--config",
        type=str,
        default=env_default("TINYSTREAM_CONFIG"),
        help="Path to a user config file. Overrides default config.",
    )

    parser.add_argument(
        "--topic", required=True, type=str, help="Topic to consume from"
    )
    parser.add_argument("--group_id", type=str, help="Consumer group ID")

    args = parser.parse_args()
    config_manager = ConfigManager(args, component_type="broker")

    asyncio.run(
        main(
            config=config_manager,
            group_id=args.group_id,
            topic=args.topic,
        )
    )
