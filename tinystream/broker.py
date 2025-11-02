import json
from pathlib import Path
from typing import Dict, Any, Optional, Literal
import asyncio
import sys
import argparse

from aiosqlite import Connection as DBConnection

from tinystream.client.connection import TinyStreamAPI
from tinystream.client.base import BaseAsyncClient
from tinystream.config.manager import ConfigManager
from tinystream.controller import BrokerInfo
from tinystream.partitions.base import BasePartition
from tinystream.partitions.segmented import SegmentedLogPartition
from tinystream.partitions.single import SingleLogPartition
from tinystream.serializer.base import AbstractSerializer
from tinystream.storage import SingleLogStorage, SegmentedLogStorage
from tinystream.utils.env import env_default
from tinystream.utils.serlializer import init_serializer


class Broker(BaseAsyncClient):
    """
    The main TinyStream server.

    It listens for client connections and routes requests to the
    correct partition.
    """

    def __init__(self, config: ConfigManager, broker_id: Optional[int]) -> None:
        self.config = config
        self.broker_id = broker_id
        self.broker_config = self.config.broker_config
        self.host = self.broker_config["host"]
        self.port = int(self.broker_config["port"])
        self.base_log_dir = Path(self.broker_config["partition_log_path"])
        self.prefix_size = int(self.broker_config.get("prefix_size", "8"))
        self.byte_order: Literal["little", "big"] = self.broker_config.get(  # type: ignore
            "byte_order", "little"
        )

        metastore_config = self.config.metastore

        self.metastore_db_path = Path(
            metastore_config.get("db_path", "./data/metastore/tinystream.meta.db")
        )
        self.db_connection: Optional[DBConnection] = None

        self.serializer_config = self.config.serialization
        self.serializer: AbstractSerializer = init_serializer(
            self.serializer_config.get("type", "messagepack")
        )

        controller_config = config.controller_config
        self.controller_client = TinyStreamAPI(
            host=controller_config.get("host"),  # type: ignore
            port=int(controller_config.get("port")),  # type: ignore
            serializer=self.serializer,
        )
        self.heartbeat_task: Optional[asyncio.Task[Any]] = None

        super().__init__(
            prefix_size=self.prefix_size,
            byte_order=self.byte_order,
            serializer=self.serializer,
            host=self.host,
            port=self.port,
        )

        self.brokers: Dict[int, BrokerInfo] = {}

        self.metastore_task: Optional[asyncio.Task[Any]] = None

        self.partitions: Dict[str, Dict[int, BasePartition]] = {}
        self._lock = asyncio.Lock()
        self._server: Optional[asyncio.Server] = None
        self.retention_task: Optional[asyncio.Task] = None

    async def _create_new_partition(
        self, topic_name: str, partition_id: int
    ) -> BasePartition:
        """
        A single, centralized method for instantiating a new partition.
        This ensures all config (serializer, storage_class) is
        passed correctly and the partition is registered in the metastore.
        """

        storage_type = self.broker_config.get("storage_type", "singlelogstorage")

        if storage_type == "singlelogstorage":
            storage_class = SingleLogStorage
            partition_path = Path(
                f"{self.base_log_dir}/{topic_name}/{partition_id}.log"
            )
        else:
            storage_class = SegmentedLogStorage
            partition_path = Path(f"{self.base_log_dir}/{topic_name}/{partition_id}")

        storage_class = storage_class(
            partition_path=partition_path,
        )

        partition_name = self.broker_config.get("partition_type", "singlelogpartition")

        if partition_name == "singlelogpartition":
            partition_class = SingleLogPartition
        else:
            partition_class = SegmentedLogPartition

        print(
            f"Creating new partition: {topic_name}-{partition_id} of type {partition_name} and storage {storage_type}"
        )

        partition = partition_class(
            topic_name=topic_name,  # type: ignore
            partition_id=partition_id,  # type: ignore
            serializer=self.serializer,  # type: ignore
            storage=storage_class,  # type: ignore
        )

        await partition.load()

        if topic_name not in self.partitions:
            self.partitions[topic_name] = {}
        self.partitions[topic_name][partition_id] = partition

        if self.db_connection:
            try:
                await self.db_connection.execute(
                    """
                    INSERT
                        OR IGNORE
                    INTO partitions (topic_name, partition_id, replicas)
                    VALUES (?, ?, ?)
                    """,
                    (topic_name, partition_id, json.dumps([])),
                )
                await self.db_connection.commit()
            except Exception as exception:
                print(
                    f"Warning: Failed to register {topic_name}-{partition_id} in metastore: {exception}"
                )

        return partition

    async def load_partitions(self) -> None:
        """
        Scans the base log directory on startup to discover and load all
        existing partitions.
        """
        print(f"Loading partitions from {self.base_log_dir}...")
        if not self.base_log_dir.exists():
            print("Log directory not found, will create on demand.")
            return

        for topic_dir in self.base_log_dir.iterdir():
            if not topic_dir.is_dir():
                continue

            topic_name = topic_dir.name
            for log_file in topic_dir.glob("*.log"):
                try:
                    partition_id = int(log_file.stem)
                    await self._create_new_partition(topic_name, partition_id)

                except ValueError:
                    print(f"Skipping non-numeric log file: {log_file}")

        print(f"Finished loading. Found {len(self.partitions)} topics.")

    async def get_or_create_partition(
        self, topic_name: str, partition_id: int
    ) -> BasePartition:
        """
        Retrieves a partition, creating it if it doesn't exist.
        This is a central part of the broker's logic.
        """
        if partition := self.partitions.get(topic_name, {}).get(partition_id):
            return partition

        async with self._lock:
            if partition := self.partitions.get(topic_name, {}).get(partition_id):
                return partition

            return await self._create_new_partition(topic_name, partition_id)

    async def start(self) -> None:
        """Starts the main broker server."""

        await self.load_partitions()

        await self.controller_client.ensure_connected()
        print(f"[Broker {self.broker_id}] Registering with controller...")

        response = await self.controller_client.send_request(
            {
                "command": "register_broker",
                "broker_id": self.broker_id,
                "host": self.host,
                "port": self.port,
            }
        )

        if response and response.get("status") == "ok":
            print(f"[Broker {self.broker_id}] Registered successfully.")
            assignments = response.get("assignments", [])
            await self._reconcile_partitions(assignments)
        else:
            raise Exception(f"Could not register with controller: {response}")

        self.heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        self.retention_task = asyncio.create_task(self._retention_loop())
        await self.load_partitions()
        await self.start_server()

        addr = self._server.sockets[0].getsockname()  # type: ignore
        print(f"[Broker] listening on {addr[0]}:{addr[1]}...")

        async with self._server:  # type: ignore
            await self._server.serve_forever()  # type: ignore

    async def close(self):
        """Shuts down the broker and closes database connections."""
        print("\n[Broker] shutting down...")

        if self.heartbeat_task:
            self.heartbeat_task.cancel()

        if self.controller_client and self.controller_client.is_connected:
            print(f"[Broker {self.broker_id}] Deregistering...")
            await self.controller_client.send_request(
                {"command": "deregister_broker", "broker_id": self.broker_id}
            )
            await self.controller_client.close()

        if self._server:
            self._server.close()
            await self._server.wait_closed()
            print("Server socket closed.")

        if self.retention_task:
            self.retention_task.cancel()

        if self.db_connection:
            await self.db_connection.close()
            print("Metastore connection closed.")

    async def send_request(self, payload_bytes: bytes) -> Dict[str, Any]:
        """Deserializes the request and calls the correct handler."""
        try:
            request = self.serializer.deserialize(payload_bytes)
            command = request.get("command")

            if command == "append":
                return await self._handle_append(request)
            elif command == "read":
                return await self._handle_read(request)
            elif command == "get_hwm":
                return await self._handle_get_hwm(request)

            elif command == "commit_offset":
                return await self._handle_commit_offset(request)
            else:
                return {"status": "error", "message": "Unknown command"}

        except Exception as exception:
            return {
                "status": "error",
                "message": f"Failed to process request: {exception}",
            }

    async def _handle_append(self, request: Dict[str, Any]) -> Dict[str, Any]:
        topic = request["topic"]
        partition_id = request["partition"]
        data = request["data"]

        partition = await self.get_or_create_partition(topic, partition_id)
        logical_offset = await partition.append(data)

        return {
            "status": "ok",
            "topic": topic,
            "partition": partition_id,
            "offset": logical_offset,
        }

    async def _handle_read(self, request: Dict[str, Any]) -> Dict[str, Any]:
        topic = request["topic"]
        partition_id = request["partition"]
        offset = request["offset"]

        try:
            partition = await self.get_or_create_partition(topic, partition_id)
            data = await partition.read(offset)
            return {"status": "ok", "data": data}
        except IndexError:
            return {"status": "error", "message": "Offset out of range"}
        except KeyError:
            return {"status": "error", "message": "Topic or partition not found"}

    async def _reconcile_partitions(self, assignments: list[dict]):
        """
        Compares the controller's assignments with local state and creates
        any missing partitions. This is the core of the "pull" model.
        """
        print(
            f"[Broker {self.broker_id}] Reconciling {len(assignments)} assignments..."
        )
        current_assignments = set()

        for assignment in assignments:
            topic = assignment["topic"]
            part_id = assignment["partition_id"]

            try:
                partition = await self.get_or_create_partition(topic, part_id)

                partition.update_policy(
                    role=assignment["role"],
                    retention_ms=assignment["retention_ms"],
                    retention_bytes=assignment["retention_bytes"],
                )

                current_assignments.add((topic, part_id))

            except Exception as exception:
                print(f"Error reconciling partition {topic}/{part_id}: {exception}")

    async def _retention_loop(self):
        """
        Runs periodically to enforce retention policies on all partitions.
        """
        while True:
            await asyncio.sleep(300)

            print(f"[Broker {self.broker_id}] Running retention policy check...")

            async with self._lock:
                for topic_name, partitions in self.partitions.items():
                    for partition in partitions.values():
                        try:
                            await partition.enforce_retention()
                        except Exception as e:
                            print(
                                f"Error enforcing retention on {topic_name}-{partition.partition_id}: {e}"
                            )

    async def _heartbeat_loop(self):
        """Runs in the background, sending heartbeats AND processing assignments."""
        while True:
            try:
                response = await self.controller_client.send_request(
                    {"command": "heartbeat", "broker_id": self.broker_id}
                )

                if response and response.get("status") == "ok":
                    assignments = response.get("assignments", [])
                    await self._reconcile_partitions(assignments)
                    print(f"[Broker {self.broker_id}] Heartbeat sent and processed.")
                else:
                    print(
                        f"[Broker {self.broker_id}] Invalid heartbeat response: {response}"
                    )

            except Exception as exception:
                print(
                    f"[Broker {self.broker_id}] Failed to send heartbeat: {exception}"
                )

            await asyncio.sleep(3)  # Configurable interval

    async def _handle_get_hwm(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Next write offset for a partition."""
        topic = request["topic"]
        partition_id = request["partition"]

        try:
            partition = await self.get_or_create_partition(topic, partition_id)
            hwm = partition.get_high_watermark()
            return {"status": "ok", "high_watermark": hwm}
        except KeyError:
            return {"status": "error", "message": "Topic or partition not found"}

    async def _handle_commit_offset(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handles a consumer's request to commit its offset for a partition.
        Writes the offset to the 'consumer_group_offsets' table.
        """
        try:
            group_id = request["group_id"]
            topic = request["topic"]
            partition_id = request["partition"]
            offset = request["offset"]

            if not self.db_connection:
                return {"status": "error", "message": "Metastore is not enabled."}

            await self.db_connection.execute(
                """
                INSERT OR REPLACE INTO consumer_group_offsets
                    (group_id, topic_name, partition_id, committed_offset)
                VALUES (?, ?, ?, ?)
                """,
                (group_id, topic, partition_id, offset),
            )

            await self.db_connection.commit()

            return {"status": "ok", "message": "Offset committed"}

        except KeyError as exception:
            return {
                "status": "error",
                "message": f"Missing required field: {exception}",
            }
        except Exception as exception:
            return {
                "status": "error",
                "message": f"Failed to commit offset: {exception}",
            }


async def main(
    config: ConfigManager,
    _broker_id: int,
    broker_number: Optional[int] = None,
):
    if broker_number:
        return

    else:
        config_obj = config
        broker_id_to_use = _broker_id

        broker_port = int(config_obj.broker_config.get("port"))

        broker = Broker(
            config=config_obj,
            broker_id=broker_id_to_use,
        )

        try:
            print(
                f"\n[Broker {broker_id_to_use}] Starting in CLUSTER mode on port {broker_port}..."
            )
            await broker.start()
        except KeyboardInterrupt:
            print(f"\n[Broker {broker_id_to_use}] Caught interrupt, shutting down...")
        finally:
            await broker.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start TinyStream broker(s).")

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
        "--port",
        type=int,
        default=env_default("TINYSTREAM_PORT")(),
        help="Broker RPC port. Overrides config.",
    )

    parser.add_argument(
        "--broker-id",
        type=int,
        default=env_default("TINYSTREAM_BROKER_ID")(),
        help="Broker ID (required). Overrides config.",
    )

    parser.add_argument(
        "--config",
        type=str,
        default=env_default("TINYSTREAM_CONFIG")(),
        help="Path to a user config file. Overrides default config.",
    )

    parser.add_argument(
        "--broker-number", type=int, help="[TESTING] Start N brokers in one process."
    )
    args = parser.parse_args()

    config_manager = ConfigManager(args, component_type="broker")

    _broker_id_to_pass = 0
    if args.broker_number:
        if args.broker_number <= 0:
            sys.exit("Error: --broker-number must be greater than 0.")
    else:
        _id = args.broker_id or config_manager.broker_config.get("id")
        if _id is None:
            sys.exit("Error: --broker-id is required (or set 'id' in [broker] config)")
        _broker_id_to_pass = int(_id)  # type: ignore

    try:
        asyncio.run(
            main(
                config=config_manager,
                _broker_id=_broker_id_to_pass,
                broker_number=args.broker_number,
            )
        )
    except Exception as e:
        print(f"FATAL: Broker main loop crashed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
