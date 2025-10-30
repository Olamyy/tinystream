import json
from pathlib import Path
from typing import Dict, Any, Optional, Type, Literal
import asyncio
import sys
import argparse
import copy
import pathlib

from aiosqlite import Connection as DBConnection

from tinystream.metastore import Metastore
from tinystream.models import TopicMetadata, PartitionMetadata
from tinystream import DEFAULT_CONFIG_PATH
from tinystream.client.connection import TinyStreamAPI
from tinystream.client.base import BaseAsyncClient
from tinystream.client.topic_manager import TopicManager
from tinystream.config.parser import TinyStreamConfig
from tinystream.controller import BrokerInfo
from tinystream.partitions.base import BasePartition
from tinystream.partitions.partition import SingleLogPartition
from tinystream.serializer.base import AbstractSerializer
from tinystream.storage.base import AbstractLogStorage
from tinystream.utils.serlializer import init_serializer


class Broker(BaseAsyncClient):
    """
    The main TinyStream server.

    It listens for client connections and routes requests to the
    correct partition.
    """

    def __init__(self, config: TinyStreamConfig, broker_id: Optional[int]) -> None:
        self.config = config
        self.mode = config.mode
        self.broker_id = broker_id
        self.broker_config = self.config.get_broker_config()
        self.host = self.broker_config["host"]
        self.port = int(self.broker_config["port"])
        self.base_log_dir = Path(self.broker_config["partition_log_path"])
        self.prefix_size = int(self.broker_config.get("prefix_size", "8"))
        self.byte_order: Literal["little", "big"] = self.broker_config.get(  # type: ignore
            "byte_order", "little"
        )

        metastore_config = self.config.get_metastore_config()

        self.metastore_db_path = Path(metastore_config.get("db_path"))
        self.db_conn: Optional[DBConnection] = None

        self.serializer_config = self.config.get_serialization_config()
        self.serializer: AbstractSerializer = init_serializer(
            self.serializer_config.get("type", "messagepack")
        )

        if self.mode == "cluster":
            controller_config = config.get_controller_config()
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
        self.partition_class: Type[BasePartition] = self.init_partition_class(
            self.broker_config.get("partition_type", "singlelogpartition")
        )

        self.brokers: Dict[int, BrokerInfo] = {}
        self.topic_metadata: Dict[str, TopicMetadata] = {}

        self.metastore_task: Optional[asyncio.Task[Any]] = None

        # In-memory mapping of actual partition objects:
        # { topic_name -> { partition_id -> BasePartition } }
        self.partitions: Dict[str, Dict[int, BasePartition]] = {}
        self._lock = asyncio.Lock()
        self._server: Optional[asyncio.Server] = None

        if self.mode == "single":
            print(
                f"[Broker {broker_id}] Running in 'single' mode. Initializing topic manager."
            )
            self.topic_manager = TopicManager(
                db_connection=None,
                brokers=self.brokers,
                topics=self.topic_metadata,
                lock=self._lock,
            )

            metastore_config = self.config.metastore
            self.metastore_http_port = int(metastore_config.get("http_port", 6000))
            self.metastore = Metastore(
                topic_manager=self.topic_manager,
                topics=self.topic_metadata,
                brokers=self.brokers,
                lock=self._lock,
                port=self.metastore_http_port,
            )

    @staticmethod
    def init_partition_class(partition_name: str) -> Type[BasePartition]:
        if partition_name == "singlelogpartition":
            return SingleLogPartition
        else:
            raise ValueError(f"Unknown partition type: {partition_name}")

    @staticmethod
    def init_storage_class(storage_name: str) -> type[AbstractLogStorage]:
        """
        Initializes the storage *class* based on configuration.
        """
        if storage_name == "filelogstorage":
            from tinystream.storage.storage import FileLogStorage

            return FileLogStorage
        else:
            raise ValueError(f"Unknown storage type: {storage_name}")

    async def _create_new_partition(
        self, topic_name: str, partition_id: int
    ) -> BasePartition:
        """
        A single, centralized method for instantiating a new partition.
        This ensures all config (serializer, storage_class) is
        passed correctly and the partition is registered in the metastore.
        """
        print(f"Creating new partition: {topic_name}-{partition_id}")

        log_file = Path(f"{self.base_log_dir}/{topic_name}/{partition_id}.log")

        storage_class = self.init_storage_class(
            self.broker_config.get("storage_type", "filelogstorage")
        )(
            log_file_path=log_file,
        )

        partition = self.partition_class(
            topic_name=topic_name,  # type: ignore
            partition_id=partition_id,  # type: ignore
            base_log_dir=self.base_log_dir,  # type: ignore
            serializer=self.serializer,  # type: ignore
            storage=storage_class,  # type: ignore
        )

        await partition.load()

        if topic_name not in self.partitions:
            self.partitions[topic_name] = {}
        self.partitions[topic_name][partition_id] = partition

        if self.db_conn:
            try:
                await self.db_conn.execute(
                    """
                    INSERT
                        OR IGNORE
                    INTO partitions (topic_name, partition_id, replicas)
                    VALUES (?, ?, ?)
                    """,
                    (topic_name, partition_id, json.dumps([])),
                )
                await self.db_conn.commit()
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
        if self.mode == "cluster":
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

        else:
            await self.init_metastore(db_path=self.metastore_db_path)
            self.topic_manager.db_connection = self.db_conn
            await self._load_metadata_from_db()
            if self.broker_id is None:
                raise ValueError("broker_id must not be None")
            self_info = BrokerInfo(
                broker_id=self.broker_id, host=self.host, port=self.port, is_alive=True
            )
            async with self._lock:
                self.brokers[self.broker_id] = self_info

            self.metastore_task = asyncio.create_task(  # type: ignore
                self.metastore.start(), name="broker-metastore-api"
            )
            print(
                f"[Broker {self.broker_id}] Metastore API docs at http://localhost:{self.port}/docs"
            )

            print("Metastore initialized successfully.")

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

        if self.db_conn:
            await self.db_conn.close()
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

            elif command == "create_topic":
                if self.mode == "single" and self.topic_manager:
                    try:
                        await self.topic_manager.create_topic(
                            request["name"],
                            request["partitions"],
                            request["replication_factor"],
                        )
                        return {
                            "status": "success",
                            "message": f"Topic {request['name']} created.",
                        }
                    except ValueError as exception:
                        return {"status": "error", "message": str(exception)}
                    except Exception as exception:
                        print(
                            f"FATAL error in handle_create_topic_request: {exception}"
                        )
                        return {
                            "status": "error",
                            "message": f"Internal server error: {exception}",
                        }
                else:
                    return {
                        "status": "error",
                        "message": "create_topic is only allowed in single mode. Use admin client.",
                    }
            else:
                return {"status": "error", "message": "Unknown command"}

        except Exception as exception:
            return {
                "status": "error",
                "message": f"Failed to process request: {exception}",
            }

    async def _load_metadata_from_db(self):
        print(f"[Broker {self.broker_id}] Loading metadata from metastore...")
        async with self._lock:
            # --- MODIFIED: Use self.topic_metadata ---
            async with self.db_conn.execute("SELECT * FROM topics") as cursor:
                async for row in cursor:
                    topic_name = row["topic_name"]
                    self.topic_metadata[topic_name] = TopicMetadata(
                        name=topic_name, partitions={}
                    )

            async with self.db_conn.execute("SELECT * FROM partitions") as cursor:
                async for row in cursor:
                    topic_name = row["topic_name"]
                    if topic_name in self.topic_metadata:
                        replicas_list = json.loads(row["replicas"])
                        part_meta = PartitionMetadata(
                            partition_id=row["partition_id"],
                            leader=row["leader"],
                            replicas=replicas_list,
                        )
                        self.topic_metadata[topic_name].partitions[
                            part_meta.partition_id
                        ] = part_meta

            print(
                f"[Broker {self.broker_id}] Loaded {len(self.topic_metadata)} topics from metastore."
            )

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
        for assignment in assignments:
            topic = assignment["topic"]
            part_id = assignment["partition_id"]

            try:
                await self.get_or_create_partition(topic, part_id)

                # TODO: A full implementation would also:
                # 1. Store the 'role' (leader/follower)
                # 2. Trigger follower fetching logic if role == 'follower'
                # 3. Handle partition *removals* (i.e., assignments that
                #    disappeared from the controller's list)

            except Exception as e:
                print(f"Error reconciling partition {topic}/{part_id}: {e}")

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

            if not self.db_conn:
                return {"status": "error", "message": "Metastore is not enabled."}

            await self.db_conn.execute(
                """
                INSERT OR REPLACE INTO consumer_group_offsets
                    (group_id, topic_name, partition_id, committed_offset)
                VALUES (?, ?, ?, ?)
                """,
                (group_id, topic, partition_id, offset),
            )

            await self.db_conn.commit()

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
    _broker_id: Optional[int] = 0,
    mode: Optional[str] = "single",
    config: Optional[str] = DEFAULT_CONFIG_PATH,
    broker_number: Optional[int] = None,
):
    if broker_number and mode == "cluster":
        print(f"[Launcher] Starting {broker_number} brokers in test mode...")
        broker_instances = []

        base_config_path = config or DEFAULT_CONFIG_PATH
        try:
            base_config = TinyStreamConfig.from_ini(base_config_path)
            base_port = int(base_config.broker_config.get("port", "9090"))
        except Exception as exception:
            print(
                f"FATAL: Could not load base config from {base_config_path}: {exception}"
            )
            return

        for i in range(broker_number):
            broker_config = copy.deepcopy(base_config)
            broker_config.mode = "cluster"
            broker_config.broker_config["port"] = f"{base_port + i}"

            print(
                f"[Launcher] Preparing Broker {i} on port {broker_config.broker_config['port']}..."
            )
            broker_instances.append(
                Broker(
                    config=broker_config,
                    broker_id=i,
                )
            )
        start_tasks = [b.start() for b in broker_instances]
        try:
            await asyncio.gather(*start_tasks)
        except KeyboardInterrupt:
            print("\n[Launcher] Caught interrupt, shutting down all brokers...")
        finally:
            print("[Launcher] Closing all brokers...")
            close_tasks = [b.close() for b in broker_instances]
            await asyncio.gather(*close_tasks)
        return

    else:
        broker_id_to_use = _broker_id

        try:
            config_obj = TinyStreamConfig.from_ini(config or DEFAULT_CONFIG_PATH)
        except Exception as exception:
            print(f"FATAL: Could not load config from {config}: {exception}")
            return

        config_obj.mode = mode  # type: ignore

        try:
            port_value = config_obj.broker_config.get("port")
            if port_value is None:
                print(f"FATAL: 'port' not found or is invalid in config file: {config}")
                return
            broker_port = int(port_value)
        except (TypeError, ValueError):
            print(f"FATAL: 'port' not found or is invalid in config file: {config}")
            return

        broker = Broker(
            config=config_obj,
            broker_id=broker_id_to_use,
        )

        try:
            if mode == "cluster":
                print(
                    f"\n[Broker {broker_id_to_use}] Starting in CLUSTER mode on port {broker_port}..."
                )
            else:
                print(f"\n[Broker 0] Starting in SINGLE mode on port {broker_port}...")

            await broker.start()

        except KeyboardInterrupt:
            if mode == "cluster":
                print(
                    f"\n[Broker {broker_id_to_use}] Caught interrupt, shutting down..."
                )
            else:
                print("\n[Broker 0] Caught interrupt, shutting down...")
        finally:
            await broker.close()


if __name__ == "__main__":

    def print_usage(parser_instance, message):
        print(f"Error: {message}\n")
        parser_instance.print_help()
        sys.exit(1)

    parser = argparse.ArgumentParser(description="Start TinyStream broker(s).")
    parser.add_argument(
        "--mode",
        choices=["single", "cluster"],
        default="single",
        help="Broker mode. 'single' for standalone, 'cluster' to connect to a controller.",
    )
    parser.add_argument(
        "--broker-id",
        type=int,
        help="Broker ID (required in 'cluster' mode when starting a single broker).",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=DEFAULT_CONFIG_PATH,
        help=f"Config file path (default: {DEFAULT_CONFIG_PATH})",
    )
    parser.add_argument(
        "--broker-number",
        type=int,
        help="[TESTING] Start N brokers in a single process. Overrides other settings.",
    )
    args = parser.parse_args()

    _broker_id_to_pass = 0

    if args.broker_number:
        if args.broker_number <= 0:
            print_usage(parser, "--broker-number must be greater than 0.")

        args.mode = "cluster"
        _broker_id_to_pass = 0

    else:
        if args.mode == "cluster" and args.broker_id is None:
            print_usage(parser, "--broker_id is required in 'cluster' mode.")

        _broker_id_to_pass = args.broker_id if args.broker_id is not None else 0

    config_path = pathlib.Path(args.config)
    if not config_path.is_file():
        print(f"Error: Config file not found at {config_path}")
        sys.exit(1)

    try:
        asyncio.run(
            main(
                _broker_id=_broker_id_to_pass,
                mode=args.mode,
                config=str(config_path),
                broker_number=args.broker_number,
            )
        )
    except Exception as e:
        print(f"FATAL: Broker main loop crashed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
