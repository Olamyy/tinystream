import asyncio
import json

from dataclasses import asdict
import time
from typing import Dict, Optional, Any, Literal
from pathlib import Path

from tinystream.metastore import Metastore
from tinystream.models import BrokerInfo, TopicMetadata, PartitionMetadata
from tinystream import DEFAULT_CONTROLLER_CONFIG_PATH
from tinystream.client.base import BaseAsyncClient
from tinystream.client.topic_manager import TopicManager
from tinystream.config.manager import ConfigManager
from tinystream.serializer.base import AbstractSerializer


class Controller(BaseAsyncClient):
    """
    Manages cluster metadata, brokers, and leader elections.
    """

    def __init__(self, config: ConfigManager):
        self.config = config

        controller_config = config.controller_config
        self.host = controller_config["host"]
        self.port = int(controller_config["port"])
        self.heartbeat_timeout = float(controller_config["heartbeat_timeout"])

        self.prefix_size = int(controller_config.get("prefix_size", "8"))
        self.byte_order: Literal["little", "big"] = controller_config.get(  # type: ignore
            "byte_order", "little"
        )
        self.serializer: AbstractSerializer = self.init_serializer(
            controller_config.get("serializer_type", "messagepack")
        )

        super().__init__(
            prefix_size=self.prefix_size,
            byte_order=self.byte_order,
            serializer=self.serializer,
            host=self.host,
            port=self.port,
        )

        metastore_config = self.config.metastore

        self.metastore_db_path = Path(metastore_config["db_path"])

        self.brokers: Dict[int, BrokerInfo] = {}
        self.topics: Dict[str, TopicMetadata] = {}

        self._lock = asyncio.Lock()
        self._monitor_task: Optional[asyncio.Task] = None

        self.topic_manager = TopicManager(
            db_connection=None,
            brokers=self.brokers,
            topics=self.topics,
            lock=self._lock,
        )

        self.metastore_http_port = int(metastore_config.get("http_port", 6000))

        self.metastore = Metastore(
            topic_manager=self.topic_manager,
            topics=self.topics,
            brokers=self.brokers,
            lock=self._lock,
            port=self.metastore_http_port,
        )
        self.metastore_task = None

    @staticmethod
    def init_serializer(serializer_name: str) -> AbstractSerializer:
        if serializer_name == "messagepack":
            from tinystream.serializer.msg_pack import MSGPackSerializer

            return MSGPackSerializer()
        else:
            raise ValueError(f"Unknown serializer: {serializer_name}")

    async def _load_metadata(self):
        """Loads all cluster metadata from the DB into the in-memory cache."""
        if not self.db_connection:
            return

        print("[Controller] Loading metadata from database...")
        async with self._lock:
            async with self.db_connection.execute("SELECT * FROM brokers") as cursor:
                async for row in cursor:
                    broker_id, host, port = row
                    self.brokers[broker_id] = BrokerInfo(
                        broker_id=broker_id, host=host, port=port
                    )

            async with self.db_connection.execute("SELECT * FROM topics") as t_cursor:
                async for row in t_cursor:
                    name, p_count, r_factor, retention_ms, retention_bytes = row
                    self.topics[name] = TopicMetadata(
                        name=name,
                        partitions={},
                        retention_ms=retention_ms,
                        retention_bytes=retention_bytes,
                    )

            async with self.db_connection.execute(
                "SELECT * FROM partitions"
            ) as p_cursor:
                async for row in p_cursor:
                    topic, p_id, leader, replicas = row
                    replicas = [int(r) for r in json.loads(replicas)]
                    if topic in self.topics:
                        self.topics[topic].partitions[p_id] = PartitionMetadata(
                            partition_id=p_id, leader=leader, replicas=replicas
                        )

        print(
            f"[Controller] Loaded {len(self.brokers)} brokers and {len(self.topics)} topics."
        )

    async def start(self):
        """Starts the controller server and background tasks."""
        await self.init_metastore(db_path=self.metastore_db_path)
        self.topic_manager.db_connection = self.db_connection

        await self._load_metadata()
        self.metastore_task = asyncio.create_task(
            self.metastore.start(), name="metastore-api-server"
        )
        self.start_background_tasks()

        print(
            f"[Controller] Metastore API docs at http://localhost:{self.metastore_http_port}/docs"
        )

        await self.start_server()
        addr = self._server.sockets[0].getsockname()
        print(f"[Controller] Listening on {addr[0]}:{addr[1]}...")

    async def run_forever(self) -> None:
        """Runs the main server loop. This call blocks forever."""
        if not self._server:
            raise RuntimeError(
                "Controller server has not been started. Call start() first."
            )

        async with self._server:
            await self._server.serve_forever()

    async def close(self):
        print("\n[Controller] Shutdown...")
        await self.stop_background_tasks()
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            print("[Controller] Server socket closed.")
        if self.db_connection:
            await self.db_connection.close()
            print("[Controller] Metastore connection closed.")

    async def send_request(self, payload_bytes: bytes) -> Dict[str, Any]:
        """Deserializes request and calls the correct controller method."""
        try:
            request = self.serializer.deserialize(payload_bytes)
            print(f"[Controller] Received request: {request}")
            command = request.get("command")

            if command == "register_broker":
                broker_id = request["broker_id"]
                await self.register_broker(broker_id, request["host"], request["port"])

                assignments = await self._get_assignments_for_broker(broker_id)

                return {
                    "status": "ok",
                    "message": "Broker registered successfully",
                    "assignments": assignments,
                }

            elif command == "deregister_broker":
                return await self._handle_deregister(request)

            elif command == "heartbeat":
                broker_id = request["broker_id"]
                await self.update_broker_heartbeat(broker_id)

                assignments = await self._get_assignments_for_broker(broker_id)

                return {"status": "ok", "assignments": assignments}

            elif command == "create_topic":
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
                except ValueError as e:
                    return {"status": "error", "message": str(e)}
                except Exception as e:
                    print(f"FATAL error in handle_create_topic_request: {e}")
                    return {"status": "error", "message": f"Internal server error: {e}"}

            elif command == "get_cluster_metadata":
                metadata = await self.get_cluster_metadata()
                return {"status": "ok", "metadata": metadata}

            else:
                return {"status": "error", "message": "Unknown command"}

        except Exception as e:
            return {"status": "error", "message": f"Failed to process request: {e}"}

    async def register_broker(self, broker_id: int, host: str, port: int):
        async with self._lock:
            if not self.db_connection:
                raise Exception("Database not connected")

            await self.db_connection.execute(
                "INSERT OR REPLACE INTO brokers (broker_id, host, port) VALUES (?, ?, ?)",
                (broker_id, host, port),
            )
            await self.db_connection.commit()

            self.brokers[broker_id] = BrokerInfo(
                broker_id=broker_id, host=host, port=port
            )
            print(f"[Controller] Registered broker {broker_id} at {host}:{port}")
        return self

    async def _handle_deregister(self, request: Dict[str, Any]) -> Dict[str, Any]:
        broker_id = request["broker_id"]
        async with self._lock:
            if broker_id in self.brokers:
                print(f"[Controller] Broker {broker_id} reported graceful shutdown.")
                await self._handle_broker_failure(broker_id, new_status="SHUTDOWN")
        return {"status": "ok", "message": "Deregistered"}

    async def update_broker_heartbeat(self, broker_id: int):
        async with self._lock:
            if broker_id not in self.brokers:
                raise ValueError(f"Broker ID {broker_id} not registered.")

            self.brokers[broker_id].last_heartbeat = time.time()
            if not self.brokers[broker_id].is_alive:
                print(f"[Controller] Broker {broker_id} is alive again.")
                self.brokers[broker_id].is_alive = True
                # TODO: Trigger rebalancing

    async def _get_assignments_for_broker(self, broker_id: int) -> list[dict]:
        """
        Scans the in-memory state for all partitions assigned to a broker.
        This method is self-contained and acquires its own lock.
        """
        assignments = []
        async with self._lock:
            for topic_name, topic_meta in self.topics.items():
                for part_id, part_meta in topic_meta.partitions.items():
                    if broker_id in part_meta.replicas:
                        role = "leader" if broker_id == part_meta.leader else "follower"
                        assignments.append(
                            {
                                "topic": topic_name,
                                "partition_id": part_id,
                                "role": role,
                                "retention_ms": topic_meta.retention_ms,
                                "retention_bytes": topic_meta.retention_bytes,
                            }
                        )
        return assignments

    async def _elect_leader(self, topic: str, partition_id: int) -> Optional[int]:
        if not self.db_connection:
            raise Exception("Database not connected")

        partition_metadata = self.topics[topic].partitions.get(partition_id)
        if not partition_metadata:
            raise ValueError(f"Partition {partition_id} does not exist.")

        new_leader = None
        for broker_id in partition_metadata.replicas:
            if self.brokers[broker_id].is_alive:
                new_leader = broker_id
                break

        await self.db_connection.execute(
            "UPDATE partitions SET leader = ? WHERE topic_name = ? AND partition_id = ?",
            (new_leader, topic, partition_id),
        )
        await self.db_connection.commit()

        partition_metadata.leader = new_leader

        if new_leader:
            print(
                f"[Controller] Elected new leader for {topic}-{partition_id}: Broker {new_leader}"
            )
        else:
            print(f"[Controller] WARNING: No live leader for {topic}-{partition_id}.")

        return new_leader

    async def remove_dead_brokers(self):
        async with self._lock:
            current_time = time.time()
            dead_broker_ids = []
            for broker in self.brokers.values():
                if broker.status == "ALIVE" and (
                    current_time - broker.last_heartbeat > self.heartbeat_timeout
                ):
                    print(f"[Controller] Broker {broker.broker_id} timed out.")
                    broker.status = "TIMED_OUT"
                    broker.failed_since = current_time
                    broker.is_alive = False
                    dead_broker_ids.append(broker.broker_id)

            for broker_id in dead_broker_ids:
                await self._handle_broker_failure(broker_id, new_status="TIMED_OUT")

    async def _handle_broker_failure(
        self, broker_id: int, new_status: Literal["TIMED_OUT", "SHUTDOWN"]
    ):
        """
        Triggers leader re-election for a failed/shutdown broker.
        Assumes lock is already held.
        """
        if broker_id in self.brokers:
            self.brokers[broker_id].status = new_status

        print(
            f"[Controller] Handling failure for broker {broker_id}, reason: {new_status}..."
        )

        for topic_metadata in self.topics.values():
            for partition_metadata in topic_metadata.partitions.values():
                if partition_metadata.leader == broker_id:
                    await self._elect_leader(
                        topic_metadata.name, partition_metadata.partition_id
                    )

    async def get_cluster_metadata(self):
        """Returns current state of brokers, topics, and partition leaders."""
        async with self._lock:
            partitions_dict = {}
            for topic_name, topic_metadata in self.topics.items():
                partitions_dict[topic_name] = {
                    part_id: {
                        "leader": part_metadata.leader,
                        "replicas": part_metadata.replicas,
                    }
                    for part_id, part_metadata in topic_metadata.partitions.items()
                }

            return {
                "brokers": {
                    broker_id: asdict(broker_info)
                    for broker_id, broker_info in self.brokers.items()
                },
                "partitions": partitions_dict,
            }

    def start_background_tasks(self):
        """Starts periodic tasks for heartbeat checking."""
        print("[Controller] Starting background heartbeat monitor...")
        self._monitor_task = asyncio.create_task(self._heartbeat_monitor_loop())

    async def stop_background_tasks(self):
        """Stops periodic tasks."""
        if self._monitor_task:
            self._monitor_task.cancel()
            print("[Controller] Stopped background heartbeat monitor.")

    async def _heartbeat_monitor_loop(self):
        """Runs periodically to check for dead brokers."""
        while True:
            await asyncio.sleep(self.heartbeat_timeout / 2)
            await self.remove_dead_brokers()

    async def get_leader(self, topic: str, partition_id: int) -> Optional[int]:
        """Returns the current leader for a given topic-partition."""
        async with self._lock:
            partition_metadata = self.topics.get(topic).partitions.get(partition_id)  # type: ignore
            if partition_metadata:
                return partition_metadata.leader
            return None


async def main(_config: str):
    """
    Custom startup script to initialize, register brokers, and run the server.
    """
    config = ConfigManager(
        args=argparse.Namespace(config=_config),
        component_type="controller",
    )
    controller = Controller(config=config)
    try:
        await controller.start()

        print("[Controller] Startup complete. Running server forever...")
        await controller.run_forever()

    except KeyboardInterrupt:
        print("\n[Controller] Caught interrupt, shutting down...")
    finally:
        await controller.close()
        print("[Controller] Shutdown complete.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Start TinyStream controller")
    parser.add_argument(
        "--config",
        type=str,
        default=DEFAULT_CONTROLLER_CONFIG_PATH,
        help=f"Config file path (default: {DEFAULT_CONTROLLER_CONFIG_PATH})",
    )
    args = parser.parse_args()

    try:
        asyncio.run(main(_config=args.config))
    except KeyboardInterrupt:
        print("\n[Controller] Shutdown forced.")
