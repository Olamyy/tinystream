import asyncio

from dataclasses import dataclass, asdict
import time
from typing import Dict, List, Optional, Any, Literal
from pathlib import Path

from tinystream import DEFAULT_CONFIG_PATH
from tinystream.client.base import BaseAsyncClient
from tinystream.config.parser import TinyStreamConfig
from tinystream.serializer.base import AbstractSerializer


@dataclass
class BrokerInfo:
    broker_id: int
    host: str
    port: int
    last_heartbeat: float = time.time()
    is_alive: bool = True
    status: Literal["ALIVE", "TIMED_OUT", "SHUTDOWN"] = "ALIVE"


@dataclass
class PartitionMetadata:
    partition_id: int
    leader: Optional[int]
    replicas: List[int]


@dataclass
class TopicMetadata:
    name: str
    partitions: Dict[int, PartitionMetadata]


class Controller(BaseAsyncClient):
    """
    TinyStream Controller — manages cluster metadata, brokers, and leader elections.
    (Now a fully asynchronous, persistent server)
    """

    def __init__(self, config: TinyStreamConfig):
        self.config = config

        controller_config = config.get_controller_config()
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
                    name, p_count, r_factor = row
                    self.topics[name] = TopicMetadata(name=name, partitions={})

            async with self.db_connection.execute(
                "SELECT * FROM partitions"
            ) as p_cursor:
                async for row in p_cursor:
                    topic, p_id, leader, replicas_json = row
                    replicas = [int(r) for r in replicas_json.split(",")]
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
        await self._load_metadata()
        self.start_background_tasks()

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
        print("\n[Controller] Shutting down...")
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
                await self.register_broker(
                    request["broker_id"], request["host"], request["port"]
                )
                return {"status": "ok"}

            elif command == "deregister_broker":
                return await self._handle_deregister(request)

            elif command == "heartbeat":
                await self.update_broker_heartbeat(request["broker_id"])
                return {"status": "ok"}

            elif command == "create_topic":
                await self.create_topic(
                    request["name"],
                    request["partitions"],
                    request["replication_factor"],
                )
                return {"status": "ok"}

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

    async def create_topic(self, name: str, partitions: int, replication_factor: int):
        async with self._lock:
            if not self.db_connection:
                raise Exception("Database not connected")
            if name in self.topics:
                raise ValueError(f"Topic {name} already exists.")

            await self.db_connection.execute(
                "INSERT INTO topics (topic_name, partition_count, replication_factor) VALUES (?, ?, ?)",
                (name, partitions, replication_factor),
            )

            topic_metadata = TopicMetadata(name=name, partitions={})
            self.topics[name] = topic_metadata

            await self._assign_partitions(name, partitions, replication_factor)

            await self.db_connection.commit()

    async def _assign_partitions(
        self, topic: str, partitions: int, replication_factor: int
    ):
        if not self.db_connection:
            raise Exception("Database not connected")

        brokers_alive = [b.broker_id for b in self.brokers.values() if b.is_alive]
        if len(brokers_alive) < replication_factor:
            raise ValueError("Not enough brokers alive to satisfy replication factor.")

        topic_metadata = self.topics[topic]

        partition_data_to_insert = []
        for partition_id in range(partitions):
            replicas = []
            for i in range(replication_factor):
                broker_index = (partition_id + i) % len(brokers_alive)
                replicas.append(brokers_alive[broker_index])

            leader = replicas[0]
            replicas_json = ",".join(map(str, replicas))

            partition_metadata = PartitionMetadata(
                partition_id=partition_id, leader=leader, replicas=replicas
            )
            topic_metadata.partitions[partition_id] = partition_metadata

            partition_data_to_insert.append(
                (topic, partition_id, leader, replicas_json)
            )

        await self.db_connection.executemany(
            "INSERT INTO partitions (topic_name, partition_id, leader, replicas) VALUES (?, ?, ?, ?)",
            partition_data_to_insert,
        )
        print(f"[Controller] Assigned and persisted partitions for topic {topic}")

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


async def main():
    """
    Custom startup script to initialize, register brokers, and run the server.
    """
    config = TinyStreamConfig.from_ini(DEFAULT_CONFIG_PATH)
    controller = Controller(config=config)
    try:
        await controller.start()

        default_broker_counter = 2
        for _broker_id in range(default_broker_counter):
            print(f"[Controller] Pre-registering broker {_broker_id}...")
            await controller.register_broker(
                broker_id=_broker_id, host="localhost", port=int(f"909{_broker_id}")
            )

        print("[Controller] Startup complete. Running server forever...")
        await controller.run_forever()

    except KeyboardInterrupt:
        print("\n[Controller] Caught interrupt, shutting down...")
    finally:
        await controller.close()
        print("[Controller] Shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[Controller] Shutdown forced.")
