import asyncio
import os
from collections import defaultdict
from pathlib import Path
from typing import Dict, Any, Optional, Type, Literal

from tinystream.partitions.base import BasePartition
from tinystream.partitions.partition import SingleLogPartition
from tinystream.serializer.base import AbstractSerializer
from tinystream.storage.base import AbstractLogStorage
from tinystream.config.parser import load_config

DEFAULT_CONFIG_PATH = os.environ.get(
    "TINYSTREAM_CONFIG_FILE", "tinystream/config/conf.ini"
)


class Broker:
    """
    The main TinyStream server.

    It listens for client connections and routes requests to the
    correct partition.
    """

    def __init__(self, config_path: Optional[str] = None):
        self.config = load_config(file_path=config_path or DEFAULT_CONFIG_PATH)
        self.broker_config = self.config["broker"]
        self.host = self.broker_config["host"]
        self.port = int(self.broker_config["port"])
        self.base_log_dir = Path(self.broker_config["partition_log_path"])
        self.prefix_size = int(self.broker_config.get("prefix_size", "8"))
        self.byte_order: Literal["little", "big"] = self.broker_config.get(
            "byte_order", "little"
        )  # type: ignore

        self.serializer: AbstractSerializer = self.init_serializer(
            self.broker_config.get("serializer_type", "messagepack")
        )
        self.partition_class: Type[BasePartition] = self.init_partition_class(
            self.broker_config.get("partition_type", "singlelogpartition")
        )

        # In-memory mapping of:
        # { topic_name -> { partition_id -> BasePartition } }
        self.topics: Dict[str, Dict[int, BasePartition]] = defaultdict(dict)
        self._lock = asyncio.Lock()

    @staticmethod
    def init_serializer(serializer_name: str) -> AbstractSerializer:
        if serializer_name == "messagepack":
            from tinystream.serializer.msg_pack import MSGPackSerializer

            return MSGPackSerializer()
        else:
            raise ValueError(f"Unknown serializer: {serializer_name}")

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
        passed correctly.
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
        self.topics[topic_name][partition_id] = partition
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
            print("topic_dir", topic_dir)
            if not topic_dir.is_dir():
                continue

            topic_name = topic_dir.name
            for log_file in topic_dir.glob("*.log"):
                try:
                    partition_id = int(log_file.stem)
                    await self._create_new_partition(topic_name, partition_id)

                except ValueError:
                    print(f"Skipping non-numeric log file: {log_file}")

        print(f"Finished loading. Found {len(self.topics)} topics.")

    async def get_or_create_partition(
        self, topic_name: str, partition_id: int
    ) -> BasePartition:
        """
        Retrieves a partition, creating it if it doesn't exist.
        This is a central part of the broker's logic.
        """
        # Check if it exists first without a lock for speed
        if partition := self.topics.get(topic_name, {}).get(partition_id):
            return partition

        async with self._lock:
            if partition := self.topics.get(topic_name, {}).get(partition_id):
                return partition

            return await self._create_new_partition(topic_name, partition_id)

    async def start(self) -> None:
        """Starts the main broker server."""
        await self.load_partitions()

        server = await asyncio.start_server(
            self.handle_client_connection, self.host, self.port
        )

        addr = server.sockets[0].getsockname()
        print(f"Broker listening on {addr[0]}:{addr[1]}...")

        async with server:
            await server.serve_forever()

    async def handle_client_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        """
        Callback for each new client connection.
        """
        peer = writer.get_extra_info("peername")
        print(f"New connection from {peer}")
        try:
            while True:
                len_prefix_bytes = await reader.readexactly(self.prefix_size)
                if not len_prefix_bytes:
                    break

                payload_len = int.from_bytes(len_prefix_bytes, self.byte_order)

                payload_bytes = await reader.readexactly(payload_len)

                response = await self.dispatch_request(payload_bytes)

                response_bytes = self.serializer.serialize(response)
                response_len_prefix = len(response_bytes).to_bytes(
                    self.prefix_size, self.byte_order
                )

                writer.write(response_len_prefix)
                writer.write(response_bytes)
                await writer.drain()

        except asyncio.IncompleteReadError:
            print(f"Client {peer} disconnected unexpectedly.")
        except Exception as e:
            print(f"Error handling client {peer}: {e}")
        finally:
            print(f"Closing connection from {peer}")
            writer.close()
            await writer.wait_closed()

    async def dispatch_request(self, payload_bytes: bytes) -> Dict[str, Any]:
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
            else:
                return {"status": "error", "message": "Unknown command"}

        except Exception as e:
            return {"status": "error", "message": f"Failed to process request: {e}"}

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

    async def _handle_get_hwm(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Gets the High Watermark (next write offset) for a partition."""
        topic = request["topic"]
        partition_id = request["partition"]

        try:
            partition = await self.get_or_create_partition(topic, partition_id)
            hwm = partition.get_high_watermark()
            return {"status": "ok", "high_watermark": hwm}
        except KeyError:
            return {"status": "error", "message": "Topic or partition not found"}


if __name__ == "__main__":
    broker = Broker()

    try:
        asyncio.run(broker.start())
    except KeyboardInterrupt:
        print("\nBroker shutting down.")
