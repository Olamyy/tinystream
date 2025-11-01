import asyncio
from typing import Dict, Any, Tuple, Optional
from tinystream.client.connection import TinyStreamAPI
from tinystream.serializer.base import AbstractSerializer
from tinystream.config.manager import ConfigManager  # Added


class ClusterManager:
    """
    A self-contained manager for cluster metadata, broker connections,
    and leader discovery.
    """

    def __init__(
        self,
        config: ConfigManager,
        serializer: AbstractSerializer,
    ) -> None:
        self._topic_metadata_cache: Dict[str, Dict[int, Any]] = {}
        self._broker_info_cache: Dict[int, Any] = {}
        self._broker_connections: Dict[Tuple[str, int], TinyStreamAPI] = {}
        self.serializer = serializer
        self._metadata_lock = asyncio.Lock()

        controller_config = config.controller_config
        self._controller_connection = TinyStreamAPI(
            controller_config.get("host"),
            int(controller_config.get("port")),
            serializer=self.serializer,
        )

    async def connect(self) -> None:
        """Connects to the controller and performs initial metadata fetch."""
        print("[ClusterManager]: Connecting to controller...")
        await self._controller_connection.ensure_connected()
        await self.refresh_cluster_metadata()

    async def close(self) -> None:
        """Closes all connections."""
        print("[ClusterManager]: Closing all connections...")
        if self._controller_connection:
            await self._controller_connection.close()
        for conn in self._broker_connections.values():
            await conn.close()
        self._broker_connections.clear()
        self._topic_metadata_cache.clear()
        self._broker_info_cache.clear()

    async def is_connected(self) -> bool:
        """Checks if the connection to the controller is active."""
        return self._controller_connection.is_connected

    async def get_topic_metadata(self, topic: str) -> Dict[int, Any]:
        """
        Gets the partition info for a topic, refreshing if not in cache.
        """
        async with self._metadata_lock:
            topic_partitions = self._topic_metadata_cache.get(topic)
            if not topic_partitions:
                await self._do_refresh()  # Already have lock
                topic_partitions = self._topic_metadata_cache.get(topic)
                if not topic_partitions:
                    raise ValueError(f"Topic '{topic}' not found after refresh.")
        return topic_partitions

    async def get_leader_connection(
        self, topic: str, partition_id: int
    ) -> TinyStreamAPI:
        """
        Gets an active connection to the leader broker for a given partition.
        Handles cache misses by querying the controller.
        """
        async with self._metadata_lock:
            topic_partitions = self._topic_metadata_cache.get(topic)
            if not topic_partitions:
                await self._do_refresh()
                topic_partitions = self._topic_metadata_cache.get(topic)
                if not topic_partitions:
                    raise ValueError(f"Topic '{topic}' not found after refresh.")

            partition_info = topic_partitions.get(
                str(partition_id), topic_partitions.get(partition_id)
            )
            if not partition_info:
                raise ValueError(
                    f"Partition {partition_id} for topic '{topic}' not found."
                )

            leader_id = partition_info.get("leader")
            if leader_id is None:
                raise ConnectionError(
                    f"No leader available for {topic}-{partition_id}."
                )

            broker_info = self._broker_info_cache.get(
                str(leader_id), self._broker_info_cache.get(leader_id)
            )
            if not broker_info:
                await self._do_refresh()
                broker_info = self._broker_info_cache.get(
                    str(leader_id), self._broker_info_cache.get(leader_id)
                )
                if not broker_info:
                    raise ValueError(f"Broker {leader_id} not found after refresh.")

            host = broker_info["host"]
            port = int(broker_info.get("data_port", broker_info["port"]))

            conn_key = (host, port)
            conn = self._broker_connections.get(conn_key)

            if not conn or not conn.is_connected:
                print(
                    f"[ClusterManager]: Creating new connection to leader {leader_id} at {host}:{port}"
                )
                conn = TinyStreamAPI(host, port, serializer=self.serializer)
                self._broker_connections[conn_key] = conn

        await conn.ensure_connected()
        return conn

    async def invalidate_caches(self, connection: Optional[TinyStreamAPI] = None):
        """Invalidates all metadata and optionally closes a bad connection."""
        print("[ClusterManager]: Invalidating caches due to error.")
        async with self._metadata_lock:
            self._topic_metadata_cache.clear()
            self._broker_info_cache.clear()

            if connection:
                conn_key = (connection.host, connection.port)
                if conn_key in self._broker_connections:
                    await self._broker_connections.pop(conn_key).close()

            await self._do_refresh()

    async def refresh_cluster_metadata(self, lock_acquired: bool = False):
        """Public method to refresh, acquiring the lock if needed."""
        if lock_acquired:
            await self._do_refresh()
        else:
            async with self._metadata_lock:
                await self._do_refresh()

    async def _do_refresh(self) -> None:
        """The actual refresh logic, assumes lock is held."""
        try:
            await self._controller_connection.ensure_connected()
            response = await self._controller_connection.send_request(
                {"command": "get_cluster_metadata"}
            )
            if response.get("status") == "ok":
                metadata = response["metadata"]
                self._broker_info_cache = metadata.get("brokers", {})
                self._topic_metadata_cache = metadata.get("partitions", {})
                print("[ClusterManager]: Metadata refreshed.")
            else:
                print(
                    f"[ClusterManager]: Failed to refresh metadata: {response.get('message')}"
                )
        except Exception as e:
            print(f"[ClusterManager]: Error refreshing metadata: {e}")

    async def create_topic(
        self, topic: str, partition_count: int, replication_factor: int = 1
    ) -> Dict[str, Any]:
        """Sends a create_topic request to the controller."""
        request = {
            "command": "create_topic",
            "topic": topic,
            "partition_count": partition_count,
            "replication_factor": replication_factor,
        }
        await self._controller_connection.ensure_connected()
        response = await self._controller_connection.send_request(request)

        if response.get("status") == "ok":
            await self.refresh_cluster_metadata()

        return response
