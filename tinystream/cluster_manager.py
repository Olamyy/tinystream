from typing import Dict, Any, Tuple, Optional
from tinystream.client.connection import TinyStreamAPI
from tinystream.serializer.base import AbstractSerializer


class ClusterManager:
    def __init__(
        self,
        mode: str,
        topic_metadata_cache: Dict[str, Dict[int, Any]],
        broker_info_cache: Dict[int, Any],
        broker_connections: Dict[Tuple[str, int], TinyStreamAPI],
        serializer: AbstractSerializer,
    ) -> None:
        self.mode = mode
        self._topic_metadata_cache = topic_metadata_cache
        self._broker_info_cache = broker_info_cache
        self._broker_connections = broker_connections
        self.serializer = serializer

    async def _refresh_cluster_metadata(self):
        raise NotImplementedError("This method should be implemented in the subclass.")

    async def get_leader_connection(
        self, topic: str, partition_id: int
    ) -> TinyStreamAPI:
        """
        Gets an active connection to the leader broker for a given partition.
        Handles cache misses by querying the controller.
        """
        if self.mode != "cluster":
            raise RuntimeError("Cannot get leader connection in single-broker mode.")

        topic_partitions = self._topic_metadata_cache.get(topic)
        if not topic_partitions:
            await self._refresh_cluster_metadata()
            topic_partitions = self._topic_metadata_cache.get(topic)
            if not topic_partitions:
                raise ValueError(f"Topic '{topic}' not found after refresh.")

        partition_info = topic_partitions.get(partition_id)
        if not partition_info:
            raise ValueError(f"Partition {partition_id} for topic '{topic}' not found.")

        leader_id = partition_info.get("leader")
        if leader_id is None:
            raise ConnectionError(f"No leader available for {topic}-{partition_id}.")

        broker_info = self._broker_info_cache.get(leader_id)
        if not broker_info:
            await self._refresh_cluster_metadata()
            broker_info = self._broker_info_cache.get(leader_id)
            if not broker_info:
                raise ValueError(f"Broker {leader_id} not found after refresh.")

        host, port = broker_info["host"], broker_info["port"]

        conn = self._broker_connections.get((host, port))
        if not conn or not conn.is_connected:
            print(
                f"Producer: Creating new connection to leader {leader_id} at {host}:{port}"
            )
            conn = TinyStreamAPI(host, port, serializer=self.serializer)
            self._broker_connections[(host, port)] = conn

        await conn.ensure_connected()
        return conn

    async def invalidate_caches(self, connection: Optional[TinyStreamAPI] = None):
        """Invalidates all metadata and optionally closes a bad connection."""
        if self.mode != "cluster":
            return

        print("Producer: Invalidating caches due to error.")

        self._topic_metadata_cache.clear()
        self._broker_info_cache.clear()

        if connection:
            conn_key = (connection.host, connection.port)
            if conn_key in self._broker_connections:
                await self._broker_connections.pop(conn_key).close()

        await self._refresh_cluster_metadata()
