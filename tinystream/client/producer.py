import hashlib
from typing import Any, Optional, Dict

from tinystream.client.connection import TinyStreamAPI
from tinystream.serializer.msg_pack import MSGPackSerializer


class Producer:
    """
    It manages a connection to the broker and provides a simple
    `send` method.
    """

    def __init__(
        self, broker_host: str = "localhost", broker_port: int = 9092, serializer=None
    ) -> None:
        self.host = broker_host
        self.port = broker_port
        self._connection = TinyStreamAPI(
            self.host, self.port, serializer=serializer or MSGPackSerializer()
        )

        # TODO: This should be dynamic from the broker
        self._partition_count = 1

    async def connect(self) -> None:
        """Explicitly connects to the broker."""
        await self._connection.ensure_connected()

    async def close(self) -> None:
        """Closes the connection to the broker."""
        await self._connection.close()

    def _get_partition(self, key: Optional[bytes]) -> int:
        """
        Determines the partition for a given key.
        If no key, defaults to partition 0.
        """
        if key is None:
            # For now, just send to partition 0 if no key
            return 0

        # Simple hash-based partitioning
        hash_bytes = hashlib.md5(key).digest()
        hash_int = int.from_bytes(hash_bytes, "little")

        # In v0.1, we only have one partition (0)
        # In a future version, we'd do:
        # return hash_int % self._partition_count
        return hash_int % self._partition_count

    async def send(
        self, topic: str, data: Any, key: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Sends a message to a topic.

        Args:
            topic: The name of the topic.
            data: The message payload (any msgpack-serializable object).
            key: An optional key (str). If provided, ensures all messages
                 with the same key go to the same partition.

        Returns:
            The response from the broker (e.g., {"status": "ok", "offset": 0}).
        """
        key_bytes = key.encode("utf-8") if key else None
        partition_id = self._get_partition(key_bytes)

        request = {
            "command": "append",
            "topic": topic,
            "partition": partition_id,
            "data": data,
        }

        try:
            response = await self._connection.send_request(request)
            return response
        except ConnectionError as e:
            print(f"Failed to send message: {e}")
            return {"status": "error", "message": str(e)}
