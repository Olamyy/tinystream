from abc import ABC


class BasePartition(ABC):
    """Base class for all partition implementations."""

    def __init__(
        self,
        topic_name: str,
        partition_id: int,
        storage=None,
        serializer=None,
    ):
        self.topic_name = topic_name
        self.partition_id = partition_id
        self.storage = storage
        self.serializer = serializer

    async def load(self):
        """Loads the partition data."""
        raise NotImplementedError

    async def append(self, data):
        """Appends data to the partition."""
        raise NotImplementedError

    async def read(self, logical_offset):
        """Reads data from the partition at the given logical offset."""
        raise NotImplementedError

    def get_high_watermark(self):
        """Returns the high watermark (next write offset) of the partition."""
        raise NotImplementedError

    def update_policy(self, role: str, retention_ms: int, retention_bytes: int):
        """Updates the retention policy of the partition."""
        raise NotImplementedError

    async def enforce_retention(self):
        """Enforces the retention policy by deleting old messages."""
        raise NotImplementedError

    async def close(self):
        """Closes the partition and releases resources."""
        raise NotImplementedError
