from abc import ABC


class BasePartition(ABC):
    """Base class for all partition implementations."""

    pass

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
