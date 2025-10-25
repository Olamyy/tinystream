import asyncio
from typing import Any, List
from pathlib import Path

from tinystream.partitions.base import BasePartition
from tinystream.serializer.msg_pack import MSGPackSerializer
from tinystream.storage.storage import FileLogStorage


class SingleLogPartition(BasePartition):
    def __init__(
        self,
        topic_name: str,
        partition_id: int,
        base_log_dir: Path,
        storage=None,
        serializer=None,
    ):
        self.topic_name = topic_name
        self.partition_id = partition_id

        log_file = base_log_dir / topic_name / f"{partition_id}.log"
        self.storage = storage or FileLogStorage(log_file_path=log_file)
        self.serializer = serializer or MSGPackSerializer()

        # The core of the partition: mapping logical offsets to physical offsets.
        # index[0] = physical offset of the 1st message
        # index[1] = physical offset of the 2nd message
        self._index: List[int] = []
        self._lock = asyncio.Lock()
        self._next_logical_offset = 0

    async def load(self) -> None:
        """
        Loads the partition by replaying its log file to rebuild
        the in-memory offset index. This must be called before
        the partition can be used.
        """
        async with self._lock:
            await self.storage.ensure_ready()

            print(f"Loading partition {self.topic_name}-{self.partition_id}...")
            self._index = []

            async for physical_offset, _ in self.storage.replay():
                self._index.append(physical_offset)

            self._next_logical_offset = len(self._index)
            print(
                f"Loaded {self._next_logical_offset} messages into index for {self.topic_name}-{self.partition_id}."
            )

    async def append(self, data: Any) -> int:
        """
        Appends a new message to the partition.

        Args:
            data: The Python object to append.

        Returns:
            The logical offset (e.g., 0, 1, 2...) of the appended message.
        """
        serialized_data = self.serializer.serialize(data)

        async with self._lock:
            physical_offset, _ = await self.storage.append(serialized_data)

            self._index.append(physical_offset)

            current_logical_offset = self._next_logical_offset
            self._next_logical_offset += 1

            return current_logical_offset

    async def read(self, logical_offset: int) -> Any:
        """
        Reads a message at a specific logical offset.

        Args:
            logical_offset: The logical offset (0, 1, 2...) to read.

        Returns:
            The deserialized Python object.

        Raises:
            IndexError: If the logical offset is out of bounds.
        """
        physical_offset: int
        try:
            physical_offset = self._index[logical_offset]
        except IndexError:
            raise IndexError(
                f"Offset {logical_offset} out of range for partition {self.topic_name}-{self.partition_id}"
            )

        serialized_data = await self.storage.read_at(physical_offset)

        return self.serializer.deserialize(serialized_data)

    def get_high_watermark(self) -> int:
        """
        Returns the next available logical offset (i.e., total message count).
        """
        return self._next_logical_offset
