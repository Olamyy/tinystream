import asyncio
import time
from typing import Any, List, Optional

from tinystream.partitions.base import BasePartition
from tinystream.serializer.msg_pack import MSGPackSerializer
from tinystream.storage import SingleLogStorage


class SingleLogPartition(BasePartition):
    def __init__(
        self,
        topic_name: str,
        partition_id: int,
        storage: SingleLogStorage,
        serializer=None,
    ):
        self.topic_name = topic_name
        self.partition_id = partition_id

        self.serializer = serializer or MSGPackSerializer()
        self.storage = storage

        super().__init__(
            topic_name=self.topic_name,
            partition_id=self.partition_id,
            storage=self.storage,
            serializer=self.serializer,
        )

        self._index: List[int] = []
        self._lock = asyncio.Lock()
        self._next_logical_offset = 0
        self.role: str = "follower"
        self.retention_ms: Optional[int] = None
        self.retention_bytes: Optional[int] = None

    def update_policy(self, role: str, retention_ms: int, retention_bytes: int):
        """
        Updates the partition's live policy from the controller.
        """
        self.role = role
        self.retention_ms = retention_ms
        self.retention_bytes = retention_bytes

    async def enforce_retention(self):
        if self.retention_ms is None and self.retention_bytes is None:
            return

        inactive_segments = await self.storage.get_inactive_segments()

        if self.retention_ms is not None:
            print("Checking time retention:", self.retention_ms)
            now = time.time() * 1000
            cutoff_time = now - self.retention_ms
            print("[Time Retention] Current time (ms):", cutoff_time)

            for segment in inactive_segments:
                print(
                    "Segment:",
                    segment.log_path.name,
                    "Last Modified:",
                    segment.last_modified_timestamp,
                )
                if segment.last_modified_timestamp < cutoff_time:
                    print(
                        f"[{self.topic_name}-{self.partition_id}] Deleting segment {segment.log_path.name} (Time Limit)"
                    )
                    await self.storage.delete_segment(segment)

        if self.retention_bytes is not None and self.retention_bytes > 0:
            print("Checking size retention:", self.retention_bytes)
            total_size = await self.storage.get_total_size()
            print("[Size Retention] Current total size:", total_size)

            segments_to_delete = sorted(inactive_segments, key=lambda s: s.base_offset)

            while total_size > self.retention_bytes:
                if not segments_to_delete:
                    break

                segment = segments_to_delete.pop(0)
                print(
                    f"[{self.topic_name}-{self.partition_id}] Deleting segment {segment.name} (Size Limit)"
                )
                await self.storage.delete_segment(segment)
                total_size -= segment.size

    async def load(self) -> None:
        async with self._lock:
            await self.storage.ensure_ready()

            print(f"Loading partition {self.topic_name}-{self.partition_id}...")
            self._index = []

            print("index_index_index", self._index)

            async for physical_offset, _ in self.storage.replay():
                print("physical_offset:", physical_offset, "_:", _)
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
            physical_offset, _ = await self.storage.append(
                data=serialized_data,
                logical_offset=None,
            )

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
