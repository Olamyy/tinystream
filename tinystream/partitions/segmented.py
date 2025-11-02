import asyncio
import time
from typing import Any, Optional

from tinystream.partitions.base import BasePartition
from tinystream.serializer.msg_pack import MSGPackSerializer
from tinystream.storage.base import AbstractLogStorage


class SegmentedLogPartition(BasePartition):
    def __init__(
        self,
        topic_name: str,
        partition_id: int,
        storage: AbstractLogStorage,
        serializer=None,
    ):
        super().__init__(topic_name, partition_id, storage, serializer)
        self.topic_name = topic_name
        self.partition_id = partition_id

        self.storage = storage
        self.serializer = serializer or MSGPackSerializer()

        self._lock = asyncio.Lock()

        self._next_logical_offset = 0

        self.role: str = "follower"
        self.retention_ms: Optional[int] = None
        self.retention_bytes: Optional[int] = None

    def update_policy(self, role: str, retention_ms: int, retention_bytes: int):
        """
        Updates the partition's live policy from the controller.
        (This logic is correct and remains unchanged)
        """
        self.role = role
        self.retention_ms = retention_ms
        self.retention_bytes = retention_bytes

    async def enforce_retention(self):
        """
        Deletes old segments based on the partition's policy.
        (This logic is correct and remains unchanged)
        """
        if self.retention_ms is None and self.retention_bytes is None:
            return

        inactive_segments = await self.storage.get_inactive_segments()

        if self.retention_ms is not None:
            now = time.time() * 1000
            cutoff_time = now - self.retention_ms

            for segment in list(inactive_segments):
                if segment.last_modified_timestamp < cutoff_time:
                    print(
                        f"[{self.topic_name}-{self.partition_id}] Deleting segment {segment.log_path.name} (Time Limit)"
                    )
                    await self.storage.delete_segment(segment)
                    inactive_segments.remove(segment)

        if self.retention_bytes is not None and self.retention_bytes > 0:
            total_size = await self.storage.get_total_size()

            segments_to_delete = sorted(inactive_segments, key=lambda s: s.base_offset)

            while total_size > self.retention_bytes:
                if not segments_to_delete:
                    break

                segment = segments_to_delete.pop(0)
                print(
                    f"[{self.topic_name}-{self.partition_id}] Deleting segment {segment.log_path.name} (Size Limit)"
                )
                await self.storage.delete_segment(segment)
                total_size -= segment.size

    async def load(self) -> None:
        """
        Loads the partition by initializing the storage and finding
        the next available logical offset.
        """
        async with self._lock:
            await self.storage.ensure_ready()

            print(f"Loading partition {self.topic_name}-{self.partition_id}...")
            message_count = 0
            try:
                print("Storage Path", self.storage.partition_path)
                async for _, _ in self.storage.replay():
                    message_count += 1
            except Exception as e:
                print(f"Error during log replay for HWM: {e}")

            self._next_logical_offset = message_count

            print(
                f"Loaded partition. Next offset (HWM) is {self._next_logical_offset}."
            )

    async def append(self, data: Any) -> int:
        """
        Appends a new message to the partition.
        Passes the logical offset to the storage layer for indexing.
        """
        serialized_data = self.serializer.serialize(data)

        async with self._lock:
            current_logical_offset = self._next_logical_offset

            self._next_logical_offset += 1

            await self.storage.append(current_logical_offset, serialized_data)

            return current_logical_offset

    async def read(self, logical_offset: int) -> Any:
        """
        Reads a message at a specific logical offset.
        Delegates the read (and index search) to the storage layer.
        """

        if logical_offset >= self._next_logical_offset:
            raise IndexError(
                f"Offset {logical_offset} out of range for partition {self.topic_name}-{self.partition_id}"
            )

        serialized_data = await self.storage.read(logical_offset)

        return self.serializer.deserialize(serialized_data)

    def get_high_watermark(self) -> int:
        """
        Returns the next available logical offset (i.e., total message count).
        (This is unchanged and correct)
        """
        return self._next_logical_offset
