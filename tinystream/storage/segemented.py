import asyncio
from pathlib import Path
from typing import List, Literal, Optional, AsyncGenerator, Tuple

from tinystream.storage.base import AbstractLogStorage
from tinystream.storage.log_segment import LogSegment


class SegmentedLogStorage(AbstractLogStorage):
    """
    Manages a directory of log segments for a single partition.
    """

    def __init__(
        self,
        partition_path: Path,
        prefix_size: int = 8,
        byte_order: Literal["little", "big"] = "little",
        max_segment_bytes: int = 16 * 1024 * 1024,
        index_interval_bytes: int = 4096,
    ):
        self.partition_path = partition_path
        self.prefix_size = prefix_size
        self.byte_order = byte_order
        self.max_segment_bytes = max_segment_bytes
        self.index_interval_bytes = index_interval_bytes

        super().__init__(
            partition_path=self.partition_path,
            prefix_size=self.prefix_size,
            byte_order=self.byte_order,
            max_segment_bytes=self.max_segment_bytes,
        )

        self.segments: List[LogSegment] = []
        self.active_segment: Optional[LogSegment] = None
        self._roll_lock = asyncio.Lock()

    async def ensure_ready(self) -> None:
        """Scans the directory for .log files and loads them as segments."""
        self.partition_path.mkdir(parents=True, exist_ok=True)
        log_files = sorted(self.partition_path.glob("*.log"))

        if not log_files:
            await self._roll_segment(base_offset=0)
            return

        for log_path in log_files:
            try:
                base_offset = int(log_path.stem)
                segment = LogSegment(
                    self.partition_path,
                    base_offset,
                    self.prefix_size,
                    self.byte_order,
                    self.index_interval_bytes,
                )
                await segment.load()
                self.segments.append(segment)
            except ValueError:
                print(f"Skipping unknown file: {log_path.name}")

        self.active_segment = self.segments[-1]
        print(
            f"Loaded {len(self.segments)} segments. Active: {self.active_segment.log_path.name}"
        )

    async def _roll_segment(self, base_offset: int):
        """Closes the current segment and starts a new one."""
        new_segment = LogSegment(
            self.partition_path,
            base_offset,
            self.prefix_size,
            self.byte_order,
            self.index_interval_bytes,
        )
        await new_segment.load()

        self.segments.append(new_segment)
        self.active_segment = new_segment
        print(f"Rolled to new segment: {new_segment.log_path.name}")

    async def append(self, logical_offset: int, data: bytes) -> int:
        """
        Appends data to the active segment, rolling if necessary.
        NOTE: Signature has changed to accept logical_offset.
        Returns total bytes written.
        """
        if not self.active_segment:
            raise Exception("Storage not initialized. Call ensure_ready().")

        if self.active_segment.size > self.max_segment_bytes:
            async with self._roll_lock:
                if self.active_segment.size > self.max_segment_bytes:
                    await self._roll_segment(base_offset=logical_offset)

        return await self.active_segment.append(logical_offset, data)

    async def read(self, logical_offset: int) -> bytes:
        """
        Finds the correct segment and reads by logical offset.
        NOTE: Replaces read_at().
        """
        segment_to_read = None
        for segment in reversed(self.segments):
            if logical_offset >= segment.base_offset:
                segment_to_read = segment
                break

        if not segment_to_read:
            raise IndexError(f"Offset {logical_offset} is before the first segment.")

        return await segment_to_read.read(logical_offset)

    async def replay(self) -> AsyncGenerator[Tuple[int, bytes], None]:  # type: ignore
        """Reads and yields all messages from all segments, in order."""
        for segment in self.segments:
            async for logical_offset, payload in segment.replay():
                yield logical_offset, payload

    async def get_current_offset(self) -> int:
        """Gets the next logical offset."""
        # This is a problem. The storage layer doesn't know the
        # next logical offset, only the next byte offset.
        # This needs to be managed by the Partition class.
        # For now, we return the base offset of the next segment.
        if not self.active_segment:
            return 0
        return await self.active_segment.get_current_offset()

    async def get_inactive_segments(self) -> List[LogSegment]:
        if not self.active_segment:
            return []
        return self.segments[:-1]

    async def get_total_size(self) -> int:
        return sum(s.size for s in self.segments)

    async def delete_segment(self, segment: LogSegment):
        await segment.delete_files()
        try:
            self.segments.remove(segment)
        except ValueError:
            print(f"Warning: Segment {segment.log_path.name} not in list.")

    async def read_at(self, logical_offset: int) -> bytes:
        ...
