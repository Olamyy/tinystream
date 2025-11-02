import aiofiles
import os
from typing import AsyncGenerator, Tuple, Literal, List, Any, Optional
from pathlib import Path
from tinystream.storage.base import AbstractLogStorage


class SingleLogStorage(AbstractLogStorage):
    """
    Manages a single log file for a partition.
    Format: [ 8-byte length ][ N-byte payload ]

    NOTE: This storage class DOES NOT support retention policies,
    as it cannot delete old data without destroying the entire log.
    """

    def __init__(
        self,
        partition_path: Path,
        prefix_size: int = 8,
        byte_order: Literal["little"] = "little",
        max_segment_bytes: int = 0,
    ):
        super().__init__(
            partition_path=partition_path,
            prefix_size=prefix_size,
            byte_order=byte_order,
            max_segment_bytes=max_segment_bytes,
        )
        self.prefix_size = prefix_size
        self.byte_order = byte_order
        self._write_lock = None

    async def _get_lock(self):
        if self._write_lock is None:
            import asyncio

            self._write_lock = asyncio.Lock()
        return self._write_lock

    async def ensure_ready(self) -> None:
        parent_dir = self.partition_path.parent
        if not os.path.exists(parent_dir):
            os.makedirs(parent_dir, exist_ok=True)

    async def append(
        self, logical_offset: Optional[int], data: bytes
    ) -> Tuple[int, int]:
        """
        Appends data to the log file with an 8-byte length prefix.
        Returns (physical_offset, bytes_written)
        """
        lock = await self._get_lock()
        async with lock:
            payload_len = len(data)
            len_prefix = payload_len.to_bytes(self.prefix_size, self.byte_order)

            async with aiofiles.open(self.partition_path, "ab") as f:
                offset = await f.tell()

                await f.write(len_prefix)
                await f.write(data)

                total_bytes = self.prefix_size + payload_len
                return offset, total_bytes

    async def read_at(self, offset: int) -> bytes:
        """
        Reads a single message payload starting at a specific physical offset.
        """
        async with aiofiles.open(self.partition_path, "rb") as f:
            await f.seek(offset)

            len_prefix_bytes = await f.read(self.prefix_size)
            if not len_prefix_bytes:
                raise EOFError("Reached end of file.")

            payload_len = int.from_bytes(len_prefix_bytes, self.byte_order)

            payload = await f.read(payload_len)
            if len(payload) != payload_len:
                raise IOError("Log file corrupted.")

            return payload

    async def replay(self) -> AsyncGenerator[Tuple[int, bytes], None]:  # type: ignore
        """Reads and yields all messages from the log file."""
        try:
            async with aiofiles.open(self.partition_path, "rb") as f:
                while True:
                    current_offset = await f.tell()

                    len_prefix_bytes = await f.read(self.prefix_size)
                    if not len_prefix_bytes:
                        break

                    payload_len = int.from_bytes(len_prefix_bytes, self.byte_order)
                    payload = await f.read(payload_len)

                    if len(payload) != payload_len:
                        print("Warning: Log file may be truncated.")
                        break

                    yield current_offset, payload
        except FileNotFoundError:
            pass

    async def get_current_offset(self) -> int:
        """Gets the current size of the log file."""
        try:
            return os.path.getsize(self.partition_path)
        except FileNotFoundError:
            return 0

    async def get_inactive_segments(self) -> List:
        return []

    async def get_total_size(self) -> int:
        return await self.get_current_offset()

    async def delete_segment(self, segment: Any):
        ...

    async def read(self, offset: int) -> bytes:
        ...
