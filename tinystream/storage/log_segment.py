import aiofiles
import os
import asyncio
import bisect
from typing import AsyncGenerator, Tuple, Literal, List
from pathlib import Path


class LogSegment:
    """
    Manages a single pair of log/index files (e.g., 000000.log, 000000.index).
    """

    INDEX_ENTRY_SIZE = 16

    def __init__(
        self,
        partition_path: Path,
        base_offset: int,
        prefix_size: int,
        byte_order: Literal["little", "big"],
        index_interval_bytes: int = 4096,
    ):
        self.partition_path = partition_path
        self.base_offset = base_offset
        self.prefix_size = prefix_size
        self.byte_order = byte_order
        self.index_interval_bytes = index_interval_bytes

        filename_base = f"{base_offset:020d}"
        self.log_path = self.partition_path / f"{filename_base}.log"
        self.index_path = self.partition_path / f"{filename_base}.index"

        self._write_lock = asyncio.Lock()
        self.size = 0
        self.last_modified_timestamp = 0.0

        self.index_entries: List[Tuple[int, int]] = []
        self._bytes_since_last_index = 0

    async def load(self) -> None:
        """Loads segment state and reads the .index file into memory."""
        self.partition_path.mkdir(parents=True, exist_ok=True)
        try:
            async with aiofiles.open(self.log_path, "xb"):
                pass
            async with aiofiles.open(self.index_path, "xb"):
                pass
        except FileExistsError:
            pass

        self.size = os.path.getsize(self.log_path)
        self.last_modified_timestamp = os.path.getmtime(self.log_path) * 1000

        try:
            async with aiofiles.open(self.index_path, "rb") as f:
                while True:
                    entry_bytes = await f.read(self.INDEX_ENTRY_SIZE)
                    if len(entry_bytes) == 0:
                        break
                    if len(entry_bytes) < self.INDEX_ENTRY_SIZE:
                        print(f"Warning: Corrupted index file {self.index_path.name}")
                        break

                    offset = int.from_bytes(entry_bytes[0:8], self.byte_order)
                    pos = int.from_bytes(entry_bytes[8:16], self.byte_order)
                    self.index_entries.append((offset, pos))
        except FileNotFoundError:
            pass

        if self.index_entries:
            self._bytes_since_last_index = self.size - self.index_entries[-1][1]
        else:
            self._bytes_since_last_index = self.size

    async def _write_to_index(self, logical_offset: int, byte_position: int):
        """Appends a new entry to the .index file and in-memory cache."""
        self.index_entries.append((logical_offset, byte_position))
        entry_bytes = logical_offset.to_bytes(
            8, self.byte_order
        ) + byte_position.to_bytes(8, self.byte_order)
        async with aiofiles.open(self.index_path, "ab") as f:
            await f.write(entry_bytes)

    async def append(self, logical_offset: int, data: bytes) -> int:
        """
        Appends data to this segment with a length prefix.
        Returns total bytes written.
        NOTE: Signature has changed.
        """
        async with self._write_lock:
            payload_len = len(data)
            len_prefix = payload_len.to_bytes(self.prefix_size, self.byte_order)
            total_bytes = self.prefix_size + payload_len

            async with aiofiles.open(self.log_path, "ab") as f:
                file_offset = await f.tell()

                if (
                    not self.index_entries
                    or self._bytes_since_last_index > self.index_interval_bytes
                ):
                    await self._write_to_index(logical_offset, file_offset)
                    self._bytes_since_last_index = 0
                else:
                    self._bytes_since_last_index += total_bytes

                await f.write(len_prefix)
                await f.write(data)

                self.size += total_bytes
                return total_bytes

    async def _find_position_from_index(self, target_offset: int) -> int:
        """Finds the *byte position* to start scanning from for a logical offset."""
        if not self.index_entries:
            return 0

        idx = bisect.bisect_right(self.index_entries, (target_offset, float("inf")))

        if idx == 0:
            return 0

        _, byte_position = self.index_entries[idx - 1]
        return byte_position

    async def read(self, logical_offset_to_find: int) -> bytes:
        """
        Reads a single message payload by LOGICAL offset.
        Uses the index to perform a fast, sparse scan.
        NOTE: Replaces read_at().
        """
        start_position = await self._find_position_from_index(logical_offset_to_find)

        async with aiofiles.open(self.log_path, "rb") as f:
            await f.seek(start_position)

            logical_offset_counter = -1
            if self.index_entries:
                idx = bisect.bisect_right(
                    self.index_entries, (logical_offset_to_find, float("inf"))
                )
                if idx > 0:
                    logical_offset_counter = self.index_entries[idx - 1][0]

            while True:
                len_prefix_bytes = await f.read(self.prefix_size)
                if not len_prefix_bytes:
                    break

                payload_len = int.from_bytes(len_prefix_bytes, self.byte_order)

                # Need to infer the logical offset.
                # This assumes offsets are sequential (e.g., 100, 101, 102)
                # This is a placeholder for a real implementation that
                # would store the offset *in the log message*
                if logical_offset_counter != -1:
                    logical_offset_counter += 1

                if logical_offset_counter == logical_offset_to_find:
                    payload = await f.read(payload_len)
                    if len(payload) != payload_len:
                        raise IOError("Log file corrupted.")
                    return payload
                else:
                    await f.seek(payload_len, 1)

        raise IndexError(f"Offset {logical_offset_to_find} not found in segment.")

    async def replay(self) -> AsyncGenerator[Tuple[int, bytes], None]:  # type: ignore
        """Reads and yields all messages from this segment."""
        print("Warning: Replay is not accurate without offsets in log")
        logical_offset_counter = self.base_offset
        try:
            async with aiofiles.open(self.log_path, "rb") as f:
                while True:
                    len_prefix_bytes = await f.read(self.prefix_size)
                    if not len_prefix_bytes:
                        break
                    payload_len = int.from_bytes(len_prefix_bytes, self.byte_order)
                    payload = await f.read(payload_len)
                    if len(payload) != payload_len:
                        break

                    yield logical_offset_counter, payload
                    logical_offset_counter += 1
        except FileNotFoundError:
            pass

    async def get_current_offset(self) -> int:
        """Gets the current end of the file (the next write offset)."""
        async with self._write_lock:
            return self.base_offset + self.size

    async def delete_files(self):
        """Deletes the .log and .index files for this segment."""
        print(f"Deleting segment: {self.log_path.name}")
        try:
            os.remove(self.log_path)
        except OSError as e:
            print(f"Error deleting {self.log_path}: {e}")
        try:
            os.remove(self.index_path)
        except OSError as e:
            print(f"Error deleting {self.index_path}: {e}")
