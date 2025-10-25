import aiofiles
import os
from typing import AsyncGenerator, Tuple, Literal
from pathlib import Path
from storage.base import AbstractLogStorage


class FileLogStorage(AbstractLogStorage):
    """
    The log file format is a sequence of records:
    [ 8-byte length ][ N-byte payload ]
    """

    def __init__(
        self,
        log_file_path: Path,
        prefix_size: int = 8,
        byte_order: Literal["little"] = "little",
    ):
        super().__init__(log_file_path=log_file_path)
        self.prefix_size = prefix_size
        self.byte_order = byte_order
        self.log_file = log_file_path
        self._log_dir = log_file_path.parent
        self._write_lock = None

    async def _get_lock(self):
        if self._write_lock is None:
            import asyncio

            self._write_lock = asyncio.Lock()
        return self._write_lock

    async def ensure_ready(self) -> None:
        if not os.path.exists(self._log_dir):
            os.makedirs(self._log_dir, exist_ok=True)

    async def append(self, data: bytes) -> Tuple[int, int]:
        """
        Appends data to the log file with an 8-byte length prefix.
        Format: [ 8-byte length ][ N-byte payload ]
        """
        lock = await self._get_lock()
        async with lock:
            payload_len = len(data)
            len_prefix = payload_len.to_bytes(self.prefix_size, self.byte_order)

            async with aiofiles.open(self.log_file, "ab") as f:
                offset = await f.tell()

                await f.write(len_prefix)
                await f.write(data)

                total_bytes = self.prefix_size + payload_len
                return offset, total_bytes

    async def read_at(self, offset: int) -> bytes:
        """
        Reads a single message payload starting at a specific offset.
        """
        async with aiofiles.open(self.log_file, "rb") as f:
            await f.seek(offset)

            len_prefix_bytes = await f.read(self.prefix_size)
            if not len_prefix_bytes:
                raise EOFError(
                    "Reached end of file while trying to read length prefix."
                )

            payload_len = int.from_bytes(len_prefix_bytes, self.byte_order)

            payload = await f.read(payload_len)
            if len(payload) != payload_len:
                raise IOError(
                    f"Log file corrupted. Expected {payload_len} bytes, got {len(payload)}."
                )

            return payload

    async def replay(self) -> AsyncGenerator[Tuple[int, bytes], None]:  # type: ignore
        """
        AsyncGenerator
                Reads and yields all messages from the log file.
        """
        try:
            async with aiofiles.open(self.log_file, "rb") as f:
                while True:
                    current_offset = await f.tell()

                    len_prefix_bytes = await f.read(self.prefix_size)
                    if not len_prefix_bytes:
                        break

                    payload_len = int.from_bytes(len_prefix_bytes, self.byte_order)

                    payload = await f.read(payload_len)
                    if len(payload) != payload_len:
                        print(
                            f"Warning: Log file may be truncated. Expected {payload_len}, got {len(payload)}."
                        )
                        break

                    yield current_offset, payload
        except FileNotFoundError:
            pass

    async def get_current_offset(self) -> int:
        """Gets the current size of the log file."""
        try:
            return os.path.getsize(self.log_file)
        except FileNotFoundError:
            return 0
