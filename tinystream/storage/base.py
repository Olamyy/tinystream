from abc import ABC, abstractmethod
from typing import AsyncGenerator, Tuple, Literal, Optional
from pathlib import Path


class AbstractLogStorage(ABC):
    def __init__(
        self,
        partition_path: Path,
        prefix_size: int = 8,
        byte_order: Literal["little", "big"] = "little",
        max_segment_bytes: int = 16 * 1024 * 1024,
    ):
        self.partition_path = partition_path
        self.prefix_size = prefix_size
        self.byte_order = byte_order
        self.max_segment_bytes = max_segment_bytes

    @abstractmethod
    async def ensure_ready(self) -> None:
        pass

    @abstractmethod
    async def append(
        self, logical_offset: Optional[int], data: bytes
    ) -> Tuple[int, int]:
        pass

    @abstractmethod
    async def read_at(self, logical_offset: int) -> bytes:
        pass

    @abstractmethod
    async def read(self, offset: int) -> bytes:
        pass

    @abstractmethod
    async def replay(self) -> AsyncGenerator[Tuple[int, bytes], None]:
        pass

    @abstractmethod
    async def get_current_offset(self) -> int:
        pass

    @abstractmethod
    async def get_inactive_segments(self):
        pass

    @abstractmethod
    async def delete_segment(self, segment_index: int) -> None:
        pass

    @abstractmethod
    async def get_total_size(self) -> int:
        pass
