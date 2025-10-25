from abc import ABC, abstractmethod
from typing import AsyncGenerator, Tuple
from pathlib import Path


class AbstractLogStorage(ABC):
    def __init__(self, log_file_path: Path):
        self.log_file = log_file_path

    @abstractmethod
    async def ensure_ready(self) -> None:
        pass

    @abstractmethod
    async def append(self, data: bytes) -> Tuple[int, int]:
        pass

    @abstractmethod
    async def read_at(self, offset: int) -> bytes:
        pass

    @abstractmethod
    async def replay(self) -> AsyncGenerator[Tuple[int, bytes], None]:
        pass

    @abstractmethod
    async def get_current_offset(self) -> int:
        pass
