from abc import ABC, abstractmethod
from typing import Any


class AbstractSerializer(ABC):
    @staticmethod
    @abstractmethod
    def serialize(data: Any) -> bytes:
        pass

    @staticmethod
    @abstractmethod
    def deserialize(data: bytes) -> Any:
        pass
