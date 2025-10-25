from typing import Any
import msgpack

from tinystream.serializer.base import AbstractSerializer


class MSGPackSerializer(AbstractSerializer):
    @staticmethod
    def serialize(data: Any) -> bytes:
        return msgpack.packb(data, use_bin_type=True)

    @staticmethod
    def deserialize(data: bytes) -> Any:
        return msgpack.unpackb(data, raw=False)
