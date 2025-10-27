from tinystream.serializer.base import AbstractSerializer


def init_serializer(serializer_name: str) -> AbstractSerializer:
    if serializer_name == "messagepack":
        from tinystream.serializer.msg_pack import MSGPackSerializer

        return MSGPackSerializer()
    else:
        raise ValueError(f"Unknown serializer: {serializer_name}")
