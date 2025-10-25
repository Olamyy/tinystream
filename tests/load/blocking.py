import socket
import time
import uuid
import random
from typing import Dict, Any, Literal

import msgpack
from locust import User, task, between

BROKER_HOST = "localhost"
BROKER_PORT = 9092
TEST_TOPIC = "locust_topic"
TEST_PARTITION = 0

LEN_PREFIX_SIZE = 8
BYTE_ORDER: Literal["little"] = "little"


class BlockingTinyStreamClient:
    def __init__(self, host, port, user_events):
        self.host = host
        self.port = port
        self.events = user_events
        self._serializer = msgpack.Packer(use_bin_type=True)
        self._unserializer = msgpack.Unpacker(raw=False)

        try:
            self.socket = socket.create_connection((self.host, self.port), timeout=5)
        except Exception as e:
            raise ConnectionError(f"Failed to connect to {self.host}:{self.port} - {e}")

    def send_request(self, command: str, **kwargs) -> Dict[str, Any]:
        request_name = command
        start_time = time.monotonic()

        try:
            request = {"command": command, **kwargs}
            payload = self._serializer.pack(request)

            len_prefix = len(payload).to_bytes(LEN_PREFIX_SIZE, BYTE_ORDER)
            self.socket.sendall(len_prefix + payload)

            resp_len_bytes = self.socket.recv(LEN_PREFIX_SIZE)
            if not resp_len_bytes:
                raise IOError("Connection closed by server (reading length)")

            resp_len = int.from_bytes(resp_len_bytes, BYTE_ORDER)

            chunks = []
            bytes_received = 0
            while bytes_received < resp_len:
                chunk = self.socket.recv(min(resp_len - bytes_received, 4096))
                if not chunk:
                    raise IOError("Connection closed by server (reading payload)")
                chunks.append(chunk)
                bytes_received += len(chunk)

            resp_payload = b"".join(chunks)
            self._unserializer.feed(resp_payload)
            response = self._unserializer.unpack()

            elapsed_ms = (time.monotonic() - start_time) * 1000

            if response.get("status") == "ok":
                self.events.request.fire(
                    request_type="tinystream",
                    name=request_name,
                    response_time=elapsed_ms,
                    response_length=len(resp_payload),
                )
                return response
            else:
                raise Exception(response.get("message", "Unknown error"))

        except Exception as e:
            elapsed_ms = (time.monotonic() - start_time) * 1000
            self.events.request.fire(
                request_type="tinystream",
                name=request_name,
                response_time=elapsed_ms,
                response_length=0,
                exception=e,
            )
            raise

    def close(self):
        self.socket.close()


class ProducerUser(User):
    wait_time = between(0.1, 0.5)

    def on_start(self):
        self.client = BlockingTinyStreamClient(
            BROKER_HOST, BROKER_PORT, self.environment.events
        )
        self.users = ["alice", "bob", "carlos", "denise"]

    def on_stop(self):
        self.client.close()

    @task
    def append_message(self):
        user = random.choice(self.users)
        msg = {"user": user, "action": "click", "payload": str(uuid.uuid4())}
        try:
            self.client.send_request(
                "append", topic=TEST_TOPIC, partition=TEST_PARTITION, data=msg
            )
        except Exception as e:
            print(f"Producer failed to append: {e}")


class ConsumerUser(User):
    wait_time = between(0.5, 2.0)

    def on_start(self):
        self.client = BlockingTinyStreamClient(
            BROKER_HOST, BROKER_PORT, self.environment.events
        )
        self.offset = 0
        try:
            resp = self.client.send_request(
                "get_hwm", topic=TEST_TOPIC, partition=TEST_PARTITION
            )
            self.offset = resp.get("high_watermark", 0)
            print(f"Consumer starting at offset {self.offset}")
        except Exception as e:
            print(f"Consumer failed to get HWM: {e}")
            self.stop()

    def on_stop(self):
        self.client.close()

    @task
    def read_message(self):
        try:
            hwm_resp = self.client.send_request(
                "get_hwm", topic=TEST_TOPIC, partition=TEST_PARTITION
            )
            hwm = hwm_resp.get("high_watermark", 0)

            if self.offset < hwm:
                read_resp = self.client.send_request(
                    "read",
                    topic=TEST_TOPIC,
                    partition=TEST_PARTITION,
                    offset=self.offset,
                )
                if read_resp.get("status") == "ok":
                    self.offset += 1
                else:
                    self.offset = hwm
            else:
                pass

        except Exception as e:
            print(f"Consumer failed to read: {e}")
