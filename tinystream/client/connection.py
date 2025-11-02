import asyncio
from typing import Any, Dict, Optional, Literal


class TinyStreamAPI:
    """
    Manages a single, durable connection to a TinyStream broker.

    It handles:
    - Connecting and reconnecting.
    - Serializing requests and deserializing responses.
    - Implementing the [Length][Payload] network protocol.
    - A lock to ensure only one request/response cycle happens at a time
      over the single connection.
    """

    def __init__(
        self,
        host: str,
        port: int,
        serializer=None,
        prefix_size: int = 8,
        byte_order: Literal["little", "big"] = "little",
    ):
        self.host = host
        self.port = port
        self.prefix_size = prefix_size
        self.byte_order = byte_order
        self.serializer = serializer

        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None

        self._lock = asyncio.Lock()
        self._connect_lock = asyncio.Lock()
        self.is_connected = False

    async def _connect(self) -> None:
        """Establishes a connection to the controller."""
        # Use a lock to prevent multiple coroutines from trying to
        # connect at the same time.
        async with self._connect_lock:
            if self.is_connected:
                return

            print(f"Connecting to broker at {self.host}:{self.port}...")
            try:
                self._reader, self._writer = await asyncio.open_connection(
                    self.host, self.port
                )
                self.is_connected = True
                print("Connection successful.")
            except (OSError, ConnectionRefusedError) as e:
                print(f"Failed to connect to controller: {e}")
                self.is_connected = False
                raise

    async def ensure_connected(self) -> None:
        """Public method to ensure connection is active before a request."""
        if not self.is_connected:
            await self._connect()

    async def close(self) -> None:
        """Closes the connection."""
        if self._writer:
            print("Closing connection...")
            self._writer.close()
            await self._writer.wait_closed()
        self._reader = None
        self._writer = None
        self.is_connected = False

    async def send_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Sends a request to the broker and waits for a response.
        This method is thread-safe (async-safe).
        """
        async with self._lock:
            try:
                await self.ensure_connected()

                if not self._writer or not self._reader:
                    raise ConnectionError("Broker is not connected.")

                payload_bytes = self.serializer.serialize(request)
                len_prefix = len(payload_bytes).to_bytes(
                    self.prefix_size, self.byte_order
                )

                self._writer.write(len_prefix)
                self._writer.write(payload_bytes)
                await self._writer.drain()

                resp_len_prefix = await self._reader.readexactly(self.prefix_size)
                resp_payload_len = int.from_bytes(resp_len_prefix, self.byte_order)

                resp_payload = await self._reader.readexactly(resp_payload_len)

                response = self.serializer.deserialize(resp_payload)
                return response

            except (asyncio.IncompleteReadError, ConnectionResetError) as e:
                print(
                    f"Connection lost: {e}. Attempting to reconnect on next call. Request data: {request}"
                )
                await self.close()
                raise ConnectionError("Connection lost while processing request.")
            except Exception as e:
                print(f"An error occurred: {e}")
                await self.close()
                raise
