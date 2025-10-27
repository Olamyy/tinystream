import asyncio
import pathlib
from typing import Literal, Optional, Dict, Any
from aiosqlite import Connection as DBConnection

import aiosqlite

from tinystream.utils.db import create_db_schemas


class BaseAsyncClient:
    def __init__(
        self,
        prefix_size=8,
        byte_order: Literal["little", "big"] = "little",
        serializer=None,
        host: str = "localhost",
        port: int = 9093,
    ):
        self.prefix_size = prefix_size
        self.byte_order = byte_order
        self.serializer = serializer
        self._server: Optional[asyncio.Server] = None
        self.host = host
        self.port = port
        self.db_connection: Optional[DBConnection] = None

    async def start_server(self):
        self._server = await asyncio.start_server(
            self.handle_client_connection, self.host, self.port
        )

    async def close_server(self):
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    async def ping(self):
        return self._server.is_serving()

    async def send_request(self, request_data: bytes) -> Dict[str, Any]:
        raise NotImplementedError()

    async def handle_client_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        peer = writer.get_extra_info("peername")
        print(f"[Server] New connection from {peer}")
        try:
            while True:
                len_prefix_bytes = await reader.readexactly(self.prefix_size)
                if not len_prefix_bytes:
                    break

                payload_len = int.from_bytes(len_prefix_bytes, self.byte_order)
                payload_bytes = await reader.readexactly(payload_len)

                response = await self.send_request(payload_bytes)

                response_bytes = self.serializer.serialize(response)

                response_len_prefix = len(response_bytes).to_bytes(
                    self.prefix_size, self.byte_order
                )
                writer.write(response_len_prefix)
                writer.write(response_bytes)
                await writer.drain()

        except asyncio.IncompleteReadError:
            print(f"[Server] Client {peer} disconnected unexpectedly.")

        except Exception as e:
            print(f"[Server] FATAL error handling client {peer}: {e}")
            try:
                error_response = {
                    "status": "error",
                    "message": f"Fatal server error: {e}",
                }
                response_bytes = self.serializer.serialize(error_response)
                response_len_prefix = len(response_bytes).to_bytes(
                    self.prefix_size, self.byte_order
                )
                writer.write(response_len_prefix)
                writer.write(response_bytes)
                await writer.drain()
            except Exception as e2:
                print(f"[Server] Could not send error response to {peer}: {e2}")

        finally:
            print(f"[Server] Closing connection from {peer}")
            writer.close()
            await writer.wait_closed()

    async def init_metastore(self, db_path: pathlib.Path):
        print(f"Initializing metastore at {db_path}...")
        try:
            db_path = pathlib.Path(db_path).resolve()
            db_path.parent.mkdir(parents=True, exist_ok=True)
            self.db_connection = await aiosqlite.connect(db_path)
            await self.db_connection.execute("PRAGMA journal_mode=WAL;")
            await create_db_schemas(connection=self.db_connection)
            await self.db_connection.commit()
            print("[Controller] Metastore tables initialized.")
        except Exception as e:
            print(f"[Controller] FATAL: Could not initialize metastore: {e}")
            raise
