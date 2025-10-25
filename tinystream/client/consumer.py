from typing import Any, Dict, List, Tuple

from tinystream.client.connection import TinyStreamAPI
from tinystream.serializer.msg_pack import MSGPackSerializer


class Consumer:
    """
    A stateful consumer that tracks its own offsets for assigned partitions.
    """

    def __init__(
        self, host: str = "localhost", port: int = 9092, serializer: Any = None
    ) -> None:
        self.host = host
        self.port = port
        self._connection = TinyStreamAPI(
            host=self.host,
            port=self.port,
            serializer=serializer or MSGPackSerializer(),
            prefix_size=8,
            byte_order="little",
        )

        # Key improvement:
        # Stores the *next offset* to read for each partition.
        # Format: { (topic, partition_id): next_offset }
        self._assignments: Dict[Tuple[str, int], int] = {}

        # Caches high-watermarks to avoid polling empty partitions
        self._hwms: Dict[Tuple[str, int], int] = {}

    async def connect(self) -> None:
        await self._connection.ensure_connected()

    async def close(self) -> None:
        await self._connection.close()

    def assign(self, topic: str, partition: int = 0, start_offset: int = 0) -> None:
        """
        Assigns this consumer to a specific partition, starting from a given
        offset. Call this *before* polling.

        Args:
            topic: The topic to read from.
            partition: The partition to read from.
            start_offset: The logical offset to start reading from (e.g., 0).
        """
        key = (topic, partition)
        self._assignments[key] = start_offset
        self._hwms[key] = 0
        print(f"Consumer assigned to {topic}-{partition} at offset {start_offset}")

    async def _update_high_watermarks(self) -> None:
        """
        Asks the broker for the latest high-watermark (HWM) for all
        assigned partitions. This tells us what the last available message offset is.
        """
        for topic, part in self._assignments.keys():
            try:
                resp = await self._connection.send_request(
                    {"command": "get_hwm", "topic": topic, "partition": part}
                )
                if resp.get("status") == "ok":
                    self._hwms[(topic, part)] = resp["high_watermark"]
            except Exception as e:
                print(f"Failed to get HWM for {topic}-{part}: {e}")

    async def poll(self, max_messages: int = 100) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []

        # 1. First, find out what messages are available
        await self._update_high_watermarks()

        # 2. Keep fetching round-robin until batch is full or we run out
        while len(results) < max_messages:
            messages_polled_this_round = 0

            for (topic, part), next_offset in self._assignments.items():
                if len(results) >= max_messages:
                    break

                hwm = self._hwms.get((topic, part), next_offset)

                # 3. If our offset is less than HWM, data is available
                if next_offset < hwm:
                    try:
                        # 4. Send the read request for the *specific* offset
                        resp = await self._connection.send_request(
                            {
                                "command": "read",
                                "topic": topic,
                                "partition": part,
                                "offset": next_offset,
                            }
                        )

                        if resp.get("status") == "ok":
                            results.append(resp["data"])
                            # 5. CRITICAL: Update our internal state
                            self._assignments[(topic, part)] = next_offset + 1
                            messages_polled_this_round += 1
                        else:
                            # Error (e.g., offset deleted?), stop polling this partition
                            print(
                                f"Read error on {topic}-{part} at {next_offset}: {resp.get('message')}"
                            )
                            self._hwms[(topic, part)] = 0  # Invalidate HWM cache

                    except Exception as e:
                        print(f"Failed to read from {topic}-{part}: {e}")
                        self._hwms[(topic, part)] = 0  # Invalidate HWM

            # If we went through all partitions and got nothing, we're caught up.
            if messages_polled_this_round == 0:
                break

        return results
