import unittest
import asyncio
import tempfile
from pathlib import Path

from tinystream.storage import SegmentedLogStorage


class TestFileLogStorage(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.temp_dir.name)

        self.storage = SegmentedLogStorage(partition_path=self.temp_path)

        asyncio.run(self.storage.ensure_ready())

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_storage_ensure_ready(self):
        new_log_dir = self.temp_path / "data"
        self.assertFalse(new_log_dir.exists())

        new_storage = SegmentedLogStorage(partition_path=new_log_dir)
        asyncio.run(new_storage.ensure_ready())

        self.assertTrue(new_log_dir.exists())

    def test_storage_append_and_read(self):
        async def _test():
            msg1 = b"hello world"
            msg2 = b"tinystream"

            offset1, bytes_written1 = await self.storage.append(msg1)
            self.assertEqual(offset1, 0)
            self.assertEqual(bytes_written1, self.storage.prefix_size + len(msg1))

            offset2, bytes_written2 = await self.storage.append(msg2)
            self.assertEqual(offset2, bytes_written1)
            self.assertEqual(bytes_written2, self.storage.prefix_size + len(msg2))

            read_msg1 = await self.storage.read_at(offset1)
            self.assertEqual(read_msg1, msg1)

            read_msg2 = await self.storage.read_at(offset2)
            self.assertEqual(read_msg2, msg2)

        asyncio.run(_test())

    def test_storage_get_current_offset(self):
        """Tests if the current offset (file size) is correct."""

        async def _test():
            self.assertEqual(await self.storage.get_current_offset(), 0)

            msg1 = b"message one"
            _, bytes_written1 = await self.storage.append(msg1)

            self.assertEqual(await self.storage.get_current_offset(), bytes_written1)

            msg2 = b"message two"
            _, bytes_written2 = await self.storage.append(msg2)

            total_size = bytes_written1 + bytes_written2
            self.assertEqual(await self.storage.get_current_offset(), total_size)

        asyncio.run(_test())

    def test_storage_replay(self):
        """Tests replaying the entire log from the beginning."""

        async def _test():
            messages = [b"first", b"second", b"third"]
            offsets = []

            for msg in messages:
                offset, _ = await self.storage.append(msg)
                offsets.append(offset)

            replayed_messages = []
            replayed_offsets = []
            async for offset, payload in self.storage.replay():
                replayed_offsets.append(offset)
                replayed_messages.append(payload)

            self.assertListEqual(replayed_messages, messages)
            self.assertListEqual(replayed_offsets, offsets)

        asyncio.run(_test())

    def test_storage_replay_empty(self):
        """Tests replaying an empty log file."""

        async def _test():
            count = 0
            async for _, _ in self.storage.replay():
                count += 1
            self.assertEqual(count, 0)

        asyncio.run(_test())

    def test_storage_read_at_offset_error(self):
        """Tests reading from an offset that doesn't exist."""

        async def _test():
            await self.storage.append(b"some data")

            # Try to read from an invalid offset
            with self.assertRaises(EOFError):
                await self.storage.read_at(9999)

        asyncio.run(_test())


if __name__ == "__main__":
    unittest.main()
