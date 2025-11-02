import asyncio
import time
import argparse
import random
import string
import uuid
import sys
from typing import Optional, List

from tinystream.config.manager import ConfigManager
from tinystream.client.producer import Producer
from tinystream.utils.env import env_default


def _random_payload(size_bytes: int) -> str:
    """Generates a random string payload of a given size."""
    return "".join(random.choices(string.ascii_letters + string.digits, k=size_bytes))


async def producer_worker(
    producer: Producer,
    topic: str,
    msg_size: int,
    batch_size: int,
    stats_queue: asyncio.Queue,
):
    """
    A single async worker that sends batches of messages in a loop.
    """
    print(f"[Worker {asyncio.current_task().get_name()}] starting...")
    while True:
        try:
            tasks = []
            for _ in range(batch_size):
                key = str(uuid.uuid4())
                payload = _random_payload(msg_size)
                tasks.append(producer.send(topic, payload, key=key))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            sent_count = 0
            error_count = 0

            for res in results:
                if isinstance(res, Exception):
                    error_count += 1
                else:
                    sent_count += 1

            await stats_queue.put(("sent", sent_count))
            if error_count > 0:
                await stats_queue.put(("errors", error_count))

        except asyncio.CancelledError:
            return


async def stats_reporter(stats_queue: asyncio.Queue):
    """
    A background task that collects and prints throughput stats.
    """
    total_sent = 0
    total_errors = 0
    start_time = time.monotonic()

    async def print_stats():
        while True:
            await asyncio.sleep(5)
            elapsed = time.monotonic() - start_time
            if elapsed == 0:
                continue

            msgs_sec = total_sent / elapsed

            print("---")
            print(f"  Total Sent: {total_sent}")
            print(f"Total Errors: {total_errors}")
            print(f"  Throughput: {msgs_sec:.2f} msgs/sec")
            print("---")

    reporter_print_task = asyncio.create_task(print_stats())

    try:
        while True:
            event_type, count = await stats_queue.get()
            if event_type == "sent":
                total_sent += count
            elif event_type == "errors":
                total_errors += count
            stats_queue.task_done()
    except asyncio.CancelledError:
        reporter_print_task.cancel()
        await asyncio.gather(reporter_print_task, return_exceptions=True)


async def main(config: ConfigManager, args: argparse.Namespace):
    producer = Producer(config=config)
    stats_queue = asyncio.Queue()
    worker_tasks: List[asyncio.Task] = []
    reporter_task: Optional[asyncio.Task] = None

    try:
        await producer.connect()
        print(f"Producer connected. Starting {args.num_workers} workers...")

        reporter_task = asyncio.create_task(stats_reporter(stats_queue))

        for i in range(args.num_workers):
            worker_tasks.append(
                asyncio.create_task(
                    producer_worker(
                        producer,
                        args.topic,
                        args.message_size,
                        args.batch_size,
                        stats_queue,
                    ),
                    name=f"worker-{i}",
                )
            )

        main_work = asyncio.gather(reporter_task, *worker_tasks)

        if args.run_time:
            print(f"--- Running test for {args.run_time} seconds ---")
            await asyncio.wait_for(main_work, timeout=args.run_time)
        else:
            print("--- Running test indefinitely (Press Ctrl+C to stop) ---")
            await main_work

    except asyncio.TimeoutError:
        print(f"\n\nTest duration of {args.run_time} seconds complete. Stopping...")
    except ConnectionRefusedError:
        print("\n[ERROR] Could not connect to controller.")
    except KeyboardInterrupt:
        print("\n\nStopping load test... (Ctrl+C pressed)")
    except Exception as e:
        print(f"\nAn error occurred: {e}")
    finally:
        print("Shutting down workers...")
        if reporter_task:
            reporter_task.cancel()
        for task in worker_tasks:
            task.cancel()

        all_tasks = worker_tasks + ([reporter_task] if reporter_task else [])
        await asyncio.gather(*all_tasks, return_exceptions=True)
        print("All tasks stopped.")

        if await producer.is_connected():
            await producer.close()
            print("Producer connection closed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TinyStream Load Test")

    parser.add_argument(
        "--controller-uri",
        type=str,
        default=env_default("TINYSTREAM_CONTROLLER_URI")(),
        help="Controller RPC URI (e.g., localhost:9093). Overrides config.",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=env_default("TINYSTREAM_CONFIG")(),
        help="Path to a user config file. Overrides default config.",
    )

    parser.add_argument(
        "--topic", type=str, default="load_test", help="Topic to produce to."
    )
    parser.add_argument(
        "--num-workers",
        type=int,
        default=50,
        help="Number of concurrent producer tasks to run.",
    )
    parser.add_argument(
        "--message-size",
        type=int,
        default=1024,
        help="Size of each message payload in bytes.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of messages each worker sends in a concurrent batch.",
    )

    parser.add_argument(
        "--run-time",
        type=int,
        default=1200,
        help="Duration to run the test in seconds. (Default: run indefinitely)",
    )

    args = parser.parse_args()

    config_manager = ConfigManager(args, component_type="broker")

    print("Starting load test...")
    print(f"           Topic: {args.topic}")
    print(f"         Workers: {args.num_workers}")
    print(f"     Batch Size: {args.batch_size}")
    print(f"    Message Size: {args.message_size} bytes")
    print(f"        Run Time: {args.run_time or 'Indefinite'}")
    print(
        f"Controller URI: {config_manager.controller_config['host']}:{config_manager.controller_config['port']}"
    )
    print("---")

    try:
        asyncio.run(main(config=config_manager, args=args))
    except Exception as e:
        print(f"FATAL: Load test crashed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
