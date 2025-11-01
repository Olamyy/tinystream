import sys
import asyncio
import httpx


class AdminClient:
    """
    Client for performing administrative actions on a TinyStream cluster.

    Connects to the Controller (or a single-mode Broker) via its HTTP API.
    """

    def __init__(self, metastore_api_address: str):
        if not metastore_api_address.startswith(("http://", "https://")):
            self.controller_addr = f"http://{metastore_api_address}"
        else:
            self.controller_addr = metastore_api_address

        self.api_base = f"{self.controller_addr}/api/v1/admin"

        self.http_client = httpx.AsyncClient(timeout=10.0)
        print(f"AdminClient initialized. Targeting controller at: {self.api_base}")

    @staticmethod
    async def _handle_response(response: httpx.Response, success_msg: str):
        """Helper to process HTTP responses and print user-friendly messages."""
        try:
            response.raise_for_status()
            data = response.json()
            print(f"Success: {data.get('message', success_msg)}")
            return data
        except httpx.HTTPStatusError as e:
            try:
                error_data = e.response.json()
                print(f"Error: {error_data}")
            except Exception:
                print(f"HTTP Error: {e.response.status_code} - {e.response.text}")
        except httpx.RequestError as e:
            print(f"Connection Error: Failed to connect to {e.request.url}.")
            print("Please ensure the Controller is running and the address is correct.")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

        return None

    async def create_topic(
        self,
        name: str,
        partitions: int,
        replication_factor: int,
        retention_ms: int = 604800000,
        retention_bytes: int = -1,
    ):
        """
        Sends a request to the controller to create a new topic.
        """
        print(
            f"Attempting to create topic '{name}' (P={partitions}, R={replication_factor})..."
        )
        endpoint = f"{self.api_base}/topics"
        payload = {
            "topic_name": name,
            "partition_count": partitions,
            "replication_factor": replication_factor,
            "retention_ms": retention_ms,
            "retention_bytes": retention_bytes,
        }

        try:
            response = await self.http_client.post(endpoint, json=payload)
            await self._handle_response(response, f"Topic '{name}' created.")
        except httpx.ConnectError:
            print(
                f"Connection Error: Could not connect to controller at {self.controller_addr}",
                file=sys.stderr,
            )
            print("Please ensure the controller is running.")
            sys.exit(1)

    async def list_topics(self):
        """
        Sends a request to the controller to list all topics.
        """
        print("Attempting to list topics...")
        endpoint = f"{self.api_base}/topics"

        response = await self.http_client.get(endpoint)
        data = await self._handle_response(response, "Fetched topic list.")

        if data and "topics" in data:
            topics = data["topics"]
            if not topics:
                print("No topics found in the cluster.")
                return

            print("\nTopics:")
            for topic_name, details in topics.items():
                part_count = details.get("partition_count", "?")
                print(f"  - {topic_name} ({part_count} partitions)")

    async def describe_cluster(self):
        """
        Sends a request to the controller for cluster status.
        """
        print("Attempting to describe cluster state...")
        endpoint = f"{self.api_base}/cluster"

        response = await self.http_client.get(endpoint)
        data = await self._handle_response(response, "Fetched cluster state.")

        if data and "brokers" in data:
            brokers = data["brokers"]
            if not brokers:
                print("No brokers registered with the controller.")
                return

            print("\nRegistered Brokers:")
            for broker_id, info in brokers.items():
                status = info.get("status")
                print(
                    f"  - Broker {broker_id} ({info.get('host')}:{info.get('port')}) - {status}"
                )

    async def close(self):
        """Closes the underlying HTTP client."""
        await self.http_client.aclose()


async def main():
    """
    Main function to parse CLI arguments and drive the AdminClient.
    """
    import argparse

    parser = argparse.ArgumentParser(description="TinyStream Admin Client")
    parser.add_argument(
        "--metastore",
        default="localhost:3200",
        help="Controller address (e.g., localhost:6000). (Default: %(default)s)",
    )

    subparsers = parser.add_subparsers(
        dest="action", required=True, help="Admin action to perform"
    )

    create_parser = subparsers.add_parser("create-topic", help="Create a new topic")
    create_parser.add_argument("--name", required=True, help="The name of the topic")
    create_parser.add_argument(
        "--partitions", required=True, type=int, help="Number of partitions"
    )
    create_parser.add_argument(
        "--replication-factor", required=True, type=int, help="Replication factor"
    )
    create_parser.add_argument(
        "--retention-ms",
        type=int,
        default=1800,
        help="Retention time in milliseconds",
    )
    create_parser.add_argument(
        "--retention-bytes", type=int, default=10000, help="Retention size in bytes"
    )

    subparsers.add_parser("list-topics", help="List all topics in the cluster")

    subparsers.add_parser("describe-cluster", help="Show cluster broker status")

    args = parser.parse_args()

    client = AdminClient(metastore_api_address=args.metastore)

    try:
        if args.action == "create-topic":
            await client.create_topic(
                name=args.name,
                partitions=args.partitions,
                replication_factor=args.replication_factor,
                retention_ms=args.retention_ms,
                retention_bytes=args.retention_bytes,
            )
        elif args.action == "list-topics":
            await client.list_topics()
        elif args.action == "describe-cluster":
            await client.describe_cluster()
        else:
            print(f"Unknown action: {args.action}")
            parser.print_help()
            sys.exit(1)

        await client.close()

    except Exception as e:
        print(f"\nAn unexpected critical error occurred: {e}")
        sys.exit(1)
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
