import json
from typing import Dict
from tinystream.models import PartitionMetadata, TopicMetadata, BrokerInfo


class TopicManager:
    def __init__(
        self,
        db_connection,
        brokers: Dict[int, BrokerInfo],
        topics: Dict[str, TopicMetadata],
        lock,
    ):
        self.db_connection = db_connection
        self.brokers = brokers
        self.topics = topics
        self._lock = lock

    async def create_topic(
        self,
        name: str,
        partitions: int,
        replication_factor: int,
        retention_ms: int = 1800,
        retention_bytes: int = 10000,
    ):
        if not self.db_connection:
            raise Exception("Database not connected")

        brokers_alive = [b.broker_id for b in self.brokers.values() if b.is_alive]
        if len(brokers_alive) < replication_factor:
            raise ValueError(
                f"Not enough brokers alive ({len(brokers_alive)}) "
                f"to satisfy replication factor ({replication_factor})."
            )

        new_topic_metadata = TopicMetadata(
            name=name,
            partitions={},
            retention_ms=retention_ms,
            retention_bytes=retention_bytes,
        )
        partition_data_to_insert = []

        for partition_id in range(partitions):
            replicas = []
            for i in range(replication_factor):
                broker_index = (partition_id + i) % len(brokers_alive)
                replicas.append(brokers_alive[broker_index])

            leader = replicas[0]
            replicas_json = json.dumps(replicas)

            partition_metadata = PartitionMetadata(
                partition_id=partition_id, leader=leader, replicas=replicas
            )
            new_topic_metadata.partitions[partition_id] = partition_metadata

            partition_data_to_insert.append((name, partition_id, leader, replicas_json))

        async with self._lock:
            if name in self.topics:
                raise ValueError(f"Topic {name} already exists.")

            try:
                await self.db_connection.execute(
                    """
                    INSERT INTO topics (topic_name, partition_count, replication_factor,
                                        retention_ms, retention_bytes)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        name,
                        partitions,
                        replication_factor,
                        retention_ms,
                        retention_bytes,
                    ),
                )

                await self.db_connection.executemany(
                    "INSERT INTO partitions (topic_name, partition_id, leader, replicas) VALUES (?, ?, ?, ?)",
                    partition_data_to_insert,
                )

                await self.db_connection.commit()
                print(f"[Controller] Persisted topic {name} and its partitions to DB.")

            except Exception as e:
                print(f"FATAL: Could not persist topic {name}: {e}")
                raise

            self.topics[name] = new_topic_metadata
            print(f"[Controller] Topic {name} is now live.")
