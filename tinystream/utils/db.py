async def create_db_schemas(connection):
    """
    Creates all tables for both Controller and Broker.
    Uses 'IF NOT EXISTS' to be idempotent.
    """

    await connection.execute(
        """
        CREATE TABLE IF NOT EXISTS brokers
        (
            broker_id INTEGER PRIMARY KEY,
            host      TEXT    NOT NULL,
            port      INTEGER NOT NULL
        )
        """
    )

    await connection.execute(
        """
        CREATE TABLE IF NOT EXISTS topics
        (
            topic_name         TEXT PRIMARY KEY,
            partition_count    INTEGER NOT NULL,
            replication_factor INTEGER NOT NULL
        )
        """
    )

    await connection.execute(
        """
        CREATE TABLE IF NOT EXISTS partitions
        (
            topic_name   TEXT    NOT NULL,
            partition_id INTEGER NOT NULL,
            leader       INTEGER,
            replicas     TEXT    NOT NULL,
            FOREIGN KEY (topic_name) REFERENCES topics (topic_name),
            PRIMARY KEY (topic_name, partition_id)
        )
        """
    )

    await connection.execute(
        """
        CREATE TABLE IF NOT EXISTS consumer_group_offsets
        (
            group_id         TEXT    NOT NULL,
            topic_name       TEXT    NOT NULL,
            partition_id     INTEGER NOT NULL,
            committed_offset INTEGER NOT NULL,
            PRIMARY KEY (group_id, topic_name, partition_id)
        )
        """
    )
