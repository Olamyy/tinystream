import time
from dataclasses import dataclass
from typing import Literal, Optional, Dict, List
from pydantic import BaseModel, Field


@dataclass
class BrokerInfo:
    broker_id: int
    host: str
    port: int
    last_heartbeat: float = time.time()
    is_alive: bool = True
    status: Literal["ALIVE", "TIMED_OUT", "SHUTDOWN"] = "ALIVE"
    failed_since: Optional[float] = None


@dataclass
class PartitionMetadata:
    partition_id: int
    leader: Optional[int]
    replicas: List[int]


@dataclass
class TopicMetadata:
    name: str
    partitions: Dict[int, PartitionMetadata]
    retention_ms: int = 1800
    retention_bytes: int = 10000


class CreateTopicRequest(BaseModel):
    """
    JSON body for a POST /api/v1/admin/topics request.
    """

    topic_name: str = Field(..., description="The name of the new topic.")
    partition_count: int = Field(..., gt=0, description="Number of partitions.")
    replication_factor: int = Field(
        ..., gt=0, description="Replication factor (must be >= 1)."
    )
    retention_ms: Optional[int] = Field(
        gt=0, description="Retention time in ms.", default=1800
    )
    retention_bytes: Optional[int] = Field(
        gt=-1, description="Retention time in bytes.", default=10000
    )


class TopicInfo(BaseModel):
    """
    Response model for a single topic's details.
    """

    name: str
    partition_count: int
    retention_ms: int = 1800
    retention_bytes: int = 10000


class ListTopicsResponse(BaseModel):
    """
    Response for GET /api/v1/admin/topics
    """

    topics: dict[str, TopicInfo]


class ClusterInfoResponse(BaseModel):
    """
    Response for GET /api/v1/admin/cluster
    """

    brokers: dict[int, BrokerInfo]
