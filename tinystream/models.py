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


@dataclass
class PartitionMetadata:
    partition_id: int
    leader: Optional[int]
    replicas: List[int]


@dataclass
class TopicMetadata:
    name: str
    partitions: Dict[int, PartitionMetadata]


class CreateTopicRequest(BaseModel):
    """
    JSON body for a POST /api/v1/admin/topics request.
    """

    topic_name: str = Field(..., description="The name of the new topic.")
    partition_count: int = Field(..., gt=0, description="Number of partitions.")
    replication_factor: int = Field(
        ..., gt=0, description="Replication factor (must be >= 1)."
    )


class TopicInfo(BaseModel):
    """
    Response model for a single topic's details.
    """

    name: str
    partition_count: int


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
