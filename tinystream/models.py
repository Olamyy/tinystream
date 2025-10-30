import time
from dataclasses import dataclass
from typing import Literal, Optional, Dict, List


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

