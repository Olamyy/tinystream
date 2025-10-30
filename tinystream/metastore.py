import uvicorn
import asyncio
from fastapi import FastAPI, HTTPException
from typing import Dict

from tinystream.models import (
    CreateTopicRequest,
    ListTopicsResponse,
    ClusterInfoResponse,
    TopicInfo,
)
from tinystream.client.topic_manager import TopicManager
from tinystream.models import BrokerInfo, TopicMetadata


class Metastore:
    """
    Hosts the Admin REST API server using FastAPI.

    This object is given references to the live cluster state
    from the Controller (or single-mode Broker) that owns it.
    """

    def __init__(
        self,
        topic_manager: TopicManager,
        topics: Dict[str, TopicMetadata],
        brokers: Dict[int, BrokerInfo],
        lock: asyncio.Lock,
        host: str = "0.0.0.0",
        port: int = 6000,
    ):
        self.topic_manager = topic_manager
        self.topics = topics
        self.brokers = brokers
        self._lock = lock
        self.host = host
        self.port = port

        self.api_app = FastAPI(
            title="TinyStream Metastore API",
            description="Admin endpoints for managing the TinyStream cluster.",
        )
        self.api_server = None
        self._setup_api_routes()

    def _setup_api_routes(self):
        """Attaches this class's methods to the FastAPI app routes."""
        self.api_app.post("/api/v1/admin/topics", status_code=201)(
            self._api_create_topic
        )
        self.api_app.get("/api/v1/admin/topics", response_model=ListTopicsResponse)(
            self._api_list_topics
        )
        self.api_app.get("/api/v1/admin/cluster", response_model=ClusterInfoResponse)(
            self._api_describe_cluster
        )

    async def start(self):
        """Starts the uvicorn server as an async task."""
        print(f"[MetastoreAPI] Starting server on http://{self.host}:{self.port}")

        config = uvicorn.Config(
            app=self.api_app,
            host=self.host,
            port=self.port,
            loop="asyncio",
            log_level="info",
        )
        self.api_server = uvicorn.Server(config)

        try:
            await self.api_server.serve()
        except asyncio.CancelledError:
            print("[MetastoreAPI] Server task cancelled.")
        finally:
            print("[MetastoreAPI] Server has shut down.")

    async def close(self):
        """Signals the Uvicorn server to shut down."""
        if self.api_server:
            print("[MetastoreAPI] Shutting down server...")
            await self.api_server.shutdown()

    async def _api_create_topic(self, request: CreateTopicRequest):
        try:
            await self.topic_manager.create_topic(
                request.topic_name, request.partition_count, request.replication_factor
            )
            return {
                "status": "success",
                "message": f"Topic '{request.topic_name}' created.",
            }
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            print(f"FATAL: _api_create_topic failed: {e}")
            raise HTTPException(status_code=500, detail="Internal server error.")

    async def _api_list_topics(self) -> ListTopicsResponse:
        async with self._lock:
            response_topics = {}
            for topic_name, meta in self.topics.items():
                response_topics[topic_name] = TopicInfo(
                    name=topic_name,
                    partition_count=len(meta.partitions),
                )
            return ListTopicsResponse(topics=response_topics)

    async def _api_describe_cluster(self) -> ClusterInfoResponse:
        async with self._lock:
            response_brokers = {}
            for broker_id, info in self.brokers.items():
                response_brokers[broker_id] = BrokerInfo(
                    broker_id=info.broker_id,
                    host=info.host,
                    port=info.port,
                    is_alive=info.is_alive,
                )
            return ClusterInfoResponse(brokers=response_brokers)
