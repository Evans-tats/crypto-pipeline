import asyncio
import asyncpg
import logging

from typing import AsyncGenerator
import redis.asyncio as redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from contextlib import asynccontextmanager

from routers import indicators

# Configuration
POSTGRES_DSN = "postgresql://crypto:crypto_secret@postgres:5432/crypto_db"
REDIS_DSN = "redis://redis:6379"

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    app.state.pg_pool = await asyncpg.create_pool(POSTGRES_DSN, min_size=2, max_size=10)
    app.state.redis = redis.from_url(REDIS_DSN,decode_responses=True)
    logger.info("Connected to PostgreSQL and Redis")
    yield
    await app.state.pg_pool.close()
    await app.state.redis.close()

app = FastAPI(
    title= "Crypto Pipelines API",
    description= "real -time crypto maeket data and trading pipelines API",
    version= "1.0.0",
    lifespan = lifespan

)

app.include_router(indicators.router, prefix="/indicators", tags=["indicators"])

