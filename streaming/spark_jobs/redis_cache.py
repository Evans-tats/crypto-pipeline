
from typing import Optional
import redis
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

REDIS_URL = "redis://redis:6379"
TTL = 120
class IndicatorCache:
    def __init__(self, redis_url : str = REDIS_URL):
        self.client = redis.from_url(redis_url, decode_responses=True)
    
    def write_indicators(self, symbol: str, data:dict) -> None:
        key = f"indicators:{symbol}"
        self.client.hset(key,mapping={k: str(v) for k, v in data.items()})
        self.client.expire(key, TTL)
    
    def read_indicators(self, symbol: str) -> Optional[dict]:
        key = f"indicators:{symbol}"
        if self.client.exists(key):
            return self.client.hgetall(key)
        return None
    def ping(self) -> bool:
        try:
            return self.client.ping()
        except redis.RedisError:
            return False
# def update_caache_from_batch(batch_df,batch_id : int) -> None:
#     cache = IndicatorCache()

#     if not cache.ping():
#         log.warning("Cannot connect to Redis. Skipping cache update for batch %d", batch_id)
#         return
    
#     rows = batch_df.collect()

#     for row in rows:
#         d = row.asDict()
#         symbol = d.pop["symbol"]
#         cache.write_indicators(symbol, d)
    
#     log.info("Updated cache for batch %d with %d records", batch_id, len(rows))
