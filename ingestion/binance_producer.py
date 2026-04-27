import websockets
import asyncio
import json
import time
import os
import random
import logging

from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)


BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"
KAFKA_BOOTSTRAP_SERVERS =os.getenv("KAFKA_BOOTSTRAP_SERVERS","localhost:9092")
SYMBOLS = ["btcusdt", "ethusdt", "bnbusdt"]
KAFKA_TOPIC = "raw_trades"

def producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        api_version=(3, 0, 0),
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        key_serializer=lambda x: x.encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=10,
        metadata_max_age_ms=30000
    )

def parse_trade(raw : dict) -> dict:
    return {
        "symbol": raw["s"],
        "trade_id": raw["t"],
        "price": float(raw["p"]),
        "quantity": float(raw["q"]),
        "buyer_market_maker": raw["m"],
        "trade_time": raw["T"],
        "event_time": raw["E"],
        "ingestion_time": int(time.time() * 1000),
    }

async def stream_symbol(symbol: str, producer: KafkaProducer):
    ws_url = f"{BINANCE_WS_URL}/{symbol.lower()}@trade"
    backoff = 1
    while True:
        try:
            log.info("connecting to %s", ws_url)
            async with websockets.connect(ws_url, ping_interval=20) as ws:
                backoff = 1
                async for raw_msg in ws:
                    data = json.loads(raw_msg)
                    if data.get("e") != "trade":
                        continue
                    trade = parse_trade(data)
                    producer.send(KAFKA_TOPIC, 
                                  key=trade["symbol"], 
                                  value=trade)
                    log.info("Produced trade for %s: %s", trade["symbol"], trade["price"])
                    
        except (websockets.ConnectionClosed, OSError) as exc:
            log.warning("Connection lost for %s: %s. Retrying in %d seconds", symbol, exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)  
        except Exception as exc:
            log.error("Unexpected error for %s :%s", symbol,exc, exc_info=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)  
    

async def main() -> None:
    producer_instance = producer()
    
    try:
        await asyncio.gather(*(stream_symbol(symbol, producer_instance) for symbol in SYMBOLS))
    finally:
        producer_instance.flush()
        producer_instance.close()

if __name__ == "__main__":
    asyncio.run(main())

    