import logging 
import os
import json
import requests
import time

from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
COINGEKO_API_URL = os.getenv("COINGEKO_BASE_URL", "https://api.coingecko.com/api/v3")
TOPIC = "market_metadata"
POLL_INTERVAL = 60

COIN_IDS = {
    "BTCUSDT": "bitcoin",
    "ETHUSDT": "ethereum",
    "SOLUSDT": "solana",
}



def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        key_serializer=lambda x: x.encode("utf-8"),
        acks="all",
        retries=5,
    )

def fetch_market_metadata(coin_ids : list[str]) -> list[dict]:
    url = f"{COINGEKO_API_URL}/coins/markets"

    params = {
        "vs_currency": "usd",
        "ids": ",".join(coin_ids),
        "order": "market_cap_desc",
        "per_page": 10,
        "page": 1,
        "sparkline": False,
        "price_change_percentage": "1h,24h,7d",
    }

    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (compatible; MarketMetadataProducer/1.0; +https://example.com/bot)"
    }

    resp = requests.get(url, params=params, timeout=10, headers=headers)
    resp.raise_for_status()
    return resp.json()

def normalize_metadata(raw : dict, symbol: str) -> dict:
    return {
        "symbol": symbol,
        "coin_id": raw["id"],
        "name": raw["name"],
        "current_price": raw["current_price"],
        "market_cap": raw["market_cap"],
        "market_cap_rank": raw["market_cap_rank"],
        "total_volume_24h": raw["total_volume"],
        "high_24h": raw["high_24h"],
        "low_24h": raw["low_24h"],
        "price_change_percentage_1h_in_currency": raw.get("price_change_percentage_1h_in_currency"),
        "price_change_percentage_24h_in_currency": raw.get("price_change_percentage_24h_in_currency"),
        "price_change_percentage_7d_in_currency": raw.get("price_change_percentage_7d_in_currency"),
        "circulating_supply": raw["circulating_supply"],
        "polled_at": int(time.time() * 1000),
    }

def main() -> None:
    producer = make_producer()
    id_to_symbol = {v : k for k, v in COIN_IDS.items()}

    while True:
        try :
            rows = fetch_market_metadata(list(COIN_IDS.values()))
            for row in rows:
                symbol = id_to_symbol.get(row["id"])
                if not symbol:
                    continue
                record = normalize_metadata(row,symbol)
                producer.send(TOPIC, key=symbol, value=record)
                log.info("produced metadata for %s: price=$%s", symbol, record["current_price"])
            producer.flush()

        except requests.HTTPError as exc:
            log.error("HTTP error while fetching metadata: %s", exc, exc_info=True)
        except Exception as exc:
            log.error("Unexpected error: %s", exc, exc_info=True)
        
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()


