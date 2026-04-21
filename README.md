# Crypto Market Intelligence Pipeline

A real-time streaming pipeline that ingests live cryptocurrency trade data,
enriches it with technical indicators (VWAP, RSI), detects volume anomalies,
and serves a live Grafana dashboard — all running locally with Docker.

## Architecture

```
Binance WebSocket ──┐
CoinGecko REST ─────┤──► Kafka ──► PySpark Structured Streaming ──► TimescaleDB ──► Grafana
Reddit API ─────────┘                      │                             │
                                           └──────────────────────► Redis ──► FastAPI
```

## Stack

| Layer       | Technology                          |
|-------------|-------------------------------------|
| Ingest      | Binance WebSocket, CoinGecko, PRAW  |
| Streaming   | Apache Kafka, PySpark 3.5           |
| Storage     | TimescaleDB (Postgres 15), Redis 7  |
| Orchestrate | Apache Airflow 2.9                  |
| Serve       | FastAPI, Grafana 10                 |

## Quick Start

### 1. Prerequisites

- Docker Desktop (4 GB RAM allocated minimum)
- Python 3.11+
- Git

### 2. Clone and configure

```bash
git clone <your-repo>
cd crypto-pipeline
cp .env.example .env   # edit with your Reddit credentials
```

### 3. Start all services

```bash
make up
```

Services will be available at:

| Service   | URL                        | Credentials      |
|-----------|----------------------------|------------------|
| Kafka UI  | http://localhost:8080      | —                |
| Spark UI  | http://localhost:8081      | —                |
| Grafana   | http://localhost:3000      | admin / admin    |
| Airflow   | http://localhost:8085      | admin / admin    |
| API docs  | http://localhost:8000/docs | —                |

### 4. Start the producers

```bash
# In separate terminals:
cd ingestion
pip install -r requirements.txt

python binance_producer.py      # live BTC/ETH/SOL trades → Kafka
python coingecko_producer.py    # market metadata → Kafka
python sentiment_producer.py    # Reddit posts → Kafka (needs .env creds)
```

### 5. Submit the Spark jobs

```bash
make spark-submit JOB=trades_enricher   # OHLCV + VWAP → Postgres + Redis
make spark-submit JOB=anomaly_detector  # Z-score anomaly detection
```

### 6. Open Grafana

Visit http://localhost:3000 → Dashboards → Crypto → **Live Trading**

The dashboard auto-refreshes every 5 seconds.

## API Usage

```bash
# Latest indicators from Redis (fast)
curl http://localhost:8000/indicators/BTCUSDT

# Historical OHLCV candles (last 6 hours, 5-min interval)
curl "http://localhost:8000/trades/BTCUSDT/ohlcv?interval=5min&hours=6"

# Recent anomaly events
curl "http://localhost:8000/trades/BTCUSDT/anomalies?hours=24&min_score=3.5"

# Live WebSocket feed
websocat ws://localhost:8000/ws/live/BTCUSDT
```

## Development

```bash
# Install dev dependencies
pip install -r requirements.txt

# Run unit tests (no Docker needed)
make test

# Lint and format
make lint
make format

# Watch Kafka messages
make kafka-consume TOPIC=raw_trades

# Open Postgres shell
make psql

# Open Redis CLI
make redis-cli
```

## Project Structure

```
crypto-pipeline/
├── ingestion/           # Kafka producers (Binance, CoinGecko, Reddit)
├── streaming/
│   ├── spark_jobs/      # PySpark streaming jobs
│   └── schemas/         # Shared data models
├── storage/
│   ├── migrations/      # SQL schema (auto-runs on Postgres start)
│   └── redis_cache.py   # Redis helper
├── orchestration/
│   └── dags/            # Airflow DAGs (quality checks, alerts, retrain)
├── serving/
│   ├── api/             # FastAPI app
│   └── grafana/         # Dashboard JSON + provisioning
└── tests/               # Unit + integration tests
```

## Key Concepts

**Watermarking** — Spark uses a 10-second watermark on the event timestamp to handle
late-arriving messages. Events older than the watermark are dropped from state.

**Continuous Aggregates** — TimescaleDB pre-computes 5-minute OHLCV candles in the
background. Grafana queries these views instead of the raw table, making dashboards
10-100x faster.

**Redis TTL** — Indicator cache keys expire after 120 seconds. If the Spark job stops,
the API returns a 503 rather than serving stale data.

**Z-score anomaly detection** — A volume spike is flagged when it exceeds 3 standard
deviations above the 30-minute rolling mean for that symbol.

## Troubleshooting

**Kafka topics not created** — Run `make kafka-topics` to list topics. If missing,
re-run the kafka-init container: `docker compose up kafka-init`.

**Spark job fails to connect to Kafka** — Ensure `KAFKA_BOOTSTRAP_SERVERS=kafka:29092`
inside the Spark container (use the internal Docker hostname, not `localhost`).

**Grafana shows "No data"** — Confirm the Spark enricher job is running and has
processed at least one micro-batch (check Spark UI at localhost:8081).
