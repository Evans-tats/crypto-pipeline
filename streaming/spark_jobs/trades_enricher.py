import os 
import time

from pyspark.sql import SparkSession,DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import psycopg2

import logging
from redis_cache import IndicatorCache


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

TRADE_SCHEMA = """
    symbol STRING,
    trade_id LONG,
    price DOUBLE,
    quantity DOUBLE,
    buyer_market_maker BOOLEAN,
    trade_time LONG,
    event_time LONG,
    ingestion_time LONG
"""


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:29092")
POSTGRES_URL = os.getenv("POSTGRES_URL","jdbc:postgresql://postgres:5432/crypto_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "crypto")
POSTGRES_DB = os.getenv("POSTGRES_DB", "crypto_db")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "crypto_secret")
CHEKPOINT_DIR = "/tmp/checkpoint/trades_enricher"
POSTGRES_HOST = POSTGRES_URL.split("//")[1].split(":")[0]


def create_spark()  -> SparkSession:
    return (
        SparkSession.builder.appName("TradesEnricher")
        .config("spark.sql.streaming.checkpointLocation", CHEKPOINT_DIR)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

def read_trades(spark: SparkSession) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", "raw_trades")
        .option("startingOffsets", "latest")
        .load()
    )

    return (
        raw.select(F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("trade"))
        .select("trade.*")
        .withColumn("event_ts", (F.col("trade_time") / 1000).cast("timestamp"))
        .withWatermark("event_ts", "10 seconds")
    )

def compute_ohlcv_vwap(df : DataFrame) -> DataFrame:
    return(
        df.groupBy(
            F.window("event_ts", "1 minute").alias("w"),
            "symbol"
        )
        .agg(
            F.first("price").alias("open_price"),
            F.max("price").alias("high_price"),
            F.min("price").alias("low_price"),
            F.last("price").alias("close_price"),
            F.sum("quantity").alias("volume"),
            F.count("*").alias("trade_count"),
            (F.sum(F.col("price") * F.col("quantity")) / F.sum("quantity")).alias("vwap"),
        
        ).select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "symbol",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "trade_count",
            "vwap"
        )
    )


def write_to_postgres(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        print(f"-> Batch {batch_id} is empty. Waiting for more data...")
        return

    staging_table = f"temp_staging_{batch_id}_{int(time.time())}"

    # 1. Write batch to a uniquely-named staging table
    batch_df.write.format("jdbc").options(
        url=POSTGRES_URL,
        driver="org.postgresql.Driver",
        dbtable=staging_table,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    ).mode("overwrite").save()

    # 2. Upsert from staging → target, then clean up
    upsert_sql = f"""
    INSERT INTO trade_indicators (window_start, window_end, symbol, open_price, high_price, low_price, close_price, volume, trade_count, vwap)
    SELECT window_start, window_end, symbol, open_price, high_price, low_price, close_price, volume, trade_count, vwap
    FROM {staging_table}
    ON CONFLICT (window_start, symbol)
    DO UPDATE SET
        window_end  = EXCLUDED.window_end,
        open_price  = EXCLUDED.open_price,
        high_price  = EXCLUDED.high_price,
        low_price   = EXCLUDED.low_price,
        close_price = EXCLUDED.close_price,
        volume      = EXCLUDED.volume,
        trade_count = EXCLUDED.trade_count,
        vwap        = EXCLUDED.vwap;
"""
    
    cleanup_sql = f"DROP TABLE IF EXISTS {staging_table};"

    conn = None
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST, dbname=POSTGRES_DB,
            user=POSTGRES_USER, password=POSTGRES_PASSWORD,
            connect_timeout=10
        )
        with conn:               # auto-commit/rollback on exit
            with conn.cursor() as cur:
                cur.execute(upsert_sql)
        print(f"Batch {batch_id} upserted successfully.")
    except Exception as e:
        print(f"Error during upsert on batch {batch_id}: {e}")
        raise                    # let Spark retry the batch
    finally:
        if conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(cleanup_sql)
                conn.commit()
            except Exception as cleanup_err:
                print(f"Warning: failed to drop staging table {staging_table}: {cleanup_err}")
            conn.close()

def write_to_kafka(df:DataFrame) -> None:
    kafka_df = df.select(
        F.col("symbol").alias("key"),
        F.to_json(F.struct("*")).alias("value")
    )

    return (
        kafka_df.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", "enriched_trades")
        .option("checkpointLocation", CHEKPOINT_DIR + "/kafka_sink")
        .outputMode("update")
        .start()
    )
def update_cache_from_batch(batch_df,batch_id : int) -> None:
    cache = IndicatorCache()

    if not cache.ping():
        log.warning("Cannot connect to Redis. Skipping cache update for batch %d", batch_id)
        return
    
    rows = batch_df.collect()

    for row in rows:
        d = row.asDict()
        symbol = d.pop("symbol")
        cache.write_indicators(symbol, d)
    
    log.info("Updated cache for batch %d with %d records", batch_id, len(rows))

def main() -> None:
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    trades_df = read_trades(spark)
    enriched_df = compute_ohlcv_vwap(trades_df)

    pg_query = (enriched_df.writeStream.foreachBatch(write_to_postgres)
                .option("checkpointLocation", CHEKPOINT_DIR + "/postgres_sink")
                .outputMode("update")
                .trigger(processingTime="30 seconds")
                .start()
    )
    
    write_to_kafka(enriched_df)
    redis_query = (enriched_df.writeStream.foreachBatch(update_cache_from_batch)
                .option("checkpointLocation", CHEKPOINT_DIR + "/redis_sink")
                .outputMode("update")
                .trigger(processingTime="30 seconds")
                .start()
    )

    print("Trades Enricher is running...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()