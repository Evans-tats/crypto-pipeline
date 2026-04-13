import os 

from pyspark.sql import SparkSession,DataFrame
from pyspark.sql import functions as F



TRADE_SCHEMA = """
    symbol STRING,
    trade_id LONG,
    price DOUBLE,
    quantity DOUBLE,
    buyer_market_maker BOOLEAN,
    trade_time LONG,
    event_time LONG,
    ingested_at LONG
"""


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
POSTGRES_URL = os.getenv("POSTGRES_URL","jdbc:postgresql://postgres:5432/crypto_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "crypto")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "crypto_secret")
CHEKPOINT_DIR = "/tmp/checkpoint/trades_enricher"

def create_spark()  -> SparkSession:
    return (
        SparkSession.builder.appName("TradesEnricher")
        .config("spark.sql.streaming.chekpointLocation", CHEKPOINT_DIR)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

def read_trades(spark: SparkSession) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", "trades")
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
        return

    batch_df.write.format("jdbc").options(
        url=POSTGRES_URL,
        driver="org.postgresql.Driver",
        dbtable="trade_indicators",
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    ).mode("append").save()

    print(f"Batch {batch_id} written to Postgres with {batch_df.count()} records")

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

    print("Trades Enricher is running...")
    spark.streams.awaitAnyTermination()

    if __name__ == "__main__":
        main()