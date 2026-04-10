import os 

from pyspark.sql import SparkSession

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
POSTGRES_URL = os.getenv("POSTGRES_URL","jdbc:postgresql://postgres:5432/crypto_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "crypto")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "crypto_secret")

def create_spark()  -> SparkSession:
    return (
        SparkSession.builder.appName("TradesEnricher")
        .config("spark.sql.streaming.chekpoint")
    )