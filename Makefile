.PHONY : up, down, logs, spark-submit, clean-checkpoint, psql

up: 
	docker compose up -d
	

down: 
	docker compose down

logs:
	docker compose logs -f --tail=50


# spark-submit:
# 	@echo "Installing psycopg2-binary on spark-master (Driver)..."
# 	docker exec spark-master pip install psycopg2-binary
# 	@echo "Installing psycopg2-binary on spark-worker (Executor)..."
# 	docker exec spark-worker pip install psycopg2-binary
# 	@echo "Submitting Trades Enricher job to spark-master..."
# 	docker exec -it spark-master spark-submit \
# 		--master spark://spark-master:7077 \
# 		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.2 \
# 		--conf "spark.driver.host=spark-master" \
# 		--conf "spark.driver.bindAddress=0.0.0.0" \
# 		--conf "spark.driver.port=7078" \
# 		--conf "spark.blockManager.port=7079" \
# 		--conf "spark.rpc.askTimeout=600s" \
# 		--conf "spark.network.timeout=600s" \
# 		/opt/bitnami/spark/streaming/trades_enricher.py
# spark-submit:
# 	@echo "Installing psycopg2-binary on spark-master (Driver)..."
# 	docker exec spark-master pip install psycopg2-binary
# 	@echo "Installing psycopg2-binary on spark-worker (Executor)..."
# 	docker exec spark-worker pip install psycopg2-binary
# 	@echo "Submitting Trades Enricher job to spark-master..."
# 	docker exec -it spark-master spark-submit \
# 		--master spark://spark-master:7077 \
# 		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.2 \
# 		--py-files /opt/bitnami/spark/streaming/redis_cache.py \
# 		--conf "spark.driver.host=spark-master" \
# 		--conf "spark.driver.bindAddress=0.0.0.0" \
# 		--conf "spark.driver.port=7078" \
# 		--conf "spark.blockManager.port=7079" \
# 		--conf "spark.rpc.askTimeout=600s" \
# 		--conf "spark.network.timeout=600s" \
# 		/opt/bitnami/spark/streaming/trades_enricher.py

spark-submit:
	@echo "Installing dependencies on spark-master (Driver)..."
	docker exec spark-master pip install psycopg2-binary redis
	@echo "Installing dependencies on spark-worker (Executor)..."
	docker exec spark-worker pip install psycopg2-binary redis
	@echo "Submitting Trades Enricher job to spark-master..."
	docker exec -it spark-master spark-submit \
		--master spark://spark-master:7077 \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.2 \
		--py-files /opt/bitnami/spark/streaming/redis_cache.py \
		--conf "spark.driver.host=spark-master" \
		--conf "spark.driver.bindAddress=0.0.0.0" \
		--conf "spark.driver.port=7078" \
		--conf "spark.blockManager.port=7079" \
		--conf "spark.rpc.askTimeout=600s" \
		--conf "spark.network.timeout=600s" \
		/opt/bitnami/spark/streaming/trades_enricher.py

clean-checkpoint:
	docker exec spark-master rm -rf /tmp/checkpoint/trades_enricher

psql:
	docker exec -it postgres psql -U crypto -d crypto_db
