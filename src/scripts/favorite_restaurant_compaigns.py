import os

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, lit, struct, from_unixtime, current_timestamp, unix_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

kafka_target_options = {
    "kafka.bootstrap.servers": "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091",
    "topic": "kolaygrech_out",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";',
    "kafka.sasl.mechanism": "SCRAM-SHA-512",
}

postgres_target_options = {
        "url": "jdbc:postgresql://localhost:5432/de",
        "driver": "org.postgresql.Driver",
        "dbtable": "public.subscribers_feedback",
        "user": "jovyan",
        "password": "jovyan"
    }

# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df, epoch_id):
    if df.rdd.isEmpty():
        return
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.cache()

    try:
        # записываем df в PostgreSQL с полем feedback
        df_for_postgres = (
            df.withColumn("feedback", lit(None).cast("string"))
        )
        (
        df_for_postgres
            .write
            .format("jdbc")
            .mode("append")
            .options(**postgres_target_options)
            .save()
        )
        # создаём df для отправки в Kafka. Сериализация в json.
        df_for_kafka = (
            df.select(to_json(struct(
                col("restaurant_id"),
                col("adv_campaign_id"),
                col("adv_campaign_content"),
                col("adv_campaign_owner"),
                col("adv_campaign_owner_contact"),
                col("adv_campaign_datetime_start"),
                col("adv_campaign_datetime_end"),
                col("datetime_created"),
                col("client_id"),
                col("trigger_datetime_created")
            )).cast("string").alias("value"))
        )
        # отправляем сообщения в результирующий топик Kafka без поля feedback
        (
        df_for_kafka.write
            .format("kafka")
            .options(**kafka_target_options)
            .save()
        )
    finally: #чтобы память очищалась даже при ошибке
        # очищаем память от df
        df.unpersist()
# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

# создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

# читаем из топика Kafka сообщения с акциями от ресторанов
restaurant_read_stream_df = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091")
        .option("subscribe", "kolaygrech_in")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.jaas.config", 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";')
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
        .load()
)

# определяем схему входного сообщения для json
incoming_message_schema = StructType([
    StructField("restaurant_id", StringType(), True),
    StructField("adv_campaign_id", StringType(), True),
    StructField("adv_campaign_content", StringType(), True),
    StructField("adv_campaign_owner", StringType(), True),
    StructField("adv_campaign_owner_contact", StringType(), True),
    StructField("adv_campaign_datetime_start", LongType(), True),
    StructField("adv_campaign_datetime_end", LongType(), True),
    StructField("datetime_created", LongType(), True),
])

parsed_df = (
    restaurant_read_stream_df
        .select(
            col("key").cast("string").alias("key"),
            from_json(col("value").cast("string"), incoming_message_schema).alias("data")
        )
        .select(
            "key",
            "data.restaurant_id",
            "data.adv_campaign_id",
            "data.adv_campaign_content",
            "data.adv_campaign_owner",
            "data.adv_campaign_owner_contact",
            "data.adv_campaign_datetime_start",
            "data.adv_campaign_datetime_end",
            from_unixtime(col("data.datetime_created")).cast(TimestampType()).alias("datetime_created")
            )
        .withWatermark("datetime_created", "10 minutes")
        .dropDuplicates(["restaurant_id", "datetime_created"])
)
# десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
filtered_read_stream_df = (
    parsed_df
        .filter(
            (col("adv_campaign_datetime_start") <= unix_timestamp()) &
            (unix_timestamp() <= col("adv_campaign_datetime_end"))
        )
)
# вычитываем всех пользователей с подпиской на рестораны
subscribers_restaurant_df = (
    spark.read
        .format("jdbc")
        .option("url", "jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de")
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", "public.subscribers_restaurants")
        .option("user", "student")
        .option("password", "de-student")
        .load()
)
# джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
result_df = (
    filtered_read_stream_df.alias("campaigns")
    .join(
        subscribers_restaurant_df.alias("subs"),
        col("campaigns.restaurant_id") == col("subs.restaurant_id"),
        "inner"
    )
    .select(
        col("campaigns.restaurant_id"),
        col("campaigns.adv_campaign_id"),
        col("campaigns.adv_campaign_content"),
        col("campaigns.adv_campaign_owner"),
        col("campaigns.adv_campaign_owner_contact"),
        col("campaigns.adv_campaign_datetime_start"),
        col("campaigns.adv_campaign_datetime_end"),
        col("campaigns.datetime_created").cast("long"),
        col("subs.client_id"),
        current_timestamp().cast("long").alias("trigger_datetime_created")
    )
)
# запускаем стриминг
result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()