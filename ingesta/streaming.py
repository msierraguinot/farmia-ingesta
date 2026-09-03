from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.avro.functions import from_avro
from confluent_kafka.schema_registry import SchemaRegistryClient


schema_registry_url = "url"
schema_registry_username = "key"
schema_registry_password = "password"

schema_registry_conf = {
    "url": schema_registry_url,
    "basic.auth.user.info": (
        f"{schema_registry_username}:{schema_registry_password}"
    )
}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)


def read_streaming(
    spark: SparkSession,
    ingestion_config: dict
) -> DataFrame:
    """
    Lee eventos desde Kafka según la configuración del dataset.
    """

    source = ingestion_config["source"]

    options = source.get("options", {})

    df = (
        spark.readStream
        .format("kafka")
        .options(**options)
        .load()
    )

    df = df.withColumn(
        "key",
        F.col("key").cast("string")
    )

    value_format = source["value_format"]

    if value_format == "string":

        df = df.withColumn(
            "value",
            F.col("value").cast("string")
        )

    elif value_format == "json":

        schema = source["json_schema"]

        df = df.withColumn(
            "value",
            F.from_json(
                F.col("value").cast("string"),
                schema
            )
        )

    elif value_format == "avro":

        value_subject = source["value_subject"]

        value_schema = (
            schema_registry_client
            .get_latest_version(value_subject)
            .schema
            .schema_str
        )

        df = df.withColumn(
            "value",
            from_avro(
                F.expr(
                    "substring(value,6,length(value)-5)"
                ),
                value_schema
            )
        )

    else:

        raise ValueError(
            f"Formato de value no soportado: {value_format}"
        )

    return df.withColumn(
        "_ingested_at",
        F.current_timestamp()
    )


def write_streaming(
    ingestion_config: dict,
    df: DataFrame
):
    """
    Escribe los datos de Kafka en la capa Bronze.
    """

    sink = ingestion_config["sink"]

    destination_path = sink["path"]
    partition_columns = sink.get("partition_columns", [])

    writer = (
        df.writeStream
        .format("delta")
        .option(
            "checkpointLocation",
            f"{destination_path}_checkpoint"
        )
        .option("mergeSchema", "true")
    )

    if partition_columns:
        writer = writer.partitionBy(*partition_columns)

    return writer.start(destination_path)