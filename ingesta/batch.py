from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F


def read_batch(
    spark: SparkSession,
    ingestion_config: dict
    ) -> DataFrame:
    """
    Lee los datos de un dataset batch utilizando Databricks Auto Loader.
    """

    source = ingestion_config["source"]
    sink = ingestion_config["sink"]

    source_format = source["format"]
    source_path = source["path"]
    destination_path = sink["path"]

    schema_location = f"{destination_path}_schema"

    options = {
        "cloudFiles.format": source_format,
        "cloudFiles.inferColumnTypes": "true",
        "cloudFiles.schemaEvolutionMode": "addNewColumns",
        "cloudFiles.schemaLocation": schema_location
    }

    if "schema_hints" in source:
        options["cloudFiles.schemaHints"] = source["schema_hints"]

    df = (
        spark.readStream
        .format("cloudFiles")
        .options(**options)
        .load(source_path)
    )

    return (
        df
        .withColumn("_ingested_filename", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
    )


def write_batch(
    ingestion_config: dict,
    df: DataFrame
):
    """
    Escribe los datos ingeridos en la capa Bronze en formato Delta.
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
        .trigger(availableNow=True)
    )

    if partition_columns:
        writer = writer.partitionBy(*partition_columns)

    return writer.start(destination_path)