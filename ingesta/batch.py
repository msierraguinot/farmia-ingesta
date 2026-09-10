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

    # La evolución de esquema se habilita para los formatos compatibles con Auto Loader
    # (binaryFile utiliza un esquema propio y no requiere evolución de esquema)
    schema_evolution_mode = (
        "none" if source_format == "binaryFile" else "addNewColumns"
    )

    options = {
        "cloudFiles.format": source_format,
        "cloudFiles.inferColumnTypes": "true",
        "cloudFiles.schemaEvolutionMode": schema_evolution_mode,
        "cloudFiles.schemaLocation": schema_location
    }

    # Aplicamos las indicaciones de esquema definidas para el dataset en el fichero de configuración
    if "schema_hints" in source:
        options["cloudFiles.schemaHints"] = source["schema_hints"]

    # Creamos un dataframe que lee con el fichero con las opciones configuradas
    df = (
        spark.readStream
        .format("cloudFiles")
        .options(**options)
        .load(source_path)
    )

    # Añadimos metadata de la ingesta al dataframe leido para identificar el fichero y el momento en que se procesó
    return (
        df
        .withColumn("_ingested_filename", F.col("_metadata.file_name"))
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

    # Configuramos la escritura en Bronze utilizando formato Delta y un checkpoint independiente por dataset.
    # checkpointLocation: Almacena el estado de la query para que se pueda reanudar en caso de fallo
    # mergeSchema: Permite que Delta incorpore columnas nuevas al esquema de destino
    # vailableNow=True: Procesa los datos disponibles y después detiene la query
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

    # Aplicamos las particiones por columnas definidas en la configuración cuando existan
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)

    return writer.start(destination_path)