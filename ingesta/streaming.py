from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from .config import load_properties
from pyspark.sql.avro.functions import from_avro


def deserialize_string(column, schema=None, options=None):
    """
    Convierte una columna binaria de Kafka a texto.
    """
    return column.cast("string")


def deserialize_json(column, schema, options=None):
    """
    Convierte una columna binaria de Kafka a un objeto JSON
    utilizando el esquema definido en la configuración.
    """
    return F.from_json(
        column.cast("string"),
        schema
    )


def deserialize_avro(column, schema, options):
    """
    Convierte una columna binaria de Kafka a un objeto Avro
    utilizando Confluent Schema Registry.
    """
    return from_avro(
        data=column,
        jsonFormatSchema=None,
        options=options,
        subject=schema["subject"],
        schemaRegistryAddress=schema["schema_registry_address"]
    )


#Guardamos todos los deserializadores que queremos implementar para los diferentes formatos de mensaje de los eventos de kafka soportados
DESERIALIZERS = {
    "string": deserialize_string,
    "json": deserialize_json,
    "avro": deserialize_avro
}


def read_streaming(
    spark: SparkSession,
    ingestion_config: dict
) -> DataFrame:
    """
    Lee eventos desde Kafka según la configuración del dataset.
    """
    
    source = ingestion_config["source"]

    # Cargamos por separado las propiedades de conexión a Kafka para mantener las credenciales fuera de la configuración de los datasets
    kafka_properties_file = source["kafka_properties_file"]
    kafka_options = load_properties(kafka_properties_file)

    # Extraemos las propiedades de autenticación de Schema Registry del fichero de propiedades
    schema_registry_options = {
        key: value
        for key, value in kafka_options.items()
        if key.startswith("confluent.schema.registry.")
    }
    schema_registry_address = schema_registry_options["confluent.schema.registry.url"]

    options = {
        key: value
        for key, value in kafka_options.items()
        if key.startswith("kafka.")
    }

    options.update(source.get("options", {}))

    # Creamos un dataframe que lee formato kafka con las opciones configuradas
    df = (
        spark.readStream
        .format("kafka")
        .options(**options)
        .load()
    )

    # Deserializamos la clave según el formato definido en la configuración
    key_format = source["key_format"]

    if key_format not in DESERIALIZERS:
        raise ValueError(f"Formato de key del evento Kafka no soportado: {key_format}")

    key_schema = None

    if key_format == "json":
        key_schema = source["json_schema"]

    elif key_format == "avro":
        key_schema = {
            "subject": source["key_subject"],
            "schema_registry_address": schema_registry_address
        }

    key_options = {
        **schema_registry_options,
        **source.get("avro_options", {})
    }

    df = df.withColumn(
        "key",
        DESERIALIZERS[key_format](
            F.col("key"),
            key_schema,
            key_options
        )
    )

    # Deserializamos el contenido del evento según el formato definido en la configuración
    value_format = source["value_format"]

    if value_format not in DESERIALIZERS:
        raise ValueError(f"Formato de value del evento Kafka no soportado: {value_format}")

    value_schema = None

    if value_format == "json":
        value_schema = source["json_schema"]

    elif value_format == "avro":
        value_schema = {
            "subject": source["value_subject"],
            "schema_registry_address": schema_registry_address
        }

    value_options = {
        **schema_registry_options,
        **source.get("avro_options", {})
    }

    df = df.withColumn(
        "value",
        DESERIALIZERS[value_format](
            F.col("value"),
            value_schema,
            value_options
        )
    )

    # Añadimos la fecha y hora en que el evento ha sido ingerido
    return df.withColumn(
        "_ingested_at",
        F.current_timestamp()
    )


def write_streaming(
    ingestion_config: dict,
    df: DataFrame,
    available_now: bool = False
):
    """
    Escribe los datos de Kafka en la capa Bronze.
    """

    sink = ingestion_config["sink"]

    destination_path = sink["path"]
    partition_columns = sink.get("partition_columns", [])

    # Configuramos la escritura de los eventos en Bronze utilizando Delta y un checkpoint independiente por dataset
    # checkpointLocation: Almacena el estado de la query para que se pueda reanudar en caso de fallo
    # mergeSchema: Permite que Delta incorpore columnas nuevas al esquema de destino
    writer = (
        df.writeStream
        .format("delta")
        .option(
            "checkpointLocation",
            f"{destination_path}_checkpoint"
        )
        .option("mergeSchema", "true")
    )

    # Aplicamos las particiones por columnas definidas en la configuración cuando existan
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)

    # AvailableNow permite procesar los eventos disponibles y finalizar la consulta, útil para ejecutar y probar en cluster serverless
    if available_now:
        writer = writer.trigger(availableNow=True)

    return writer.start(destination_path)