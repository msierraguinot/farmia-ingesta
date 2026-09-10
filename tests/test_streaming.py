import os

from ingesta.config import load_config


CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "config",
    "ingestion_config.json"
)


SUPPORTED_MESSAGE_FORMATS = {
    "string",
    "json",
    "avro"
}


def test_streaming_datasets_exist():
    """
    Comprueba que existe al menos un dataset configurado para streaming.
    """
    config = load_config(CONFIG_PATH)

    assert len(config["streaming"]) > 0


def test_streaming_datasets_have_required_fields():
    """
    Comprueba que todos los datasets streaming contienen los campos
    necesarios para crear una ingesta Kafka.
    """
    config = load_config(CONFIG_PATH)

    for ingestion in config["streaming"]:
        assert "datasource" in ingestion
        assert "dataset" in ingestion
        assert "source" in ingestion
        assert "sink" in ingestion

        source = ingestion["source"]
        sink = ingestion["sink"]

        assert source["format"] == "kafka"

        assert "kafka_properties_file" in source
        assert "options" in source

        assert "key_format" in source
        assert "key_subject" in source

        assert "value_format" in source
        assert "value_subject" in source

        assert source["key_format"] in SUPPORTED_MESSAGE_FORMATS
        assert source["value_format"] in SUPPORTED_MESSAGE_FORMATS

        assert sink["format"] == "delta"
        assert "path" in sink
        assert "partition_columns" in sink

        assert isinstance(sink["partition_columns"], list)


def test_streaming_topics_are_configured():
    """
    Comprueba que cada dataset streaming tiene configurado un tópico
    o un patrón de tópicos Kafka.
    """
    config = load_config(CONFIG_PATH)

    for ingestion in config["streaming"]:
        options = ingestion["source"]["options"]

        assert (
            "subscribe" in options
            or "subscribePattern" in options
        )


def test_streaming_key_and_value_subjects_are_configured():
    """
    Comprueba que los datasets tienen configurados los subjects
    utilizados para la serialización de los mensajes.
    """
    config = load_config(CONFIG_PATH)

    for ingestion in config["streaming"]:
        source = ingestion["source"]

        assert isinstance(source["key_subject"], str)
        assert source["key_subject"].strip() != ""

        assert isinstance(source["value_subject"], str)
        assert source["value_subject"].strip() != ""


def test_streaming_json_datasets_have_schema():
    """
    Comprueba que los datasets cuyo value utiliza JSON tienen
    definido el esquema necesario para deserializarlo.
    """
    config = load_config(CONFIG_PATH)

    for ingestion in config["streaming"]:
        source = ingestion["source"]

        if source["value_format"] == "json":
            assert "json_schema" in source
            assert source["json_schema"].strip() != ""


def test_streaming_avro_datasets_have_subjects():
    """
    Comprueba que los datasets cuyo value utiliza Avro tienen
    configurado su subject de Schema Registry.
    """
    config = load_config(CONFIG_PATH)

    avro_datasets = [
        ingestion
        for ingestion in config["streaming"]
        if ingestion["source"]["value_format"] == "avro"
    ]

    assert len(avro_datasets) > 0

    for ingestion in avro_datasets:
        source = ingestion["source"]

        assert "value_subject" in source
        assert source["value_subject"].strip() != ""


def test_streaming_demonstrates_configurable_formats():
    """
    Comprueba que la configuración utiliza más de un formato de
    mensajes, demostrando que el motor no está limitado a un único
    formato.
    """
    config = load_config(CONFIG_PATH)

    value_formats = {
        ingestion["source"]["value_format"]
        for ingestion in config["streaming"]
    }

    assert "json" in value_formats
    assert "avro" in value_formats


def test_streaming_sinks_use_delta():
    """
    Comprueba que todos los datasets streaming escriben Bronze en Delta.
    """
    config = load_config(CONFIG_PATH)

    for ingestion in config["streaming"]:
        assert ingestion["sink"]["format"] == "delta"