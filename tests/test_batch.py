import os

from ingesta.config import load_config


CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "config",
    "ingestion_config.json"
)


SUPPORTED_BATCH_FORMATS = {
    "json",
    "csv",
    "avro",
    "parquet",
    "binaryFile"
}


def test_batch_datasets_exist():
    """
    Comprueba que existe al menos un dataset configurado para batch.
    """
    config = load_config(CONFIG_PATH)

    assert len(config["batch"]) > 0


def test_batch_datasets_have_required_fields():
    """
    Comprueba que todos los datasets batch contienen los campos
    necesarios para ejecutar una ingesta.
    """
    config = load_config(CONFIG_PATH)

    for ingestion in config["batch"]:
        assert "datasource" in ingestion
        assert "dataset" in ingestion
        assert "source" in ingestion
        assert "sink" in ingestion

        source = ingestion["source"]
        sink = ingestion["sink"]

        assert "format" in source
        assert "path" in source

        assert "format" in sink
        assert "path" in sink
        assert "partition_columns" in sink

        assert isinstance(sink["partition_columns"], list)


def test_batch_uses_supported_formats():
    """
    Comprueba que todos los formatos batch configurados están
    entre los formatos soportados por el motor.
    """
    config = load_config(CONFIG_PATH)

    for ingestion in config["batch"]:
        source_format = ingestion["source"]["format"]

        assert source_format in SUPPORTED_BATCH_FORMATS


def test_batch_contains_required_formats():
    """
    Comprueba que la configuración de ejemplo demuestra los cinco
    formatos exigidos por el ejercicio.
    """
    config = load_config(CONFIG_PATH)

    configured_formats = {
        ingestion["source"]["format"]
        for ingestion in config["batch"]
    }

    assert "json" in configured_formats
    assert "csv" in configured_formats
    assert "avro" in configured_formats
    assert "parquet" in configured_formats
    assert "binaryFile" in configured_formats


def test_batch_schema_hints_are_configured_for_structured_formats():
    """
    Comprueba que los datasets estructurados tienen indicaciones
    de esquema configuradas.
    """
    config = load_config(CONFIG_PATH)

    for ingestion in config["batch"]:
        source = ingestion["source"]
        source_format = source["format"]

        if source_format != "binaryFile":
            assert "schema_hints" in source
            assert source["schema_hints"].strip() != ""


def test_batch_images_use_binary_file():
    """
    Comprueba que el dataset de imágenes utiliza el formato
    binaryFile de Auto Loader.
    """
    config = load_config(CONFIG_PATH)

    image_datasets = [
        ingestion
        for ingestion in config["batch"]
        if ingestion["dataset"] == "imagenes"
    ]

    assert len(image_datasets) == 1
    assert image_datasets[0]["source"]["format"] == "binaryFile"


def test_batch_sink_uses_delta():
    """
    Comprueba que todos los datasets batch escriben Bronze en Delta.
    """
    config = load_config(CONFIG_PATH)

    for ingestion in config["batch"]:
        assert ingestion["sink"]["format"] == "delta"