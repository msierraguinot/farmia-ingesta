import os

from ingesta.config import load_config


CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "config",
    "ingestion_config.json"
)


def test_batch_datasets_exist():
    config = load_config(CONFIG_PATH)

    batch = config["batch"]

    assert len(batch) > 0


def test_batch_datasets_have_required_fields():
    config = load_config(CONFIG_PATH)

    for ingestion in config["batch"]:
        assert "datasource" in ingestion
        assert "dataset" in ingestion
        assert "source" in ingestion
        assert "sink" in ingestion

        assert "format" in ingestion["source"]
        assert "path" in ingestion["source"]

        assert "format" in ingestion["sink"]
        assert "path" in ingestion["sink"]
        assert "partition_columns" in ingestion["sink"]


def test_batch_uses_supported_formats():
    config = load_config(CONFIG_PATH)

    supported_formats = {
        "json",
        "csv",
        "avro",
        "parquet",
        "binaryFile"
    }

    for ingestion in config["batch"]:
        source_format = ingestion["source"]["format"]

        assert source_format in supported_formats