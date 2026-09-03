import os

from ingesta.config import load_config


CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "config",
    "ingestion_config.json"
)


def test_load_config():
    config = load_config(CONFIG_PATH)

    assert isinstance(config, dict)
    assert "batch" in config
    assert "streaming" in config


def test_batch_config():
    config = load_config(CONFIG_PATH)

    assert len(config["batch"]) > 0

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


def test_streaming_config():
    config = load_config(CONFIG_PATH)

    assert len(config["streaming"]) > 0

    for ingestion in config["streaming"]:
        assert "datasource" in ingestion
        assert "dataset" in ingestion
        assert "source" in ingestion
        assert "sink" in ingestion

        assert ingestion["source"]["format"] == "kafka"
        assert "options" in ingestion["source"]
        assert "value_format" in ingestion["source"]

        assert "format" in ingestion["sink"]
        assert "path" in ingestion["sink"]
        assert "partition_columns" in ingestion["sink"]