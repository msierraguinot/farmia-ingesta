import os

from ingesta.config import load_config


CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "config",
    "ingestion_config.json"
)


def test_streaming_datasets_exist():
    config = load_config(CONFIG_PATH)

    streaming = config["streaming"]

    assert len(streaming) > 0


def test_streaming_datasets_have_required_fields():
    config = load_config(CONFIG_PATH)

    for ingestion in config["streaming"]:
        assert "datasource" in ingestion
        assert "dataset" in ingestion
        assert "source" in ingestion
        assert "sink" in ingestion

        source = ingestion["source"]

        assert source["format"] == "kafka"
        assert "options" in source
        assert "key_format" in source
        assert "key_subject" in source
        assert "value_format" in source
        assert "value_subject" in source

        sink = ingestion["sink"]

        assert sink["format"] == "delta"
        assert "path" in sink
        assert "partition_columns" in sink


def test_streaming_topics_are_configured():
    config = load_config(CONFIG_PATH)

    for ingestion in config["streaming"]:
        options = ingestion["source"]["options"]

        assert (
            "subscribe" in options
            or "subscribePattern" in options
        )
        