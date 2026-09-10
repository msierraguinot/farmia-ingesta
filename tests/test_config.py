import json
import os

from ingesta.config import load_config, load_properties


CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "config",
    "ingestion_config.json"
)


def test_load_config():
    """
    Comprueba que la configuración principal se carga correctamente
    y contiene las dos modalidades de ingesta.
    """
    config = load_config(CONFIG_PATH)

    assert isinstance(config, dict)
    assert "batch" in config
    assert "streaming" in config

    assert isinstance(config["batch"], list)
    assert isinstance(config["streaming"], list)


def test_load_config_is_valid_json():
    """
    Comprueba que el fichero de configuración contiene JSON válido.
    """
    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        config = json.load(file)

    assert isinstance(config, dict)


def test_batch_and_streaming_datasets_have_unique_names():
    """
    Comprueba que no existen datasets duplicados dentro de cada modalidad.
    """
    config = load_config(CONFIG_PATH)

    batch_datasets = [
        ingestion["dataset"]
        for ingestion in config["batch"]
    ]

    streaming_datasets = [
        ingestion["dataset"]
        for ingestion in config["streaming"]
    ]

    assert len(batch_datasets) == len(set(batch_datasets))
    assert len(streaming_datasets) == len(set(streaming_datasets))


def test_load_properties(tmp_path):
    """
    Comprueba que load_properties carga correctamente un fichero
    de propiedades clave=valor e ignora comentarios y líneas vacías.
    """
    properties_file = tmp_path / "test.properties"

    properties_file.write_text(
        """
# comentario

key1=value1
key2=value2
key3=value=with=equals
""",
        encoding="utf-8"
    )

    properties = load_properties(str(properties_file))

    assert properties["key1"] == "value1"
    assert properties["key2"] == "value2"
    assert properties["key3"] == "value=with=equals"