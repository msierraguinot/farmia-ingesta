import json

def load_config(config_path: str) -> dict:
    """
    Carga la configuración de las ingestas desde un fichero JSON.
    """
    with open(config_path, "r", encoding="utf-8") as file:
        return json.load(file)
    
def load_properties(path: str) -> dict:
    """
    Carga la configuración de las propiedades desde un fichero de texto plano
    """
    with open(path, "r", encoding="utf-8") as f:
        return dict(
            line.strip().split("=", 1)
            for line in f
            if line.strip() and not line.startswith("#")
        )