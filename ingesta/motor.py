from pyspark.sql import SparkSession

from .config import load_config
from .batch import read_batch, write_batch
from .streaming import read_streaming, write_streaming


class MotorIngesta:

    def __init__(self, spark: SparkSession, config_path: str):
        self.spark = spark
        self.config = load_config(config_path)

    def ejecutar_batch(self):
        queries = []

        for ingestion_config in self.config.get("batch", []):
            df = read_batch(self.spark, ingestion_config)
            query = write_batch(ingestion_config, df)
            queries.append(query)

        for query in queries:
            query.awaitTermination()

    def ejecutar_streaming(self):
        queries = []

        for ingestion_config in self.config.get("streaming", []):
            df = read_streaming(self.spark, ingestion_config)
            query = write_streaming(ingestion_config, df)
            queries.append(query)

        return queries

    def ejecutar(self):
        self.ejecutar_batch()
        self.ejecutar_streaming()