import logging

from pyspark.sql import SparkSession

from .config import load_config
from .batch import read_batch, write_batch
from .streaming import read_streaming, write_streaming

#Inicializamos el logger
logger = logging.getLogger(__name__)

#Mensajes de Spark cuando el sistema de origen no tiene ficheros nuevos para procesar
_MENSAJES_NO_HAY_FICHEROS = (
    "cannot infer schema when the input path",
    "please try to start the stream when there are files"
)


class IngestaBatchException(Exception):
    """
    Excepcion lanzada cuando en uno o más datasets procesados durante la ejecución de la parte batch del motor de ingestas ha habido errores no relacionados con la falta de ficheros en origen
    """
    def __init__(self, errores: list):
        self.errores = errores
        mensaje = f"Se han producido {len(errores)} errores en la ejecución de la ingesta batch: {errores}"
        super().__init__(mensaje)


class IngestaStreamingException(Exception):
    """
    Excepcion lanzada cuando en uno o más datasets procesados durante la ejecución de la parte streaming del motor de ingestas ha habido errores
    """
    def __init__(self, queries: list, errores: list):
        self.queries = queries
        self.errores = errores
        mensaje = f"Se han producido {len(errores)} errores en la ejecución de la ingesta streaming: {errores}"
        super().__init__(mensaje)


class MotorIngesta:

    def __init__(self, spark: SparkSession, config_path: str):
        self.spark = spark
        self.config = load_config(config_path)


    def ejecutar_batch(self):
        """
        Método principal para ejecutar la parte batch del motor de ingesta
        """
        queries = []
        errores = []

        #Recorremos los datasets del config de la parte batch
        for ingestion_config in self.config.get("batch", []):

            dataset = ingestion_config.get("dataset", "desconocido")
            try:
                df = read_batch(self.spark, ingestion_config)
                query = write_batch(ingestion_config, df)
                queries.append((dataset, query))

            except Exception as e:
                if any(mensaje in str(e).lower() for mensaje in _MENSAJES_NO_HAY_FICHEROS):
                    logger.info(f"Dataset {dataset}: No hay ficheros nuevos que ejecutar. {e}")
                else:
                    logger.error(f"Dataset {dataset}: Error al iniciar la ingesta batch: {e}")
                    errores.append((dataset, str(e)))

        #Recorremos todas las consultas batch y esperamos a que terminen
        for dataset, query in queries:
            try:
                query.awaitTermination()
                logger.info(f"Dataset {dataset}: Terminada la ingesta batch")
                    
            except Exception as e:
                logger.error(f"Dataset {dataset}: Fallo durante la ejecución de la ingesta batch: {e}")
                errores.append((dataset, str(e)))

        if len(errores) > 0:
            logger.warning(f"Se han producido {len(errores)} errores en la ejecución de la ingesta batch: {errores}")
            raise IngestaBatchException(errores)

        return queries, errores


    def ejecutar_streaming(self, available_now: bool = False):
        """
        Método principal para ejecutar la parte streaming del motor de ingesta
        """
        queries = []
        errores = []

        #Recorremos los datasets del config de la parte streaming
        for ingestion_config in self.config.get("streaming", []):

            dataset = ingestion_config.get("dataset", "desconocido")
            try:
                df = read_streaming(self.spark, ingestion_config)
                query = write_streaming(ingestion_config, df, available_now)
                queries.append((dataset, query))

            except Exception as e:
                logger.error(f"Dataset {dataset}: Error al iniciar la ingesta streaming: {e}")
                errores.append((dataset, str(e)))

        if len(errores) > 0:
            logger.warning(f"Se han producido {len(errores)} errores en la ejecución de la ingesta streaming: {errores}")
            raise IngestaStreamingException(queries, errores)   
                                        
        return queries, errores