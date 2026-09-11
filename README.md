# FarmIA – Motor de Ingesta de Datos

Motor de ingesta desarrollado en **Python y Apache Spark** para implementar las cargas batch y streaming de la plataforma de datos de FarmIA.

La solución utiliza **Azure Databricks**, **Databricks Auto Loader**, **Delta Lake**, **Apache Kafka** y **Confluent Schema Registry**.

--- 

## Índice

- [1. Objetivo y arquitectura](#1-objetivo-y-arquitectura)
- [2. Estructura del proyecto](#2-estructura-del-proyecto)
- [3. Configuración](#3-configuración)
  - [3.1. Configuración Batch](#31-configuración-batch)
  - [3.2. Configuración Streaming](#32-configuración-streaming)
  - [3.3. Kafka y Schema Registry](#33-kafka-y-schema-registry)
  - [3.4. Credenciales](#34-credenciales)
- [4. Funcionamiento del motor](#4-funcionamiento-del-motor)
  - [4.1. Batch](#41-batch)
  - [4.2. Streaming](#42-streaming)
  - [4.3. Checkpoints, logs y errores](#43-checkpoints-logs-y-errores)
- [5. Ejecución](#5-ejecución)
  - [5.1. Preparación](#51-preparación)
  - [5.2. Ejecución Batch](#52-ejecución-batch)
  - [5.3. Ejecución Streaming](#53-ejecución-streaming)
  - [5.4. Ejecución de los tests](#54-ejecución-de-los-tests)
- [6. Añadir nuevos datasets](#6-añadir-nuevos-datasets)
- [7. Decisiones técnicas y buenas prácticas](#7-decisiones-técnicas-y-buenas-prácticas)
- [8. Correspondencia con los requisitos](#8-correspondencia-con-los-requisitos)
- [9. Resumen](#9-resumen)

---

# 1. Objetivo y arquitectura

El objetivo es desarrollar un motor de ingesta basado en **Apache Spark** capaz de procesar datos batch y streaming y llevarlos hasta la capa **Bronze** del Lakehouse.

El motor batch se ejecuta inicialmente **cada hora**, mientras que la parte streaming mantiene consultas activas para procesar eventos **en tiempo real**.

### Arquitectura

```text
                         ┌──────────────────────┐
                         │       FUENTES        │
                         └──────────┬───────────┘
                                    │
                    ┌───────────────┴────────────────┐
                    │                                │
                    ▼                                ▼
             DATOS BATCH                         STREAMING
        CSV / JSON / Avro                    Apache Kafka
        Parquet / Imágenes                       │
                    │                            │
                    ▼                            ▼
             ┌─────────────┐              ┌─────────────┐
             │   LANDING   │              │    KAFKA    │
             └──────┬──────┘              └──────┬──────┘
                    │                            │
                    │ Auto Loader                │ Spark
                    │                            │ Structured
                    │                            │ Streaming
                    └──────────────┬─────────────┘
                                   │
                                   ▼
                           ┌─────────────┐
                           │   BRONZE    │
                           │    Delta    │
                           └──────┬──────┘
                                  │
                                  ▼
                           ┌─────────────┐
                           │   SILVER    │
                           │  (futuro)   │
                           └──────┬──────┘
                                  │
                                  ▼
                           ┌─────────────┐
                           │    GOLD     │
                           │  (futuro)   │
                           └─────────────┘
```

### Capas

- **Landing:** recibe los datos batch en su formato original.
- **Bronze:** almacena los datos ingeridos en Delta, manteniéndolos próximos al origen y añadiendo metadatos de ingesta.
- **Silver:** futura capa de limpieza, normalización y transformación.
- **Gold:** futura capa orientada al análisis y consumo de negocio.

En este proyecto se utilizan **Unity Catalog Volumes** porque es la solución utilizada en el entorno Databricks disponible. Por tanto, las rutas son configurables y podrían adaptarse a otra ubicación de almacenamiento sin cambiar la lógica del motor.

---

# 2. Estructura del proyecto

```text
farmia-ingesta/
│
├── config/
│   ├── ingestion_config.json
│   └── client.properties
│
├── ingesta/
│   ├── __init__.py
│   ├── config.py
│   ├── batch.py
│   ├── streaming.py
│   └── motor.py
│
├── tests/
│   ├── __init__.py
│   ├── test_config.py
│   ├── test_batch.py
│   └── test_streaming.py
│
├── README.md
└── .gitignore
```

Los componentes principales son:

| Componente | Función |
|---|---|
| `config.py` | Carga la configuración JSON y las propiedades de conexión |
| `batch.py` | Lectura con Auto Loader y escritura en Bronze |
| `streaming.py` | Lectura de Kafka y deserialización de mensajes |
| `motor.py` | Coordina todas las ingestas configuradas |
| `ingestion_config.json` | Define datasets, formatos, rutas y opciones |

La lógica está separada de la configuración para poder añadir datasets sin modificar el motor.

---

# 3. Configuración

La configuración funcional está centralizada en:

```text
config/ingestion_config.json
```

Se divide en:

```json
{
  "batch": [],
  "streaming": []
}
```

## 3.1. Configuración Batch

Cada dataset define independientemente:

- Formato de entrada.
- Esquema esperado.
- Ruta de Landing.
- Ruta de Bronze.
- Columnas de particionado.

Ejemplo:

```json
{
  "datasource": "farmia",
  "dataset": "ventas",
  "source": {
    "format": "json",
    "path": "/Volumes/.../landing/ventas",
    "schema_hints": "id long, producto string, cantidad integer, fecha string"
  },
  "sink": {
    "format": "delta",
    "path": "/Volumes/.../bronze/ventas",
    "partition_columns": ["fecha"]
  }
}
```

Los datasets incluidos cubren los cinco formatos batch requeridos:

| Dataset | Formato |
|---|---|
| `ventas` | JSON |
| `inventario` | CSV |
| `meteorologia` | Parquet |
| `proveedores` | Avro |
| `imagenes` | Imágenes (`binaryFile`) |

La entrada y la salida tienen formatos independientes. Por ejemplo, `meteorologia` se recibe en Parquet y se almacena en Bronze como Delta.

La ingesta batch utiliza Databricks Auto Loader:

```python
spark.readStream.format("cloudFiles")
```

Se utilizan `schema_hints`, ubicación de esquema, evolución de esquema y checkpoints independientes por dataset.

Además, se añaden:

```text
_ingested_filename
_ingested_at
```

para identificar el fichero de origen y el momento de la ingesta.

---

## 3.2. Configuración Streaming

Los datasets streaming se configuran también de forma independiente.

Ejemplo:

```json
{
  "datasource": "farmia",
  "dataset": "eventos_clientes",
  "source": {
    "format": "kafka",
    "kafka_properties_file": "config/client.properties",
    "options": {
      "subscribe": "eventos_clientes",
      "startingOffsets": "earliest"
    },
    "key_format": "string",
    "key_subject": "eventos_clientes-key",
    "value_format": "avro",
    "value_subject": "eventos_clientes-value"
  },
  "sink": {
    "format": "delta",
    "path": "/Volumes/.../bronze/eventos_clientes",
    "partition_columns": []
  }
}
```

El motor permite configurar:

- Topic concreto mediante `subscribe`.
- Patrón de topics mediante `subscribePattern`.
- Formato de key.
- Key subject.
- Formato de value.
- Value subject.
- Esquema JSON cuando corresponda.
- Ruta y particiones de Bronze.

Actualmente se incluyen:

- `eventos_clientes`: key `string` + value `Avro`.
- `sensores_iot`: key `string` + value `JSON`.

---

## 3.3. Kafka y Schema Registry

La parte streaming utiliza un cluster de **Confluent Cloud** como fuente Kafka.

La configuración conceptual es:

```text
Confluent Cloud
      │
      ├── Kafka Cluster
      │       │
      │       ├── eventos_clientes
      │       └── sensores_...
      │
      └── Schema Registry
              │
              ├── eventos_clientes-key
              └── eventos_clientes-value
```

### Topics

Los productores publican eventos en topics de Kafka.

Por ejemplo:

```text
eventos_clientes
```

El motor se suscribe al topic mediante:

```json
"subscribe": "eventos_clientes"
```

Para sensores se puede utilizar un patrón:

```json
"subscribePattern": "sensores_.*"
```

### Schema Registry

Cuando el value utiliza Avro, el esquema se gestiona mediante Schema Registry.

Por ejemplo:

```text
eventos_clientes-value
```

El motor utiliza el `value_subject` definido en la configuración para obtener el esquema correspondiente.

---

## 3.4. Credenciales

Las propiedades de conexión se mantienen separadas de la configuración de los datasets:

```text
config/client.properties
```

Este fichero contiene las propiedades necesarias para conectar con Kafka y, cuando se utiliza Avro, con Schema Registry.

Es necesario especificar las credenciales de la API-key de Kafka y Schema Registry.

---

# 4. Funcionamiento del motor

El componente central es:

```python
MotorIngesta
```

y proporciona:

```python
ejecutar_batch()
ejecutar_streaming()
```

El motor lee `ingestion_config.json` y crea las ingestas correspondientes para todos los datasets configurados.

---

## 4.1. Batch

El flujo batch es:

```text
Landing
   │
   │ Auto Loader
   ▼
Spark
   │
   ├── Esquema
   ├── Evolución
   ├── Metadatos
   │
   ▼
Bronze / Delta
```

El motor batch debe ejecutarse **cada hora**. Esta periodicidad no se implementa con un bucle dentro de Python, sino mediante la planificación de **Databricks Jobs**:

```text
Databricks Job
      │
      │ cada 1 hora
      ▼
MotorIngesta.ejecutar_batch()
      │
      ▼
Landing → Bronze
```

En cada ejecución, Auto Loader procesa los nuevos ficheros pendientes y el checkpoint permite mantener el estado de cada dataset.

---

## 4.2. Streaming

El flujo streaming es:

```text
Kafka
  │
  ▼
Spark Structured Streaming
  │
  ├── String
  ├── JSON
  └── Avro + Schema Registry
  │
  ▼
Bronze / Delta
```

A diferencia del batch, el streaming **no se programa cada hora**, sino que se inicia y permanece activa mientras el proceso siga ejecutándose.

En este proyecto, la llamada:

```python
queries, errores = motor.ejecutar_streaming()
```

**inicia** las queries, pero el método devuelve inmediatamente la lista de queries. Para mantener el proceso de ejecución asociado al motor bloqueado mientras las consultas siguen activas, puede esperarse a su terminación con `awaitTermination()`:

```python
queries, errores = motor.ejecutar_streaming()

for dataset, query in queries:
    query.awaitTermination()
```

En un despliegue continuo, la query seguirá funcionando hasta que se detenga, falle o se cancele el proceso.

### `availableNow`

El motor también admite:

```python
queries, errores = motor.ejecutar_streaming(
    available_now=True
)
```

`availableNow` procesa todos los datos disponibles en ese momento, utilizando uno o varios micro-batches, y después termina la query. 

### Importante en el entorno Serverless utilizado

Durante el desarrollo se ha utilizado Databricks Serverless. En este entorno no están soportados los triggers `ProcessingTime` y `Continuous` de Structured Streaming; `AvailableNow` es el trigger recomendado. Para un patrón continuo en Serverless, Databricks ofrece la opción de ejecutar Jobs en modo **Continuous** con triggers acotados como `AvailableNow`, o utilizar pipelines Lakeflow en modo continuo. 

Esto explica por qué durante las pruebas del proyecto se utiliza `available_now=True`: permite ejecutar la ingesta streaming en el entorno Serverless disponible. En un cluster que permita Structured Streaming continuo, se puede utilizar la modalidad continua descrita anteriormente.

---

## 4.3. Checkpoints, logs y errores

Cada dataset dispone de un checkpoint independiente para que Spark pueda mantener su estado de procesamiento.

El motor utiliza `logging` para registrar:

- Finalización de ingestas.
- Ausencia de nuevos ficheros.
- Errores de datasets.
- Errores durante la ejecución de las queries.

Los errores se almacenan y se notifican mediante excepciones específicas:

```python
IngestaBatchException
IngestaStreamingException
```

Esto permite identificar qué dataset ha fallado sin ocultar el problema.

---

# 5. Ejecución

## 5.1. Preparación

### 1. Clonar el repositorio

Abrir el repositorio desde Databricks y comprobar que están disponibles:

```text
config/
ingesta/
tests/
README.md
```

### 2. Preparar Landing y Bronze

En el entorno utilizado para desarrollar el proyecto se utilizan Unity Catalog Volumes:

```text
/Volumes/mastermsg001dbr/default/landing/
```

```text
/Volumes/mastermsg001dbr/default/bronze/
```

Las carpetas de los datasets deben coincidir con las rutas definidas en `ingestion_config.json`.

### 3. Configurar Kafka y Schema Registry

Comprobar que `config/client.properties` contiene las propiedades necesarias para conectarse a Kafka y, cuando se utilice Avro, a Schema Registry.

Especificar las **credenciales de la API-key de Kafka y Schema Registry** en el archivo `client.properties`.

---

## 5.2. Ejecución Batch

El motor se inicializa con:

```python
from ingesta.motor import MotorIngesta

motor = MotorIngesta(
    spark,
    "config/ingestion_config.json"
)

motor.ejecutar_batch()
```

La ejecución recorre todos los datasets definidos en:

```json
"batch": []
```

y realiza:

```text
Landing
   ↓
Auto Loader
   ↓
Spark
   ↓
Bronze / Delta
```

---

## 5.3. Ejecución Streaming

El motor crea una Streaming Query por cada dataset incluido en:

```json
"streaming": []
```

La ejecución normal es:

```python
from ingesta.motor import MotorIngesta

motor = MotorIngesta(
    spark,
    "config/ingestion_config.json"
)

queries, errores = motor.ejecutar_streaming()
```

`ejecutar_streaming()` inicia las queries y estas pueden seguir procesando nuevos mensajes de Kafka mientras el proceso Spark permanezca activo.

El flujo es:

```text
Kafka
  ↓
Spark Structured Streaming
  ↓
Bronze / Delta
```

Los nuevos eventos se van procesando a medida que están disponibles.

Para mantener explícitamente el proceso esperando a que las queries sigan activas:

```python
for dataset, query in queries:
    query.awaitTermination()
```

En un despliegue continuo de Databricks, el proceso o Job debe permanecer activo para que las queries continúen ejecutándose.

### `availableNow`

El motor también permite:

```python
queries, errores = motor.ejecutar_streaming(
    available_now=True
)
```

En este modo se procesan los datos disponibles y las queries terminan cuando finaliza el trabajo pendiente.

---

## 5.4. Ejecución de los tests

Los tests automatizados se encuentran en:

```text
tests/
├── test_config.py
├── test_batch.py
└── test_streaming.py
```

Desde la raíz del proyecto:

```bash
pytest
```

Para obtener información detallada:

```bash
pytest -v
```


# 6. Añadir nuevos datasets

Los datasets se añaden mediante configuración.

### Nuevo dataset Batch

Se añade una nueva entrada dentro de:

```json
"batch": []
```

indicando:

```text
dataset
source.format
source.path
source.schema_hints
sink.format
sink.path
sink.partition_columns
```

### Nuevo dataset Streaming

Se añade una entrada dentro de:

```json
"streaming": []
```

indicando:

```text
dataset
topic o patrón de topics
key/value format
key/value subject
destino Bronze
particiones
```

El motor creará automáticamente la nueva Streaming Query.

---

# 7. Correspondencia con los requisitos

| Requisito | Implementación |
|---|---|
| Motor basado en Spark | `MotorIngesta` |
| Batch Landing → Bronze | `batch.py` |
| Ejecución batch cada hora | Databricks Job |
| Streaming en tiempo real | Structured Streaming |
| CSV | `inventario` |
| JSON | `ventas` |
| Avro | `proveedores` |
| Parquet | `meteorologia` |
| Imágenes | `imagenes` |
| Esquema esperado | `schema_hints` |
| Rutas configurables | `source.path` / `sink.path` |
| Particiones configurables | `partition_columns` |
| Fecha de ingesta | `_ingested_at` |
| Nombre de fichero | `_ingested_filename` |
| Evolución de esquema | Auto Loader |
| Auto Loader | `cloudFiles` |
| Kafka | `streaming.py` |
| Key / Value subject | Configuración streaming |
| Topics / patrones | `subscribe` / `subscribePattern` |
| JSON streaming | `sensores_iot` |
| Avro streaming | `eventos_clientes` |
| Schema Registry | Confluent Schema Registry |
| Queries desde configuración | `MotorIngesta` |
| Logs | `logging` |
| Manejo de errores | Excepciones específicas |

---
## Autor

**María Sierra Guinot**  
📧 msierraguinot@gmail.com
