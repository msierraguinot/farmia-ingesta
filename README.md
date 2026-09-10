# FarmIA – Motor de Ingesta de Datos

Motor de ingesta de datos desarrollado en **Python y Apache Spark** para la plataforma de datos de FarmIA.

El proyecto implementa dos tipos de ingesta:

- **Batch:** desde la capa `Landing` hasta `Bronze`, utilizando **Databricks Auto Loader**.
- **Streaming:** desde **Apache Kafka** hasta `Bronze`, soportando diferentes formatos de mensajes y **Confluent Schema Registry** para Avro.

La configuración de las ingestas se encuentra centralizada en un único fichero JSON, permitiendo añadir o modificar datasets sin cambiar la lógica principal del motor.

---

## 1. Objetivo

FarmIA necesita integrar datos procedentes de diferentes fuentes y con diferentes formatos:

- Ventas online.
- Inventario.
- Sensores IoT.
- Eventos de clientes.
- Proveedores y logística.
- Información meteorológica.
- Imágenes.

La solución propuesta utiliza una arquitectura **Lakehouse sobre Azure Databricks**, separando los datos en diferentes capas y utilizando Spark como motor de procesamiento.

El objetivo del proyecto es disponer de un motor de ingesta **reutilizable, configurable y escalable**, capaz de procesar tanto datos batch como eventos en streaming.

---

# 2. Arquitectura

La arquitectura sigue el patrón de un Lakehouse:

```text
                         ┌──────────────────────┐
                         │      FUENTES         │
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

## Capas

### Landing

Zona de entrada de los datos batch.

Aquí se almacenan los ficheros originales procedentes de las diferentes fuentes antes de ser procesados.

En este proyecto se utilizan:

- JSON
- CSV
- Avro
- Parquet
- Imágenes

La capa Landing mantiene los datos en su formato de origen.

### Bronze

Primera capa procesada del Lakehouse.

El motor de ingesta mueve los datos desde Landing hasta Bronze y los almacena en formato **Delta Lake**.

En esta capa se mantienen los datos lo más cercanos posible al origen, incorporando además información de la propia ingesta.

Para los datasets batch se añaden:

- `_ingested_filename`: nombre del fichero de origen.
- `_ingested_at`: fecha y hora de la ingesta.

En streaming se añade:

- `_ingested_at`: fecha y hora en la que el evento ha sido ingerido.

### Silver

Capa destinada a datos limpios, normalizados y transformados.

No forma parte de la implementación actual del motor, pero se contempla como siguiente etapa de la arquitectura.

Ejemplos:

- Limpieza.
- Tratamiento de valores nulos.
- Normalización.
- Validación.
- Deduplicación.
- Unificación de fuentes.

### Gold

Capa orientada al consumo analítico y de negocio.

Tampoco forma parte de la implementación actual.

Podría contener:

- KPIs.
- Agregaciones.
- Modelos analíticos.
- Datos preparados para BI.
- Indicadores de negocio de FarmIA.

---

# 3. Tecnologías utilizadas

| Tecnología | Uso |
|---|---|
| Python | Desarrollo del motor |
| Apache Spark | Motor de procesamiento |
| Databricks | Plataforma de ejecución |
| Databricks Auto Loader | Ingesta incremental de ficheros |
| Delta Lake | Almacenamiento de Bronze |
| Apache Kafka | Fuente de datos streaming |
| Confluent Schema Registry | Gestión de esquemas Avro |
| JSON | Configuración del motor |
| Git | Control de versiones |

---

# 4. Estructura del proyecto

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
├── run_batch_ingesta.py
├── run_streaming_ingesta.py
├── farmia-pruebas.py
├── README.md
└── .gitignore
```

## Descripción de los componentes

### `ingesta/config.py`

Contiene las funciones encargadas de cargar:

- La configuración principal del motor.
- El fichero de propiedades de conexión.

Funciones principales:

```python
load_config()
load_properties()
```

### `ingesta/batch.py`

Implementa la ingesta batch.

Se encarga de:

1. Leer los ficheros mediante Databricks Auto Loader.
2. Aplicar las opciones definidas en la configuración.
3. Añadir metadatos de ingesta.
4. Escribir los datos en Bronze en formato Delta.
5. Aplicar las particiones configuradas.
6. Mantener un checkpoint independiente por dataset.
7. Permitir evolución de esquema.

### `ingesta/streaming.py`

Implementa la lectura y deserialización de eventos Kafka.

El motor soporta:

- `string`
- `json`
- `avro`

Para Avro se utiliza **Confluent Schema Registry**.

### `ingesta/motor.py`

Es el componente principal del proyecto.

La clase:

```python
MotorIngesta
```

coordina las operaciones de ingesta.

Dispone de dos métodos principales:

```python
ejecutar_batch()
ejecutar_streaming()
```

El motor obtiene los datasets directamente de `ingestion_config.json` y crea las operaciones correspondientes.

Esto permite añadir nuevos datasets modificando principalmente la configuración.

---

# 5. Configuración

Toda la configuración funcional de los datasets se encuentra en:

```text
config/ingestion_config.json
```

La configuración está dividida en dos bloques:

```json
{
  "batch": [],
  "streaming": []
}
```

---

# 6. Configuración Batch

Cada dataset batch tiene su propia configuración.

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

## Parámetros

### `datasource`

Identifica la fuente de datos.

```json
"datasource": "farmia"
```

### `dataset`

Identifica el dataset.

```json
"dataset": "ventas"
```

### `source.format`

Define el formato de entrada.

Los formatos utilizados en el proyecto son:

```text
json
csv
avro
parquet
binaryFile
```

`binaryFile` se utiliza para las imágenes.

### `source.path`

Ruta donde se encuentran los datos en Landing.

```json
"path": "/Volumes/.../landing/ventas"
```

### `source.schema_hints`

Permite indicar el esquema esperado para los formatos estructurados.

Ejemplo:

```json
"schema_hints": "id long, producto string, cantidad integer, fecha string"
```

### `sink.format`

Formato de almacenamiento en Bronze.

Actualmente:

```json
"format": "delta"
```

### `sink.path`

Ruta de destino en Bronze.

### `sink.partition_columns`

Define las columnas utilizadas para particionar los datos.

Ejemplo:

```json
"partition_columns": ["fecha"]
```

---

# 7. Datasets Batch incluidos

La configuración de ejemplo demuestra los cinco formatos requeridos:

| Dataset | Formato origen | Destino | Partición |
|---|---|---|---|
| `ventas` | JSON | Delta | `fecha` |
| `inventario` | CSV | Delta | `fecha` |
| `meteorologia` | Parquet | Delta | `fecha` |
| `proveedores` | Avro | Delta | `fecha` |
| `imagenes` | Imágenes (`binaryFile`) | Delta | Sin partición |

Esto permite comprobar que el motor no está limitado a un único formato de entrada.

---

# 8. Databricks Auto Loader

La ingesta batch utiliza **Databricks Auto Loader** mediante:

```python
spark.readStream.format("cloudFiles")
```

Auto Loader permite detectar e ingerir nuevos ficheros de forma incremental.

La configuración utiliza:

```text
cloudFiles.format
cloudFiles.inferColumnTypes
cloudFiles.schemaEvolutionMode
cloudFiles.schemaLocation
cloudFiles.schemaHints
```

El motor mantiene una ubicación de esquema independiente para cada dataset:

```text
<destination_path>_schema
```

y un checkpoint independiente:

```text
<destination_path>_checkpoint
```

Esto permite que cada dataset mantenga su propio estado de procesamiento.

---

# 9. Evolución de esquema

Para los datasets estructurados se habilita:

```text
cloudFiles.schemaEvolutionMode = addNewColumns
```

Esto permite incorporar columnas nuevas cuando el cambio de esquema es compatible.

Además, la escritura en Delta utiliza:

```text
mergeSchema = true
```

De esta forma, el esquema de la tabla Bronze puede incorporar las nuevas columnas detectadas.

Las imágenes utilizan `binaryFile`, por lo que no necesitan evolución de esquema como los datasets estructurados.

---

# 10. Metadatos de ingesta

Cada registro batch incorpora información sobre su procedencia y momento de procesamiento.

### Nombre del fichero

```text
_ingested_filename
```

Se obtiene de los metadatos proporcionados por Auto Loader.

### Fecha de ingesta

```text
_ingested_at
```

Se genera utilizando:

```python
current_timestamp()
```

Estos campos permiten conocer cuándo se procesó un registro y de qué fichero procede.

---

# 11. Configuración Streaming

Los datasets streaming se configuran dentro de:

```json
"streaming": []
```

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

---

# 12. Parámetros Streaming

Cada dataset puede definir de forma independiente:

### Formato

```json
"format": "kafka"
```

### Topic

Se puede utilizar:

```json
"subscribe": "eventos_clientes"
```

para suscribirse a un topic concreto.

También:

```json
"subscribePattern": "sensores_.*"
```

para suscribirse a un patrón de topics.

### Key

```json
"key_format": "string",
"key_subject": "..."
```

### Value

```json
"value_format": "avro",
"value_subject": "..."
```

El motor soporta actualmente:

```text
string
json
avro
```

---

# 13. Streaming con JSON

Los mensajes JSON utilizan el esquema definido en la configuración.

Ejemplo:

```json
"value_format": "json",
"json_schema": "sensor_id string, temperatura double, humedad double, timestamp string"
```

El motor utiliza este esquema para convertir el contenido binario recibido desde Kafka en una estructura Spark.

---

# 14. Streaming con Avro y Schema Registry

El dataset `eventos_clientes` utiliza:

```text
value_format = avro
```

El esquema se obtiene mediante **Confluent Schema Registry** utilizando el `value_subject` configurado.

Ejemplo:

```json
"value_format": "avro",
"value_subject": "eventos_clientes-value"
```

La dirección y las credenciales de Schema Registry no se almacenan en `ingestion_config.json`.

Se mantienen en:

```text
config/client.properties
```

Esto permite separar:

- Configuración funcional de los datasets.
- Configuración sensible de conexión.

---

# 15. Seguridad de credenciales

El fichero:

```text
config/client.properties
```

contiene información sensible de conexión con Kafka y Schema Registry.

Por este motivo:

- No debe contener credenciales reales en el repositorio público.
- No debe incluirse en Git.
- Debe utilizarse un fichero local o un mecanismo seguro de secretos de Databricks.

El `.gitignore` debe incluir:

```text
config/client.properties
```

Si se necesita incluir una referencia en el repositorio, puede utilizarse una plantilla:

```text
config/client.properties.example
```

con valores ficticios.

---

# 16. Ejecución en Databricks

El proyecto está preparado para ejecutarse desde un repositorio de Databricks.

## Paso 1 – Clonar el repositorio

En Databricks:

```text
Workspace
    ↓
Repos
    ↓
Add Repo
    ↓
URL del repositorio Git
```

Una vez clonado, la estructura será:

```text
farmia-ingesta/
├── config/
├── ingesta/
├── tests/
├── run_batch_ingesta.py
├── run_streaming_ingesta.py
└── README.md
```

## Paso 2 – Preparar los Volumes

El proyecto utiliza Unity Catalog Volumes para almacenar Landing y Bronze.

La estructura utilizada es:

```text
/Volumes/<catalog>/<schema>/landing/
```

y:

```text
/Volumes/<catalog>/<schema>/bronze/
```

Por ejemplo:

```text
/Volumes/mastermsg001dbr/default/landing/
```

```text
/Volumes/mastermsg001dbr/default/bronze/
```

Dentro de estas ubicaciones se crean las carpetas de cada dataset:

```text
landing/
├── ventas/
├── inventario/
├── meteorologia/
├── proveedores/
└── imagenes/

bronze/
├── ventas/
├── inventario/
├── meteorologia/
├── proveedores/
└── imagenes/
```

Las rutas concretas deben coincidir con las definidas en:

```text
config/ingestion_config.json
```

---

# 17. Configurar las conexiones

Antes de ejecutar streaming es necesario configurar el acceso al clúster Kafka.

El fichero:

```text
config/client.properties
```

debe contener las propiedades de conexión de Kafka y, cuando corresponda, las propiedades de autenticación de Schema Registry.

**No se deben guardar credenciales reales en Git.**

Como mínimo, la configuración debe proporcionar:

```text
kafka.bootstrap.servers
kafka.security.protocol
kafka.sasl.mechanism
kafka.sasl.jaas.config
```

Cuando se utilice Avro:

```text
confluent.schema.registry.url
confluent.schema.registry.basic.auth.credentials.source
confluent.schema.registry.basic.auth.user.info
```

El motor carga estas propiedades automáticamente mediante `load_properties()`.

---

# 18. Ejecución Batch

El fichero:

```text
run_batch_ingesta.py
```

inicializa el motor utilizando la configuración:

```text
config/ingestion_config.json
```

Conceptualmente, la ejecución es:

```python
from ingesta.motor import MotorIngesta

config_path = "config/ingestion_config.json"

motor = MotorIngesta(
    spark,
    config_path
)

motor.ejecutar_batch()
```

El motor:

1. Lee todos los datasets configurados en `batch`.
2. Crea una lectura Auto Loader para cada dataset.
3. Añade los metadatos de ingesta.
4. Escribe cada dataset en su destino Bronze.
5. Espera a que finalicen las ingestas.
6. Registra los errores mediante logging.

---

# 19. Ejecución Batch programada

El enunciado establece que la primera versión del motor batch debe ejecutarse cada hora.

El código del motor realiza una ejecución batch, mientras que la periodicidad puede gestionarse mediante un **Databricks Job**.

Configuración:

```text
Databricks Job
       │
       ▼
run_batch_ingesta.py
       │
       ▼
MotorIngesta.ejecutar_batch()
       │
       ▼
Landing → Bronze
```

El Job puede configurarse con una periodicidad:

```text
Cada 1 hora
```

De esta forma, el motor se ejecuta periódicamente y Auto Loader procesa los nuevos ficheros pendientes.

---

# 20. Ejecución Streaming

El fichero:

```text
run_streaming_ingesta.py
```

inicializa el motor:

```python
from ingesta.motor import MotorIngesta

config_path = "config/ingestion_config.json"

motor = MotorIngesta(
    spark,
    config_path
)

queries, errores = motor.ejecutar_streaming()
```

El motor recorre todos los datasets definidos en:

```json
"streaming": []
```

y crea una Streaming Query para cada uno.

Por ejemplo:

```text
eventos_clientes
sensores_iot
```

generan dos consultas independientes.

---

# 21. Streaming continuo

Para mantener las consultas activas y procesar eventos en tiempo real:

```python
queries, errores = motor.ejecutar_streaming()
```

El parámetro:

```python
available_now=False
```

utiliza el comportamiento continuo de Structured Streaming.

La arquitectura resultante es:

```text
Kafka
  │
  ├── eventos_clientes
  │
  └── sensores_*
          │
          ▼
    Spark Structured Streaming
          │
          ▼
       Bronze Delta
```

Las consultas permanecen activas mientras el proceso de streaming esté ejecutándose.

---

# 22. Procesamiento Available Now

El motor también permite utilizar:

```python
queries, errores = motor.ejecutar_streaming(
    available_now=True
)
```

En este modo se procesan los datos disponibles y las consultas finalizan cuando terminan de procesar los datos pendientes.

Este modo resulta útil para ejecuciones puntuales o entornos donde no se desea mantener una consulta continua.

---

# 23. Checkpoints

Cada dataset tiene su propio checkpoint.

Para batch:

```text
<destination_path>_checkpoint
```

Para streaming se utiliza igualmente un checkpoint independiente por destino.

El checkpoint permite a Spark mantener el estado de procesamiento y evitar volver a procesar datos ya tratados.

Esto es especialmente importante en procesos incrementales y streaming.

---

# 24. Manejo de errores

El motor incorpora manejo de errores a nivel de dataset.

Si un dataset falla, el error se registra indicando qué dataset ha producido el problema.

Ejemplo:

```text
Dataset ventas: Error al iniciar la ingesta batch: ...
```

Los errores se almacenan y, al finalizar la ejecución, se lanza una excepción específica:

```python
IngestaBatchException
```

o:

```python
IngestaStreamingException
```

Esto permite distinguir los errores de batch y streaming y evita ocultar fallos durante la ejecución.

---

# 25. Logs

El motor utiliza el módulo estándar:

```python
logging
```

para registrar:

- Inicio y finalización de ingestas.
- Errores de datasets.
- Ausencia de nuevos ficheros.
- Problemas durante la ejecución de las queries.

Esto facilita la monitorización de las ejecuciones desde Databricks.

---

# 26. Añadir un nuevo dataset Batch

Una de las principales ventajas del diseño es que no es necesario modificar el código del motor para añadir un nuevo dataset.

Por ejemplo, para añadir:

```text
clientes
```

se incorpora una nueva entrada en:

```text
config/ingestion_config.json
```

Ejemplo:

```json
{
  "datasource": "farmia",
  "dataset": "clientes",
  "source": {
    "format": "json",
    "path": "/Volumes/.../landing/clientes",
    "schema_hints": "id long, nombre string, fecha string"
  },
  "sink": {
    "format": "delta",
    "path": "/Volumes/.../bronze/clientes",
    "partition_columns": ["fecha"]
  }
}
```

El motor detectará automáticamente el nuevo dataset durante la siguiente ejecución.

---

# 27. Añadir un nuevo dataset Streaming

El mismo principio se aplica a streaming.

Para añadir un nuevo origen Kafka se incorpora una nueva configuración:

```json
{
  "datasource": "farmia",
  "dataset": "nuevo_dataset",
  "source": {
    "format": "kafka",
    "kafka_properties_file": "config/client.properties",
    "options": {
      "subscribe": "nuevo_topic",
      "startingOffsets": "earliest"
    },
    "key_format": "string",
    "key_subject": "nuevo_dataset-key",
    "value_format": "json",
    "value_subject": "nuevo_dataset-value",
    "json_schema": "id long, evento string, timestamp string"
  },
  "sink": {
    "format": "delta",
    "path": "/Volumes/.../bronze/nuevo_dataset",
    "partition_columns": []
  }
}
```

No es necesario modificar `motor.py`.

---

# 28. Flujo completo de datos

## Batch

```text
Fuente
  │
  ▼
Landing
  │
  │ Databricks Auto Loader
  ▼
Spark
  │
  ├── Schema Hints
  ├── Schema Evolution
  ├── Metadatos
  │
  ▼
Delta Bronze
```

## Streaming

```text
Fuente
  │
  ▼
Apache Kafka
  │
  │ Structured Streaming
  ▼
Spark
  │
  ├── String
  ├── JSON
  └── Avro + Schema Registry
  │
  ▼
Delta Bronze
```

---

# 29. Consultar los datos en Bronze

Una vez ejecutada la ingesta, los datos pueden consultarse directamente desde Databricks.

Ejemplo batch:

```python
df = (
    spark.read
    .format("delta")
    .load(
        "/Volumes/mastermsg001dbr/default/bronze/ventas"
    )
)

display(df)
```

Para consultar el esquema:

```python
df.printSchema()
```

Ejemplo streaming:

```python
df = (
    spark.read
    .format("delta")
    .load(
        "/Volumes/mastermsg001dbr/default/bronze/eventos_clientes"
    )
)

display(df)
```

---

# 30. Buenas prácticas implementadas

El proyecto incorpora varias decisiones orientadas a facilitar su mantenimiento y escalabilidad.

### Configuración independiente

Cada dataset define sus propias características sin necesidad de modificar el código del motor.

### Modularidad

La lógica está separada en:

```text
config.py
batch.py
streaming.py
motor.py
```

### Auto Loader

Permite una ingesta incremental de ficheros y evita tener que gestionar manualmente qué ficheros nuevos deben procesarse.

### Delta Lake

Bronze utiliza Delta para disponer de un formato transaccional y adecuado para cargas incrementales.

### Checkpoints independientes

Cada dataset mantiene su propio estado de procesamiento.

### Particionado configurable

Cada dataset puede definir sus propias columnas de particionado.

### Evolución de esquema

Los datasets estructurados permiten incorporar columnas nuevas compatibles.

### Separación de credenciales

Las credenciales de Kafka y Schema Registry se mantienen fuera de la configuración funcional de los datasets.

### Logs y manejo de errores

Los errores se registran identificando el dataset afectado.

---

# 31. Correspondencia con los requisitos del ejercicio

| Requisito | Implementación |
|---|---|
| Motor basado en Apache Spark | `MotorIngesta` + Spark |
| Ingesta Landing → Bronze | `batch.py` |
| CSV | Dataset `inventario` |
| JSON | Dataset `ventas` |
| Avro | Dataset `proveedores` |
| Parquet | Dataset `meteorologia` |
| Imágenes | Dataset `imagenes` + `binaryFile` |
| Esquema esperado | `schema_hints` |
| Ruta de origen configurable | `source.path` |
| Ruta de destino configurable | `sink.path` |
| Particionado configurable | `partition_columns` |
| Fecha de ingesta | `_ingested_at` |
| Nombre de fichero | `_ingested_filename` |
| Evolución de esquema | Auto Loader + `addNewColumns` |
| Databricks Auto Loader | `cloudFiles` |
| Kafka | `streaming.py` |
| Formato configurable | `key_format` / `value_format` |
| Key subject | `key_subject` |
| Value subject | `value_subject` |
| Topic | `subscribe` |
| Patrón de topics | `subscribePattern` |
| Kafka → Bronze | `write_streaming()` |
| JSON streaming | `sensores_iot` |
| Avro streaming | `eventos_clientes` |
| Schema Registry | Avro + Confluent Schema Registry |
| Queries desde configuración | `MotorIngesta.ejecutar_streaming()` |
| Escritura Delta | `write_batch()` / `write_streaming()` |
| Logs | `logging` |
| Manejo de errores | Excepciones específicas por modalidad |

---

# 32. Puesta en marcha rápida

## Batch

1. Clonar el repositorio en Databricks.
2. Crear los Volumes de Landing y Bronze.
3. Colocar los ficheros de entrada en las carpetas de Landing.
4. Revisar las rutas de `config/ingestion_config.json`.
5. Configurar el Job de Databricks.
6. Ejecutar:

```python
from ingesta.motor import MotorIngesta

motor = MotorIngesta(
    spark,
    "config/ingestion_config.json"
)

motor.ejecutar_batch()
```

Los datos quedarán disponibles en Bronze en formato Delta.

---

## Streaming

1. Configurar el acceso al clúster Kafka.
2. Configurar Schema Registry si se utiliza Avro.
3. Mantener las credenciales fuera de Git.
4. Revisar los topics y subjects definidos en `ingestion_config.json`.
5. Ejecutar:

```python
from ingesta.motor import MotorIngesta

motor = MotorIngesta(
    spark,
    "config/ingestion_config.json"
)

queries, errores = motor.ejecutar_streaming()
```

Las consultas creadas procesarán los eventos Kafka y los almacenarán en Bronze.

---

# 33. Consideraciones para despliegue

Antes de desplegar el proyecto en otro entorno es necesario revisar:

### Rutas

Actualizar las rutas:

```text
/Volumes/<catalog>/<schema>/...
```

para adaptarlas al entorno de destino.

### Kafka

Configurar las propiedades de conexión en:

```text
config/client.properties
```

### Schema Registry

Si se utiliza Avro, configurar las propiedades correspondientes de Schema Registry en el fichero de propiedades.

### Permisos

El usuario o servicio que ejecute el motor debe tener permisos para:

- Leer Landing.
- Escribir Bronze.
- Crear y utilizar checkpoints.
- Leer la configuración.
- Acceder a Kafka.
- Acceder a Schema Registry cuando se utilice Avro.

---

# 34. Resumen

El proyecto proporciona un motor de ingesta configurable que permite centralizar la lógica de procesamiento y separar dicha lógica de la configuración de cada dataset.

La solución soporta:

```text
                 FARMIA INGESTION ENGINE
                           │
             ┌─────────────┴─────────────┐
             │                           │
           BATCH                      STREAMING
             │                           │
      Databricks                    Apache Kafka
       Auto Loader                       │
             │                           │
      CSV / JSON /                       │
      Avro / Parquet /                   │
      Imágenes                           │
             │                           │
             └─────────────┬─────────────┘
                           │
                           ▼
                       SPARK
                           │
                           ▼
                    DELTA BRONZE
                           │
                           ▼
                    SILVER / GOLD
                     (futuro)
```

El diseño permite incorporar nuevos datasets principalmente mediante configuración, manteniendo una única implementación reutilizable del motor.
