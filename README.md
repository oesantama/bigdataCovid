# bigdataCovid
# Análisis de Datos COVID-19 en Colombia con Spark y Kafka

Este proyecto demuestra el uso de Apache Spark y Apache Kafka para procesar y analizar datos de casos de COVID-19 en Colombia, provenientes del portal de Datos Abiertos Colombia. Incluye procesamiento batch para análisis histórico y procesamiento en tiempo real (simulado) para monitorización.

## Descripción

El proyecto consta de tres scripts principales:

1.  **`batch_covid_analysis.py`**: Un script de PySpark que:
    *   Carga el dataset histórico completo de casos COVID-19 desde un archivo CSV.
    *   Realiza limpieza y transformaciones de datos (manejo de fechas, estandarización de categorías).
    *   Ejecuta análisis exploratorios básicos (EDA) como conteos por departamento, sexo, estado, y tendencias temporales.
    *   Guarda los datos limpios en formato Parquet y los resultados agregados en CSV.

2.  **`kafka_covid_producer.py`**: Un script de Python que simula la generación de nuevos reportes de casos COVID-19 y los publica en un topic de Kafka (`covid-colombia-nuevos-casos`).

3.  **`spark_covid_streaming.py`**: Un script de PySpark Structured Streaming que:
    *   Se conecta al topic de Kafka para consumir los nuevos reportes simulados.
    *   Parsea los mensajes JSON.
    *   Realiza agregaciones en tiempo real (ej: conteo de nuevos casos por departamento).
    *   Muestra los resultados actualizados en la consola.

## Tecnologías Utilizadas

*   **Apache Spark (PySpark)**: Motor de procesamiento distribuido para batch y streaming. Se usan DataFrames y Spark SQL para la manipulación de datos.
*   **Apache Kafka**: Plataforma de streaming de eventos para desacoplar la producción y consumo de datos en tiempo real (simulado).
*   **Python**: Lenguaje de programación principal.
*   **kafka-python**: Librería Python para interactuar con Kafka (productor).

## Prerrequisitos

*   **Java 8 o superior** (Requerido por Spark y Kafka).
*   **Apache Spark** (Versión 3.x recomendada). [Descargar Spark](https://spark.apache.org/downloads.html)
*   **Apache Kafka** (Versión 2.x o 3.x). [Descargar Kafka](https://kafka.apache.org/downloads)
*   **Python 3.6+**
*   **Librerías Python**: Instalar usando pip:
    ```bash
    pip install -r requirements.txt
    ```

## Conjunto de Datos

El script batch utiliza el dataset de casos positivos de COVID-19 en Colombia, disponible en el Portal de Datos Abiertos Colombia.

*   **Fuente:** [https://www.datos.gov.co/](https://www.datos.gov.co/) (Buscar "Casos positivos de COVID-19 en Colombia")
*   **Descarga:** Se recomienda descargar el archivo CSV completo o usar una herramienta como `wget` o `curl` con la URL de la API Socrata (puede requerir ajustes en la URL/query para obtener todos los datos).
*   **Importante:** Debes **descargar el archivo CSV** y **actualizar la variable `DATA_PATH`** en `batch_covid_analysis.py` con la ruta donde lo guardaste.

## Configuración y Ejecución

### 1. Configurar Kafka

*   Inicia Zookeeper (si tu versión de Kafka aún lo requiere):
    ```bash
    bin/zookeeper-server-start.sh config/zookeeper.properties
    ```
*   Inicia el Broker de Kafka:
    ```bash
    bin/kafka-server-start.sh config/server.properties
    ```
*   Crea el topic de Kafka que usará el productor y consumidor de streaming (si no existe y `auto.create.topics.enable` está en `false`):
    ```bash
    bin/kafka-topics.sh --create --topic covid-colombia-nuevos-casos --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
    # Ajusta particiones/replicación según tu configuración
    ```
*   **Nota:** Asegúrate de que Kafka se esté ejecutando en `localhost:9092` o actualiza `KAFKA_BROKER` en los scripts `kafka_covid_producer.py` y `spark_covid_streaming.py`.

### 2. Ejecutar el Procesamiento Batch

*   Asegúrate de haber descargado el dataset CSV y actualizado `DATA_PATH` en `batch_covid_analysis.py`.
*   Crea el directorio de salida si no existe: `mkdir -p output/batch_results`
*   Ejecuta el script usando `spark-submit`:
    ```bash
    spark-submit batch_covid_analysis.py
    ```
*   Los resultados (archivos Parquet y CSV) se guardarán en el directorio `output/batch_results`.

### 3. Ejecutar el Procesamiento Streaming

*   **Paso 1: Iniciar el Productor Kafka:**
    *   Abre una terminal y ejecuta el productor:
        ```bash
        python kafka_covid_producer.py
        ```
    *   Deberías ver mensajes indicando que se están enviando eventos a Kafka. Déjalo corriendo en segundo plano.

*   **Paso 2: Iniciar el Consumidor Spark Streaming:**
    *   Abre **otra terminal**.
    *   Ejecuta el consumidor usando `spark-submit`, asegurándote de incluir el paquete de Kafka correcto para tu versión de Spark:
        ```bash
        # Reemplaza la versión del paquete si usas una versión diferente de Spark/Scala
        spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 spark_covid_streaming.py
        ```
    *   Después de la inicialización de Spark, deberías ver una tabla en la consola que se actualiza periódicamente con el conteo de nuevos casos por departamento (o el análisis configurado).
    *   Presiona `Ctrl+C` en la terminal del consumidor para detener el streaming.
    *   Presiona `Ctrl+C` en la terminal del productor para detener la generación de datos.

## Estructura del Repositorio