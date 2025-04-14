"""
Script de Spark Structured Streaming para consumir datos de nuevos casos COVID-19
desde un topic de Kafka y realizar análisis simples en tiempo real (simulado).
Tareas:
1. Conectar a Kafka y suscribirse al topic.
2. Parsear los mensajes JSON entrantes.
3. Realizar agregaciones (ej: contar nuevos casos por departamento).
4. Mostrar resultados en la consola.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, window, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# --- Configuración ---
APP_NAME = "CovidColombiaStreaming"
# ¡¡IMPORTANTE!! Asegúrate de que esta versión coincida con tu instalación de Spark y Scala
# Ejemplo para Spark 3.4.1 con Scala 2.12
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"
KAFKA_BROKER = 'localhost:9092'  # Ajusta si es necesario
KAFKA_TOPIC = 'covid-colombia-nuevos-casos' # Debe coincidir con el topic del productor

# Define el esquema del JSON que esperamos recibir de Kafka
# Debe coincidir exactamente con la estructura generada por kafka_covid_producer.py
# Usar TimestampType si el productor envía un formato parseable directamente por Spark SQL
# O StringType si se requiere parseo manual posterior.
EVENT_SCHEMA = StructType([
    StructField("fecha_reporte_web", StringType(), True), # Usar String y parsear luego si hay dudas
    StructField("id_de_caso", StringType(), True),
    StructField("fecha_de_notificaci_n", StringType(), True),
    StructField("departamento_nom", StringType(), True),
    StructField("ciudad_municipio_nom", StringType(), True),
    StructField("edad", IntegerType(), True),
    StructField("unidad_medida", IntegerType(), True),
    StructField("sexo", StringType(), True),
    StructField("fuente_tipo_contagio", StringType(), True),
    StructField("ubicacion", StringType(), True),
    StructField("estado", StringType(), True),
    StructField("recuperado", StringType(), True),
    StructField("fecha_inicio_sintomas", StringType(), True),
    StructField("fecha_diagnostico", StringType(), True),
    StructField("fecha_muerte", StringType(), True),
    StructField("fecha_recuperado", StringType(), True),
    StructField("tipo_recuperacion", StringType(), True),
    StructField("cod_etnia", IntegerType(), True),
    StructField("grupo_etnico", StringType(), True)
])

def main():
    """Función principal del script de streaming."""
    # Inicializar SparkSession con el paquete Kafka
    spark = SparkSession.builder \
        .appName(APP_NAME) \
        .config("spark.jars.packages", KAFKA_PACKAGE) \
        .config("spark.sql.streaming.schemaInference", "true") # Permite inferir schema si es necesario, aunque es mejor definirlo
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN") # Reducir verbosidad
    print(f"SparkSession '{APP_NAME}' iniciada con soporte Kafka.")

    # Leer Stream desde Kafka
    print(f"Suscribiéndose al topic Kafka '{KAFKA_TOPIC}' en {KAFKA_BROKER}...")
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest")  # Procesar solo mensajes nuevos
        # .option("failOnDataLoss", "false") # Para evitar fallos si se pierden offsets (considerar implicaciones)
        .load()

    # Parsear el JSON de la columna 'value'
    # Kafka entrega key/value como binario, castear 'value' a String primero
    json_df = kafka_df.selectExpr("CAST(value AS STRING)")

    # Aplicar el esquema al JSON string para obtener columnas estructuradas
    parsed_df = json_df.withColumn("data", from_json(col("value"), EVENT_SCHEMA)) \
                       .select("data.*") # Expandir la estructura JSON en columnas

    # Añadir timestamp de procesamiento (útil para ventanas de tiempo)
    processed_df = parsed_df.withColumn("processing_timestamp", current_timestamp())

    print("Schema del stream parseado:")
    processed_df.printSchema()

    # --- Procesamiento en Tiempo Real ---
    # Ejemplo 1: Contar nuevos casos por departamento (actualización continua)
    print("Configurando consulta: Conteo de nuevos casos por departamento...")
    cases_by_dept_query = processed_df \
        .groupBy("departamento_nom") \
        .count() \
        .orderBy(desc("count")) # Ordenar para ver los más activos

    # Ejemplo 2: Contar nuevos casos por estado en ventanas de 5 minutos
    # print("Configurando consulta: Conteo de nuevos casos por estado (ventana de 5 min)...")
    # cases_by_status_window_query = processed_df \
    #     .withWatermark("processing_timestamp", "10 minutes") # Manejo de datos tardíos
    #     .groupBy(
    #         window(col("processing_timestamp"), "5 minutes", "1 minute"), # Ventana deslizante cada minuto
    #         col("estado")
    #     ) \
    #     .count() \
    #     .orderBy("window", "estado")

    # --- Visualización (Salida a Consola) ---
    # Query 1: Conteo por departamento
    query_dept = cases_by_dept_query.writeStream \
        .outputMode("complete")  # Mostrar el conteo total actualizado
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 20) # Mostrar más departamentos
        .start()

    # Query 2: Conteo por estado en ventana (descomentar si se usa el Ejemplo 2)
    # query_status_window = cases_by_status_window_query.writeStream \
    #     .outputMode("update") # Mostrar solo las ventanas actualizadas
    #     .format("console") \
    #     .option("truncate", "false") \
    #     .start()

    print("Streaming iniciado. Mostrando resultados en consola...")
    print("Presiona Ctrl+C en la terminal donde ejecutas spark-submit para detener.")

    # Esperar a que las consultas terminen (o sean interrumpidas)
    # spark.streams.awaitAnyTermination() # Esperar a que CUALQUIER stream termine
    query_dept.awaitTermination() # Esperar a que esta query específica termine
    # Si tienes múltiples queries activas, puedes usar awaitAnyTermination()
    # o awaitTermination() en la última que quieras mantener activa.

    print("Streaming detenido.")
    spark.stop()

if __name__ == "__main__":
    main()