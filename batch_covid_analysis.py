"""
Script de Spark para procesar en batch datos históricos de COVID-19 de Colombia.
Fuente: datos.gov.co (INS)
Tareas:
1. Cargar datos desde CSV.
2. Limpiar y transformar datos (fechas, categorías, tipos).
3. Realizar Análisis Exploratorio de Datos (EDA) básicos (agregaciones).
4. Guardar datos limpios en formato Parquet.
5. Guardar resultados agregados en formato CSV.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, upper, count, desc, year, month, avg, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# --- Configuración ---
APP_NAME = "CovidColombiaBatchAnalysis"
# ¡¡IMPORTANTE!! Ajusta esta ruta al lugar donde descargaste el archivo CSV
# Ejemplo: "data/Casos_positivos_de_COVID-19_en_Colombia.csv"
# O puedes pasarla como argumento al ejecutar con spark-submit
DATA_PATH = "gt2j-8ykr.csv"
OUTPUT_DIR = "output/batch_results" # Directorio para guardar resultados

# Define el esquema basado en las columnas del CSV de datos.gov.co
# Usar StringType para fechas inicialmente es más seguro por variedad de formatos, luego convertir.
SCHEMA = StructType([
    StructField("fecha_reporte_web", StringType(), True),
    StructField("id_de_caso", StringType(), True), # Aunque parezca numérico, puede ser muy largo o tener variaciones
    StructField("fecha_de_notificaci_n", StringType(), True),
    StructField("departamento", StringType(), True), # Código DANE Dept
    StructField("departamento_nom", StringType(), True), # Nombre Dept
    StructField("ciudad_municipio", StringType(), True), # Código DANE Munic
    StructField("ciudad_municipio_nom", StringType(), True), # Nombre Munic
    StructField("edad", IntegerType(), True),
    StructField("unidad_medida", IntegerType(), True), # 1: Años, 2: Meses, 3: Días
    StructField("sexo", StringType(), True),
    StructField("fuente_tipo_contagio", StringType(), True),
    StructField("ubicacion", StringType(), True), # Ubicación del caso (Casa, Hospital, UCI)
    StructField("estado", StringType(), True), # Estado (Leve, Moderado, Grave, Fallecido)
    StructField("pais_viajo_1_cod", StringType(), True), # Código país
    StructField("pais_viajo_1_nom", StringType(), True), # Nombre país
    StructField("recuperado", StringType(), True), # Estado de recuperación (Activo, Fallecido, Recuperado, N/A)
    StructField("fecha_inicio_sintomas", StringType(), True),
    StructField("fecha_muerte", StringType(), True),
    StructField("fecha_diagnostico", StringType(), True),
    StructField("fecha_recuperado", StringType(), True),
    StructField("tipo_recuperacion", StringType(), True), # PCR, Tiempo
    StructField("per_etn_", IntegerType(), True), # Pertenencia Étnica (código)
    StructField("nom_grupo_", StringType(), True) # Nombre Grupo Étnico
])

# Formato de fecha esperado en los datos (ajusta si es necesario basado en exploración)
# El formato 'yyyy-MM-dd'T'HH:mm:ss.SSS' es común en la API Socrata de datos.gov.co
DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS"

def clean_data(df):
    """Aplica limpieza y transformaciones al DataFrame."""
    print("Iniciando limpieza y transformación...")

    # 1. Renombrar columnas para claridad y quitar caracteres especiales
    df_renamed = df \
        .withColumnRenamed("fecha_reporte_web", "fecha_reporte") \
        .withColumnRenamed("fecha_de_notificaci_n", "fecha_notificacion") \
        .withColumnRenamed("departamento_nom", "departamento") \
        .withColumnRenamed("ciudad_municipio_nom", "municipio") \
        .withColumnRenamed("fuente_tipo_contagio", "tipo_contagio") \
        .withColumnRenamed("per_etn_", "cod_etnia") \
        .withColumnRenamed("nom_grupo_", "grupo_etnico")

    # 2. Convertir columnas de fecha de String a DateType
    # Intentar parsear con el formato esperado, poner null si falla
    date_columns = ["fecha_reporte", "fecha_notificacion", "fecha_inicio_sintomas",
                    "fecha_muerte", "fecha_diagnostico", "fecha_recuperado"]
    df_dates = df_renamed
    for date_col in date_columns:
        df_dates = df_dates.withColumn(date_col, to_date(col(date_col), DATE_FORMAT))

    # 3. Estandarizar campos categóricos (convertir a mayúsculas y limpiar)
    # Sexo: asegurar 'F', 'M' o 'Indeterminado'/'Desconocido'
    df_std = df_dates.withColumn("sexo",
        when(upper(col("sexo")).isin(['F', 'FEMENINO']), "F")
        .when(upper(col("sexo")).isin(['M', 'MASCULINO']), "M")
        .otherwise("Desconocido")
    )
    # Estado: ('Leve', 'Grave', 'Moderado', 'Fallecido', 'N/A')
    df_std = df_std.withColumn("estado",
        when(upper(col("estado")) == "LEVE", "Leve")
        .when(upper(col("estado")) == "GRAVE", "Grave")
        .when(upper(col("estado")) == "MODERADO", "Moderado")
        .when(upper(col("estado")) == "FALLECIDO", "Fallecido")
        .otherwise("N/A") # Agrupar otros o desconocidos
    )
     # Recuperado: ('Activo', 'Fallecido', 'Recuperado', 'N/A')
    df_std = df_std.withColumn("recuperado",
        when(upper(col("recuperado")) == "ACTIVO", "Activo")
        .when(upper(col("recuperado")) == "FALLECIDO", "Fallecido")
        .when(upper(col("recuperado")) == "RECUPERADO", "Recuperado")
        .otherwise("N/A")
    )
    # Tipo Contagio (simplificado)
    df_std = df_std.withColumn("tipo_contagio",
        when(upper(col("tipo_contagio")) == "RELACIONADO", "Relacionado")
        .when(upper(col("tipo_contagio")) == "IMPORTADO", "Importado")
        .when(upper(col("tipo_contagio")) == "EN ESTUDIO", "En Estudio")
        .otherwise("Desconocido")
    )
    # Ubicacion
    df_std = df_std.withColumn("ubicacion",
        when(upper(col("ubicacion")) == "CASA", "Casa")
        .when(upper(col("ubicacion")) == "HOSPITAL", "Hospital")
        .when(upper(col("ubicacion")) == "HOSPITAL UCI", "UCI")
        .when(upper(col("ubicacion")) == "FALLECIDO", "Fallecido") # A veces la ubicación indica fallecimiento
        .otherwise("N/A")
    )

    # 4. Convertir Edad a años (si unidad_medida es meses o días)
    # Nota: Esto es una simplificación. Para análisis precisos, se requiere más cuidado.
    df_age = df_std.withColumn("edad_anios",
        when(col("unidad_medida") == 1, col("edad")) # Ya está en años
        .when(col("unidad_medida") == 2, col("edad") / 12) # Meses a años
        .when(col("unidad_medida") == 3, col("edad") / 365) # Días a años
        .otherwise(col("edad")) # Asumir años si la unidad es desconocida o nula
    ).drop("edad", "unidad_medida") # Quitar columnas originales

    # 5. Eliminar filas con datos esenciales nulos (ej: fecha_reporte, departamento, municipio, edad)
    initial_count = df_age.count()
    df_cleaned = df_age.dropna(subset=["fecha_reporte", "departamento", "municipio", "edad_anios", "sexo"])
    dropped_count = initial_count - df_cleaned.count()
    print(f"Filas eliminadas por nulos en columnas clave: {dropped_count}")

    print(f"Número de registros después de limpieza: {df_cleaned.count()}")
    df_cleaned.printSchema()
    df_cleaned.show(5, truncate=False)
    return df_cleaned

def perform_eda(df, output_base_path):
    """Realiza Análisis Exploratorio de Datos y guarda resultados."""
    print("Realizando Análisis Exploratorio (EDA)...")

    # 1. Conteo total de casos
    total_cases = df.count()
    print(f"\nTotal de casos analizados: {total_cases}")

    # 2. Casos por Departamento (Top 15)
    print("\nCasos por Departamento (Top 15):")
    cases_by_dept = df.groupBy("departamento").count().orderBy(desc("count")).limit(15)
    cases_by_dept.show(truncate=False)
    cases_by_dept.write.mode("overwrite").csv(f"{output_base_path}/cases_by_dept.csv", header=True)

    # 3. Casos por Sexo
    print("\nCasos por Sexo:")
    cases_by_sex = df.groupBy("sexo").count().orderBy(desc("count"))
    cases_by_sex.show()
    cases_by_sex.write.mode("overwrite").csv(f"{output_base_path}/cases_by_sex.csv", header=True)

    # 4. Casos por Estado del Paciente ('estado' o 'recuperado')
    print("\nCasos por Estado ('recuperado'):")
    cases_by_status = df.groupBy("recuperado").count().orderBy(desc("count"))
    cases_by_status.show()
    cases_by_status.write.mode("overwrite").csv(f"{output_base_path}/cases_by_status.csv", header=True)

    # 5. Edad promedio de casos
    avg_age = df.agg(avg("edad_anios")).first()[0]
    print(f"\nEdad promedio de los casos: {avg_age:.2f} años")

    # 6. Casos por Tipo de Contagio
    print("\nCasos por Tipo de Contagio:")
    cases_by_contagion = df.groupBy("tipo_contagio").count().orderBy(desc("count"))
    cases_by_contagion.show()
    cases_by_contagion.write.mode("overwrite").csv(f"{output_base_path}/cases_by_contagion.csv", header=True)

    # 7. Tendencia Temporal (Casos por mes)
    print("\nCasos reportados por Mes:")
    cases_by_month = df.withColumn("anio", year(col("fecha_reporte"))) \
                       .withColumn("mes", month(col("fecha_reporte"))) \
                       .groupBy("anio", "mes").count() \
                       .orderBy("anio", "mes")
    cases_by_month.show(50) # Mostrar más meses
    cases_by_month.write.mode("overwrite").csv(f"{output_base_path}/cases_by_month.csv", header=True)

    print(f"Resultados del EDA guardados en: {output_base_path}")

def main():
    """Función principal del script."""
    # Inicializar SparkSession
    spark = SparkSession.builder \
        .appName(APP_NAME) \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") # Para formatos de fecha flexibles
        .getOrCreate()

    print(f"SparkSession '{APP_NAME}' iniciada.")
    spark.sparkContext.setLogLevel("WARN") # Reducir verbosidad

    # Cargar datos
    print(f"Cargando datos desde: {DATA_PATH}")
    try:
        raw_df = spark.read.csv(DATA_PATH, header=True, schema=SCHEMA, sep=",", quote='"', escape='"')
        print("Datos cargados exitosamente.")
        print(f"Número inicial de registros: {raw_df.count()}")
    except Exception as e:
        print(f"Error cargando datos desde {DATA_PATH}: {e}")
        spark.stop()
        exit(1)

    # Limpiar y transformar datos
    cleaned_df = clean_data(raw_df)

    # Realizar EDA y guardar resultados agregados
    perform_eda(cleaned_df, OUTPUT_DIR)

    # Guardar el DataFrame limpio completo en Parquet
    parquet_output_path = f"{OUTPUT_DIR}/cleaned_covid_data.parquet"
    print(f"\nGuardando DataFrame limpio en formato Parquet en: {parquet_output_path}")
    try:
        cleaned_df.write.mode("overwrite").parquet(parquet_output_path)
        print("DataFrame limpio guardado exitosamente.")
    except Exception as e:
        print(f"Error guardando DataFrame limpio en Parquet: {e}")

    # Detener SparkSession
    spark.stop()
    print("SparkSession detenida.")

if __name__ == "__main__":
    main()