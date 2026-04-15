from pyspark.sql import SparkSession
from pyspark.sql.functions import col

print("=== INICIANDO ANALISIS COVID COLOMBIA ===")

# Crear sesión
spark = SparkSession.builder \
    .appName("Analisis_COVID_Colombia") \
    .getOrCreate()

print("✔ Spark iniciado")

# Leer archivo local (el que descargaste)
print("📂 Cargando archivo covid.csv ...")
df = spark.read.csv("covid.csv", header=True, inferSchema=True)

print("✔ Archivo cargado correctamente")

# Mostrar estructura
print("\n=== ESTRUCTURA ===")
df.printSchema()

# Mostrar datos
print("\n=== PRIMEROS DATOS ===")
df.show(10, False)

# Seleccionar columnas
df_select = df.select(
    "fecha_diagnostico",
    "departamento",
    "edad",
    "sexo",
    "estado"
)

print("\n=== DATOS SELECCIONADOS ===")
df_select.show(10, False)

# Filtro
df_filtrado = df_select.filter(col("edad") > 30)

print("\n=== MAYORES DE 30 AÑOS ===")
df_filtrado.show(10, False)

# Agrupar
df_group = df_filtrado.groupBy("departamento").count()

print("\n=== CASOS POR DEPARTAMENTO ===")
df_group.show(20, False)

# Total
print("\n📊 TOTAL REGISTROS:", df.count())

# Guardar resultado
df_group.write.mode("overwrite").csv("resultado_covid", header=True)

print("\n✔ PROCESO FINALIZADO")

spark.stop()
