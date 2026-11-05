import os
from pathlib import Path

# 1. Configurar Java 8
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]

# 2. Importar Spark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, trim

# 3. Configurar Spark
conf = SparkConf() \
    .setAppName("Homologacion") \
    .setMaster("local[2]")

sc = None

try:
    # 4. Crear SparkContext y SQLContext
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    sqlContext = SQLContext(sc)

    # 5. Ubicar el CSV original
    carpeta_proyecto = Path(__file__).resolve().parent.parent
    ruta_csv = carpeta_proyecto / "datos" / "Datos_Grupo - Hoja 1.csv"

    # 6. Leer el CSV
    df_original = sqlContext.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(str(ruta_csv))

    # 7. Homologar nombres y quitar espacios
    df_homologado = df_original.select(
        trim(col("Nombre")).alias("nombre"),
        trim(col("Direccion")).alias("direccion"),
        trim(col("Nombre de Mascota")).alias("nombre_mascota"),
        trim(col("Apodo de 2 mejores amigos")).alias("apodo_mejores_amigos"),
        trim(col("CURP")).alias("curp"),
        trim(col("Pasatiempo")).alias("pasatiempo"),
        trim(col("Carrera")).alias("carrera"),
        trim(col("Facultad")).alias("facultad"),
        trim(col("Cantante favorito")).alias("cantante_favorito"),
        col("Edad").alias("edad"),
        trim(col("Banda Favorita")).alias("banda_favorita")
    )

    # 8. Mostrar resultados de la homologacion
    print("\n====================================")
    print("HOMOLOGACION DE DATOS")
    print("====================================")

    print("\nColumnas homologadas:")
    print(df_homologado.columns)

    print("\nEstructura del DataFrame:")
    df_homologado.printSchema()

    print("\nCantidad de registros:")
    print(df_homologado.count())

    print("\nHomologacion finalizada correctamente.")

except Exception as error:
    print("\nOcurrio un error:")
    print(error)

finally:
    if sc is not None:
        sc.stop()
        print("\nSparkContext detenido.")