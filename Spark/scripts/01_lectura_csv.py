import os
from pathlib import Path

# 1. Configurar Java 8
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]

# 2. Importar las herramientas de Spark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

# 3. Definir la configuracion de Spark
conf = SparkConf() \
    .setAppName("Lectura CSV") \
    .setMaster("local[2]")

sc = None

try:
    # 4. Crear SparkContext
    sc = SparkContext(conf=conf)

    # 5. Crear SQLContext para trabajar con DataFrames
    sqlContext = SQLContext(sc)

    # 6. Ubicar el archivo CSV
    carpeta_proyecto = Path(__file__).resolve().parent.parent
    ruta_csv = carpeta_proyecto / "datos" / "Datos_Grupo - Hoja 1.csv"

    # 7. Leer el archivo como DataFrame
    df = sqlContext.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(str(ruta_csv))

    # 8. Mostrar informacion general
    print("\n====================================")
    print("LECTURA DEL ARCHIVO CSV")
    print("====================================")

    print("\nColumnas del DataFrame:")
    print(df.columns)

    print("\nEstructura del DataFrame:")
    df.printSchema()

    print("\nCantidad de registros:")
    print(df.count())

    print("\nLectura finalizada correctamente.")

except Exception as error:
    print("\nOcurrio un error:")
    print(error)

finally:
    if sc is not None:
        sc.stop()
        print("\nSparkContext detenido.")