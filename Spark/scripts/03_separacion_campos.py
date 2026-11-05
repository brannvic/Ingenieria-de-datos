
import os
from pathlib import Path

# ==================================================
# 1. CONFIGURAR JAVA 8
# ==================================================

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]


# ==================================================
# 2. IMPORTAR LIBRERIAS DE SPARK
# ==================================================

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, trim, split, count, when


# ==================================================
# 3. CONFIGURAR SPARK
# ==================================================

conf = SparkConf() \
    .setAppName("Laboratorio Spark 2 - Separacion de campos") \
    .setMaster("local[2]")

sc = None

try:

    # ==================================================
    # 4. CREAR SPARKCONTEXT Y SQLCONTEXT
    # ==================================================

    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    sqlContext = SQLContext(sc)


    # ==================================================
    # 5. LOCALIZAR EL ARCHIVO CSV
    # ==================================================

    carpeta_proyecto = Path(__file__).resolve().parent.parent

    ruta_csv = carpeta_proyecto / "datos" / "Datos_Grupo - Hoja 1.csv"


    # ==================================================
    # 6. LEER EL ARCHIVO CSV
    # ==================================================

    df_original = sqlContext.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(str(ruta_csv))


    # ==================================================
    # 7. HOMOLOGAR NOMBRES DE COLUMNAS Y LIMPIAR ESPACIOS
    # ==================================================

    df = df_original.select(
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


    # ==================================================
    # 8. SEPARAR NOMBRES DE MASCOTAS
    # CONFIGURACION HOP: DELIMITADOR "y"
    # ==================================================

    df = df.withColumn(
        "mascota_1",
        trim(split(col("nombre_mascota"), "y").getItem(0))
    ).withColumn(
        "mascota_2",
        trim(split(col("nombre_mascota"), "y").getItem(1))
    )


    # ==================================================
    # 9. SEPARAR APODOS DE MEJORES AMIGOS
    # CONFIGURACION HOP: DELIMITADOR "y"
    # ==================================================

    df = df.withColumn(
        "apodo_1",
        trim(split(col("apodo_mejores_amigos"), "y").getItem(0))
    ).withColumn(
        "apodo_2",
        trim(split(col("apodo_mejores_amigos"), "y").getItem(1))
    )


    # ==================================================
    # 10. SEPARAR BANDAS FAVORITAS
    # CONFIGURACION HOP: DELIMITADOR "+"
    # ==================================================

    df = df.withColumn(
        "banda_1",
        trim(split(col("banda_favorita"), r"\+").getItem(0))
    ).withColumn(
        "banda_2",
        trim(split(col("banda_favorita"), r"\+").getItem(1))
    )


    # ==================================================
    # 11. MOSTRAR RESULTADOS GENERALES
    # ==================================================

    print("\n====================================")
    print("SEPARACION DE CAMPOS")
    print("====================================")

    print("\nColumnas del DataFrame:")
    print(df.columns)

    print("\nEstructura del DataFrame:")
    df.printSchema()

    print("\nCantidad de registros:")
    print(df.count())


    # ==================================================
    # 12. VALIDAR LAS SEPARACIONES
    # ==================================================

    print("\nValidacion de separaciones:")

    df.select(
        count(
            when(col("mascota_2").isNotNull(), 1)
        ).alias("mascotas_separadas"),

        count(
            when(col("apodo_2").isNotNull(), 1)
        ).alias("apodos_separados"),

        count(
            when(col("banda_2").isNotNull(), 1)
        ).alias("bandas_separadas")
    ).show()


    # ==================================================
    # 13. FINALIZAR
    # ==================================================

    print("\nSeparacion finalizada correctamente.")

except Exception as error:

    print("\nOcurrio un error:")
    print(error)

finally:

    if sc is not None:
        sc.stop()
        print("\nSparkContext detenido.")