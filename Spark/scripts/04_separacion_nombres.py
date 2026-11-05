
import os

# ============================================================
# PASO 1. CONFIGURACION DEL ENTORNO
# ============================================================

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]

# ============================================================
# PASO 2. IMPORTAR LIBRERIAS DE SPARK
# ============================================================

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import (
    col,
    trim,
    split,
    regexp_extract,
    when,
    length,
    lit
)

# ============================================================
# PASO 3. CONFIGURAR SPARK
# ============================================================

configuracion = SparkConf()
configuracion.setAppName("Paso 04 - Separacion de nombres")
configuracion.setMaster("local[2]")

sc = SparkContext(conf=configuracion)
sqlContext = SQLContext(sc)

try:

    # ========================================================
    # PASO 4. LEER EL CSV ORIGINAL
    # ========================================================

    ruta_csv = (
        "/home/victoria/Proyecto_ETL/Spark/datos/"
        "Datos_Grupo - Hoja 1.csv"
    )

    datos = sqlContext.read.csv(
        ruta_csv,
        header=True,
        inferSchema=True,
        encoding="UTF-8"
    )

    print("\nLECTURA DEL ARCHIVO ORIGINAL")
    print("Registros:", datos.count())

    # ========================================================
    # PASO 5. HOMOLOGAR LOS NOMBRES DE LAS COLUMNAS
    # ========================================================

    datos = datos.select(
        col("Nombre").alias("nombre"),
        col("Direccion").alias("direccion"),
        col("Nombre de Mascota").alias("nombre_mascota"),
        col("Apodo de 2 mejores amigos").alias("apodo_mejores_amigos"),
        col("CURP").alias("curp"),
        col("Pasatiempo").alias("pasatiempo"),
        col("Carrera").alias("carrera"),
        col("Facultad").alias("facultad"),
        col("Cantante favorito").alias("cantante_favorito"),
        col("Edad").alias("edad"),
        col("Banda Favorita").alias("banda_favorita")
    )

    # ========================================================
    # PASO 6. LIMPIAR ESPACIOS EN LOS CAMPOS DE TEXTO
    # ========================================================

    datos = datos.select(
        trim(col("nombre")).alias("nombre"),
        trim(col("direccion")).alias("direccion"),
        trim(col("nombre_mascota")).alias("nombre_mascota"),
        trim(col("apodo_mejores_amigos")).alias("apodo_mejores_amigos"),
        trim(col("curp")).alias("curp"),
        trim(col("pasatiempo")).alias("pasatiempo"),
        trim(col("carrera")).alias("carrera"),
        trim(col("facultad")).alias("facultad"),
        trim(col("cantante_favorito")).alias("cantante_favorito"),
        col("edad"),
        trim(col("banda_favorita")).alias("banda_favorita")
    )

    # ========================================================
    # PASO 7. SEPARAR LOS NOMBRES DE LAS MASCOTAS
    # ========================================================

    datos = datos.withColumn(
        "mascota_1",
        trim(split(col("nombre_mascota"), "y").getItem(0))
    )

    datos = datos.withColumn(
        "mascota_2",
        trim(split(col("nombre_mascota"), "y").getItem(1))
    )

    # ========================================================
    # PASO 8. SEPARAR LOS APODOS DE LOS AMIGOS
    # ========================================================

    datos = datos.withColumn(
        "apodo_1",
        trim(split(col("apodo_mejores_amigos"), "y").getItem(0))
    )

    datos = datos.withColumn(
        "apodo_2",
        trim(split(col("apodo_mejores_amigos"), "y").getItem(1))
    )

    # ========================================================
    # PASO 9. SEPARAR LAS BANDAS FAVORITAS
    # ========================================================

    datos = datos.withColumn(
        "banda_1",
        trim(split(col("banda_favorita"), r"\+").getItem(0))
    )

    datos = datos.withColumn(
        "banda_2",
        trim(split(col("banda_favorita"), r"\+").getItem(1))
    )

    # ========================================================
    # PASO 10. SEPARAR APELLIDOS Y NOMBRES
    # ========================================================

    patron_nombre = r"^(\S+)\s+(\S+)\s+(.+)$"

    datos = datos.withColumn(
        "apellido_paterno",
        when(
            length(regexp_extract(col("nombre"), patron_nombre, 1)) > 0,
            regexp_extract(col("nombre"), patron_nombre, 1)
        ).otherwise(lit(None))
    )

    datos = datos.withColumn(
        "apellido_materno",
        when(
            length(regexp_extract(col("nombre"), patron_nombre, 2)) > 0,
            regexp_extract(col("nombre"), patron_nombre, 2)
        ).otherwise(lit(None))
    )

    datos = datos.withColumn(
        "nombres",
        when(
            length(regexp_extract(col("nombre"), patron_nombre, 3)) > 0,
            regexp_extract(col("nombre"), patron_nombre, 3)
        ).otherwise(lit(None))
    )

    # ========================================================
    # PASO 11. MOSTRAR RESULTADOS
    # ========================================================

    print("\n========================================")
    print("PASO 04: SEPARACION DE NOMBRES")
    print("========================================")

    print("\nCOLUMNAS GENERADAS:")
    print("apellido_paterno")
    print("apellido_materno")
    print("nombres")

    print("\nESTRUCTURA DEL DATAFRAME:")
    datos.printSchema()

    print("\nTOTAL DE REGISTROS:")
    print(datos.count())

    print("\nTOTAL DE COLUMNAS:")
    print(len(datos.columns))

    print("\nPROCESO FINALIZADO CORRECTAMENTE")

finally:

    # ========================================================
    # PASO 12. CERRAR SPARK
    # ========================================================

    sc.stop()
    print("\nSparkContext detenido.")