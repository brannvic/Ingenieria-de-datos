
# ============================================================
# PASO 08 - GENERACION Y VALIDACION DEL RESULTADO FINAL
# Proyecto: Migracion de Apache Hop a Apache Spark 2
#
# Integra las transformaciones de los pasos 02 al 07.
# Genera un CSV final y valida que conserve los registros,
# las columnas y los tipos de datos definidos.
#
# No utiliza SparkSession, RDD, Datasets ni ciclos.
# ============================================================

import os
import re

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import (
    col,
    trim,
    split,
    regexp_replace,
    regexp_extract,
    when,
    length,
    lit
)

# ------------------------------------------------------------
# 1. CONFIGURACION DE SPARK
# ------------------------------------------------------------

conf = SparkConf().setAppName(
    "Paso08_GenerarResultado"
).setMaster("local[2]")

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

try:

    # --------------------------------------------------------
    # 2. RUTAS DE ENTRADA Y SALIDA
    # --------------------------------------------------------

    ruta_csv = (
        "/home/victoria/Proyecto_ETL/Spark/datos/"
        "Datos_Grupo - Hoja 1.csv"
    )

    ruta_salida = (
        "/home/victoria/Proyecto_ETL/Spark/resultados/"
        "datos_limpios_csv"
    )

    # --------------------------------------------------------
    # 3. LECTURA DEL CSV ORIGINAL
    # --------------------------------------------------------

    df_original = sqlContext.read.csv(
        ruta_csv,
        header=True,
        inferSchema=True,
        encoding="UTF-8"
    )

    print("\nPASO 08 - GENERACION DEL RESULTADO FINAL")
    print("----------------------------------------")
    print("Registros originales:", df_original.count())

    # --------------------------------------------------------
    # 4. HOMOLOGACION DE NOMBRES DE COLUMNAS
    # --------------------------------------------------------

    df = df_original.select(
        trim(col("Nombre")).alias("nombre"),
        trim(col("Direccion")).alias("direccion"),
        trim(col("Nombre de Mascota")).alias("nombre_mascota"),
        trim(col("Apodo de 2 mejores amigos")).alias(
            "apodo_mejores_amigos"
        ),
        trim(col("CURP")).alias("curp"),
        trim(col("Pasatiempo")).alias("pasatiempo"),
        trim(col("Carrera")).alias("carrera"),
        trim(col("Facultad")).alias("facultad"),
        trim(col("Cantante favorito")).alias("cantante_favorito"),
        col("Edad").cast("integer").alias("edad"),
        trim(col("Banda Favorita")).alias("banda_favorita")
    )

    # --------------------------------------------------------
    # 5. SEPARACION DE MASCOTAS, APODOS Y BANDAS
    # --------------------------------------------------------

    df = df.withColumn(
        "mascota_1",
        trim(split(col("nombre_mascota"), "y").getItem(0))
    )

    df = df.withColumn(
        "mascota_2",
        trim(split(col("nombre_mascota"), "y").getItem(1))
    )

    df = df.withColumn(
        "apodo_1",
        trim(split(col("apodo_mejores_amigos"), "y").getItem(0))
    )

    df = df.withColumn(
        "apodo_2",
        trim(split(col("apodo_mejores_amigos"), "y").getItem(1))
    )

    df = df.withColumn(
        "banda_1",
        trim(split(col("banda_favorita"), r"\+").getItem(0))
    )

    df = df.withColumn(
        "banda_2",
        trim(split(col("banda_favorita"), r"\+").getItem(1))
    )

    # --------------------------------------------------------
    # 6. CORRECCIONES DE NOMBRES CONFIGURADAS EN HOP
    # --------------------------------------------------------

    df = df.withColumn(
        "nombre",
        regexp_replace(
            col("nombre"),
            "(?i)" + re.escape("Diego Varela Jimenez"),
            "Varela Jimenez Diego"
        )
    )

    df = df.withColumn(
        "nombre",
        regexp_replace(
            col("nombre"),
            "(?i)" + re.escape("Axel Zarza"),
            "Zarza SIN_MATERNO Axel"
        )
    )

    df = df.withColumn(
        "nombre",
        regexp_replace(
            col("nombre"),
            "(?i)" + re.escape("San Agustin"),
            "San_Agustin"
        )
    )

    # --------------------------------------------------------
    # 7. SEPARACION DE APELLIDOS Y NOMBRES
    # --------------------------------------------------------

    patron_nombre = r"^(\S+)\s+(\S+)\s+(.+)$"

    df = df.withColumn(
        "apellido_paterno",
        when(
            length(regexp_extract(col("nombre"), patron_nombre, 1)) > 0,
            regexp_extract(col("nombre"), patron_nombre, 1)
        ).otherwise(None)
    )

    df = df.withColumn(
        "apellido_materno",
        when(
            length(regexp_extract(col("nombre"), patron_nombre, 2)) > 0,
            regexp_extract(col("nombre"), patron_nombre, 2)
        ).otherwise(None)
    )

    df = df.withColumn(
        "nombres_1",
        when(
            length(regexp_extract(col("nombre"), patron_nombre, 3)) > 0,
            regexp_extract(col("nombre"), patron_nombre, 3)
        ).otherwise(None)
    )

    # --------------------------------------------------------
    # 8. NORMALIZACION DEL DOMICILIO
    # --------------------------------------------------------

    df = df.withColumn(
        "dir_norm",
        regexp_replace(
            col("direccion"),
            r"(?i)\bDel\.\s*",
            ""
        )
    )

    df = df.withColumn(
        "dir_norm_2",
        regexp_replace(
            col("dir_norm"),
            r"(?i)\b(Av|And|Col|Mz|Lt)\.\s*",
            "$1"
        )
    )

    df = df.withColumn(
        "dir_norm_3",
        regexp_replace(
            col("dir_norm_2"),
            r"\.\s*",
            ","
        )
    )

    df = df.withColumn(
        "dir_norm_3",
        regexp_replace(
            col("dir_norm_3"),
            r"\s*,\s*",
            ","
        )
    )

    # --------------------------------------------------------
    # 9. EXTRACCION DE COMPONENTES DEL DOMICILIO
    # --------------------------------------------------------

    patron_calle = (
        r"(?is)^(?!(?:Xochimilco|Coyoac[aá]n|Iztapalapa|"
        r"Tlalpan|Iztacalco|GAM)\s*(?:,|$))"
        r"([^,]+?)(?:\s*#?\s*(?:Mz\.?\s*\d+\s*Lt\.?\s*\d+|"
        r"lote\s*\d+|\d{1,4}[A-Za-z]?))?\s*(?:,.*)?$"
    )

    patron_numero = (
        r"(?i)^[^,]*?(?:\s|#)"
        r"(Mz\.?\s*\d+\s*Lt\.?\s*\d+|"
        r"\d{1,4}[A-Za-z]?)\s*(?:,.*)?$"
    )

    patron_colonia = (
        r"(?i)^[^,]+,\s*(?:lote\s*\d+,\s*)?"
        r"(?:col(?:onia)?\.?\s+)?"
        r"(?!\d|CDMX|Ciudad de|Mexico City|Estado de|GAM)"
        r"([^,]+?)\s*(?:,.*)?$"
    )

    patron_alcaldia = (
        r"(?isu)^(?:.*[\s,])?"
        r"(Álvaro Obregón|Alvaro Obregon|Azcapotzalco|"
        r"Benito Juárez|Benito Juarez|Coyoacán|Coyoacan|"
        r"Cuajimalpa|Cuauhtémoc|Cuauhtemoc|GAM|"
        r"Gustavo A\.? Madero|Iztacalco|Iztapalapa|"
        r"Magdalena Contreras|Miguel Hidalgo|Milpa Alta|"
        r"Tláhuac|Tlahuac|Tlalpan|Venustiano Carranza|"
        r"Xochimilco|Huixquilucan)(?:[\s,].*)?$"
    )

    patron_cp = r"(?s)^.*?\b(\d{5})\b.*$"

    patron_estado = (
        r"(?is)^.*(Estado de M[eé]xico|Huixquilucan).*$"
    )

    df = df.withColumn(
        "calle",
        when(
            length(trim(regexp_extract(
                col("dir_norm_3"), patron_calle, 1
            ))) > 0,
            trim(regexp_extract(col("dir_norm_3"), patron_calle, 1))
        ).otherwise(None)
    )

    df = df.withColumn(
        "numero",
        when(
            length(trim(regexp_extract(
                col("dir_norm_3"), patron_numero, 1
            ))) > 0,
            trim(regexp_extract(col("dir_norm_3"), patron_numero, 1))
        ).otherwise(None)
    )

    df = df.withColumn(
        "colonia",
        when(
            length(trim(regexp_extract(
                col("dir_norm_3"), patron_colonia, 1
            ))) > 0,
            trim(regexp_extract(col("dir_norm_3"), patron_colonia, 1))
        ).otherwise(None)
    )

    df = df.withColumn(
        "alcaldia",
        when(
            length(trim(regexp_extract(
                col("dir_norm_3"), patron_alcaldia, 1
            ))) > 0,
            trim(regexp_extract(col("dir_norm_3"), patron_alcaldia, 1))
        ).otherwise(None)
    )

    # El codigo postal se conserva como texto.
    df = df.withColumn(
        "cp",
        when(
            length(regexp_extract(col("dir_norm_3"), patron_cp, 1)) > 0,
            regexp_extract(col("dir_norm_3"), patron_cp, 1).cast("string")
        ).otherwise(None).cast("string")
    )

    df = df.withColumn(
        "estado",
        when(
            length(regexp_extract(col("dir_norm_3"), patron_estado, 1)) > 0,
            trim(regexp_extract(col("dir_norm_3"), patron_estado, 1))
        ).otherwise(None)
    )

    # --------------------------------------------------------
    # 10. VALUE MAPPER E IF NULL DE APACHE HOP
    # --------------------------------------------------------

    df = df.withColumn(
        "estado",
        lit("Ciudad de Mexico")
    )

    df = df.withColumn(
        "estado",
        when(
            col("estado").isNull(),
            lit("Ciudad de Mexico")
        ).otherwise(col("estado"))
    )

    # --------------------------------------------------------
    # 11. SELECT VALUES 2: LAS 21 COLUMNAS FINALES
    # --------------------------------------------------------

    df_limpio = df.select(
        col("mascota_1"),
        col("mascota_2"),
        col("apodo_1"),
        col("apodo_2"),
        col("curp"),
        col("pasatiempo"),
        col("carrera"),
        col("facultad"),
        col("cantante_favorito"),
        col("edad").cast("integer").alias("edad"),
        col("banda_1"),
        col("banda_2"),
        col("apellido_paterno"),
        col("apellido_materno"),
        col("nombres_1").alias("nombres"),
        col("calle"),
        col("numero"),
        col("colonia"),
        col("alcaldia"),
        col("cp").cast("string").alias("cp"),
        col("estado")
    )

    # --------------------------------------------------------
    # 12. VALIDACION ANTES DE GUARDAR
    # --------------------------------------------------------

    total_antes = df_limpio.count()

    print("\nVALIDACION ANTES DE GUARDAR")
    print("----------------------------------------")
    print("Registros:", total_antes)
    print("Columnas:", len(df_limpio.columns))
    print("Tipo de edad:", df_limpio.schema["edad"].dataType)
    print("Tipo de cp:", df_limpio.schema["cp"].dataType)

    # --------------------------------------------------------
    # 13. GENERACION DEL CSV FINAL
    # --------------------------------------------------------

    df_limpio.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .option("encoding", "UTF-8") \
        .csv(ruta_salida)

    print("\nARCHIVO GENERADO")
    print("----------------------------------------")
    print("Ruta de salida:", ruta_salida)

    # --------------------------------------------------------
    # 14. LECTURA DEL CSV CON EL ESQUEMA ORIGINAL
    # --------------------------------------------------------

    # Se utiliza el esquema de df_limpio para evitar que Spark
    # convierta el codigo postal a numero al volver a leerlo.

    df_verificacion = sqlContext.read \
        .schema(df_limpio.schema) \
        .option("header", "true") \
        .option("encoding", "UTF-8") \
        .csv(ruta_salida)

    total_despues = df_verificacion.count()
    columnas_despues = len(df_verificacion.columns)

    print("\nVALIDACION DEL ARCHIVO GUARDADO")
    print("----------------------------------------")
    print("Registros recuperados:", total_despues)
    print("Columnas recuperadas:", columnas_despues)
    print(
        "Tipo de edad recuperado:",
        df_verificacion.schema["edad"].dataType
    )
    print(
        "Tipo de cp recuperado:",
        df_verificacion.schema["cp"].dataType
    )

    print("\nESQUEMA RECUPERADO")
    print("----------------------------------------")
    df_verificacion.printSchema()

    # --------------------------------------------------------
    # 15. COMPROBACION FINAL
    # --------------------------------------------------------

    if (
        total_antes == total_despues
        and columnas_despues == 21
        and str(df_verificacion.schema["cp"].dataType) == "StringType"
        and str(df_verificacion.schema["edad"].dataType) == "IntegerType"
    ):
        print("\nCORRECTO: el archivo se guardo y se leyo correctamente.")
        print("Se conservaron los tipos de datos de edad y cp.")
    else:
        print("\nERROR: revisar registros, columnas o tipos de datos.")

    print("\nPASO 08 FINALIZADO")

finally:
    sc.stop()
    print("SparkContext detenido.")
