
# ============================================================
# PASO 07 - SELECCION Y ORDENAMIENTO DE COLUMNAS
# Proyecto: Migracion de Apache Hop a Apache Spark 2
#
# Este script integra las transformaciones anteriores:
# 1. Homologacion de nombres de columnas.
# 2. Limpieza de espacios.
# 3. Separacion de mascotas, apodos y bandas.
# 4. Correcciones de nombres y separacion de apellidos.
# 5. Normalizacion y extraccion de componentes del domicilio.
# 6. Homologacion del campo estado.
# 7. Seleccion y ordenamiento de las 21 columnas de salida.
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

conf = SparkConf().setAppName("Paso07_SeleccionColumnas").setMaster("local[2]")

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

try:

    # --------------------------------------------------------
    # 2. LECTURA DEL CSV ORIGINAL
    # --------------------------------------------------------

    ruta_csv = (
        "/home/victoria/Proyecto_ETL/Spark/datos/"
        "Datos_Grupo - Hoja 1.csv"
    )

    df_original = sqlContext.read.csv(
        ruta_csv,
        header=True,
        inferSchema=True,
        encoding="UTF-8"
    )

    print("\nPASO 07 - SELECCION DE COLUMNAS")
    print("----------------------------------------")
    print("Registros originales:", df_original.count())

    # --------------------------------------------------------
    # 3. HOMOLOGACION DE NOMBRES DE COLUMNAS
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
    # 4. SEPARACION DE MASCOTAS, APODOS Y BANDAS
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
    # 5. CORRECCIONES DE NOMBRES CONFIGURADAS EN HOP
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
    # 6. SEPARACION DE APELLIDOS Y NOMBRES
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
    # 7. NORMALIZACION DEL DOMICILIO
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
    # 8. EXTRACCION DE COMPONENTES DEL DOMICILIO
    # --------------------------------------------------------

    patron_calle = (
        r"(?is)^(?!(?:Xochimilco|Coyoac[aá]n|Iztapalapa|"
        r"Tlalpan|Iztacalco|GAM)\s*(?:,|$))"
        r"([^,]+?)(?:\s*#?\s*(?:Mz\.?\s*\d+\s*Lt\.?\s*\d+|"
        r"lote\s*\d+|\d{1,4}[A-Za-z]?))?\s*(?:,.*)?$"
    )

    patron_numero = (
        r"(?i)^[^,]*?(?:\s|#)"
        r"(Mz\.?\s*\d+\s*Lt\.?\s*\d+|\d{1,4}[A-Za-z]?)"
        r"\s*(?:,.*)?$"
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

    patron_estado = r"(?is)^.*(Estado de M[eé]xico|Huixquilucan).*$"

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

    df = df.withColumn(
        "cp",
        when(
            length(regexp_extract(col("dir_norm_3"), patron_cp, 1)) > 0,
            regexp_extract(col("dir_norm_3"), patron_cp, 1)
        ).otherwise(None)
    )

    df = df.withColumn(
        "estado",
        when(
            length(regexp_extract(col("dir_norm_3"), patron_estado, 1)) > 0,
            trim(regexp_extract(col("dir_norm_3"), patron_estado, 1))
        ).otherwise(None)
    )

    # --------------------------------------------------------
    # 9. VALUE MAPPER E IF NULL DE APACHE HOP
    # --------------------------------------------------------

    # Hop tiene configurado "Ciudad de Mexico" como valor
    # predeterminado y no conserva valores que no coinciden.

    df = df.withColumn(
        "estado",
        lit("Ciudad de Mexico")
    )

    # Equivalente al paso If Null de Hop.
    df = df.withColumn(
        "estado",
        when(
            col("estado").isNull(),
            lit("Ciudad de Mexico")
        ).otherwise(col("estado"))
    )

    # --------------------------------------------------------
    # 10. SELECT VALUES 2: SELECCION Y ORDEN DE COLUMNAS
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
        col("edad"),
        col("banda_1"),
        col("banda_2"),
        col("apellido_paterno"),
        col("apellido_materno"),
        col("nombres_1").alias("nombres"),
        col("calle"),
        col("numero"),
        col("colonia"),
        col("alcaldia"),
        col("cp"),
        col("estado")
    )

    # --------------------------------------------------------
    # 11. VALIDACIONES DEL RESULTADO
    # --------------------------------------------------------

    total_registros = df_limpio.count()
    total_columnas = len(df_limpio.columns)

    print("\nRESULTADO DE SELECT VALUES 2")
    print("----------------------------------------")
    print("Total de registros:", total_registros)
    print("Total de columnas:", total_columnas)

    print("\nColumnas finales y su orden:")
    print(df_limpio.columns)

    print("\nEsquema final:")
    df_limpio.printSchema()

    print("\nValidacion del campo estado:")
    print(
        "Registros con estado predeterminado:",
        df_limpio.filter(
            col("estado") == "Ciudad de Mexico"
        ).count()
    )

    print(
        "Registros con estado nulo:",
        df_limpio.filter(col("estado").isNull()).count()
    )

    print("\nValidacion de cantidad de columnas:")
    if total_columnas == 21:
        print("CORRECTO: se obtuvieron las 21 columnas esperadas.")
    else:
        print("REVISAR: la cantidad de columnas no es 21.")

    print("\nPASO 07 FINALIZADO")

finally:
    sc.stop()
    print("SparkContext detenido.")
