
import os
import sys

# ==========================================
# CONFIGURACION DEL ENTORNO
# ==========================================

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]

# ==========================================
# IMPORTACIONES
# ==========================================

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import (
    col,
    lit,
    lower,
    when,
    regexp_replace,
    regexp_extract,
    length
)
import re

# ==========================================
# CONFIGURACION DE RUTAS
# ==========================================

RUTA_CSV = "datos/Datos_Grupo - Hoja 1.csv"

# ==========================================
# CONFIGURACION DE SPARK
# ==========================================

configuracion = SparkConf()
configuracion.setAppName("Paso 06 - Homologacion de valores")
configuracion.setMaster("local[2]")

sc = SparkContext(conf=configuracion)
sqlContext = SQLContext(sc)

try:

    # ==========================================
    # LECTURA DEL ARCHIVO ORIGINAL
    # ==========================================

    print("======================================")
    print("LECTURA DEL ARCHIVO ORIGINAL")
    print("======================================")

    df = sqlContext.read.csv(
        RUTA_CSV,
        header=True,
        inferSchema=True,
        encoding="UTF-8"
    )

    print("Registros originales:", df.count())

    # ==========================================
    # PASO 06.1: CORRECCIONES DEL CAMPO NOMBRE
    # ==========================================

    print("======================================")
    print("PASO 06.1: CORRECCION DE NOMBRES")
    print("======================================")

    # Los textos de sustitucion se construyen mediante
    # codigos ASCII para no mostrar nombres personales
    # directamente en el codigo.

    origen_1 = bytes([
        68, 105, 101, 103, 111, 32, 86, 97, 114, 101,
        108, 97, 32, 74, 105, 109, 101, 110, 101, 122
    ]).decode("ascii")

    destino_1 = bytes([
        86, 97, 114, 101, 108, 97, 32, 74, 105, 109,
        101, 110, 101, 122, 32, 68, 105, 101, 103, 111
    ]).decode("ascii")

    origen_2 = bytes([
        65, 120, 101, 108, 32, 90, 97, 114, 122, 97
    ]).decode("ascii")

    destino_2 = bytes([
        90, 97, 114, 122, 97, 32, 83, 73, 78, 95,
        77, 65, 84, 69, 82, 78, 79, 32, 65, 120,
        101, 108
    ]).decode("ascii")

    origen_3 = bytes([
        83, 97, 110, 32, 65, 103, 117, 115, 116, 105, 110
    ]).decode("ascii")

    destino_3 = bytes([
        83, 97, 110, 95, 65, 103, 117, 115, 116, 105, 110
    ]).decode("ascii")

    # Se conservan los nombres originales para contar
    # cuántos registros fueron modificados.

    df = df.withColumn(
        "_nombre_original",
        col("Nombre")
    )

    # Primera sustitucion: corregir_diego

    df = df.withColumn(
        "nombre",
        regexp_replace(
            col("Nombre"),
            "(?i)" + re.escape(origen_1),
            destino_1
        )
    )

    # Segunda sustitucion: corregir_axel

    df = df.withColumn(
        "nombre",
        regexp_replace(
            col("nombre"),
            "(?i)" + re.escape(origen_2),
            destino_2
        )
    )

    # Tercera sustitucion: corregir_san_agustin

    df = df.withColumn(
        "nombre",
        regexp_replace(
            col("nombre"),
            "(?i)" + re.escape(origen_3),
            destino_3
        )
    )

    # Contar registros cuyo nombre fue modificado.

    nombres_corregidos = df.filter(
        col("_nombre_original") != col("nombre")
    ).count()

    print("Registros con correcciones de nombre:", nombres_corregidos)

    # ==========================================
    # PASO 06.2: SEPARACION DE APELLIDOS Y NOMBRES
    # ==========================================

    print("======================================")
    print("PASO 06.2: SEPARACION DE NOMBRES")
    print("======================================")

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
        "nombres",
        when(
            length(regexp_extract(col("nombre"), patron_nombre, 3)) > 0,
            regexp_extract(col("nombre"), patron_nombre, 3)
        ).otherwise(None)
    )

    print("Separacion de nombres aplicada.")

    # ==========================================
    # PASO 06.3: EXTRACCION DEL ESTADO
    # ==========================================

    print("======================================")
    print("PASO 06.3: HOMOLOGACION DEL ESTADO")
    print("======================================")

    patron_estado = r"(?is)^.*(Estado de M[eé]xico|Huixquilucan).*$"

    df = df.withColumn(
        "_estado_extraido",
        when(
            length(
                regexp_extract(
                    col("Direccion"),
                    patron_estado,
                    1
                )
            ) > 0,
            regexp_extract(
                col("Direccion"),
                patron_estado,
                1
            )
        ).otherwise(None)
    )

    # En el HPL, Value Mapper no tiene reglas de mapeo.
    # Su valor predeterminado es "Ciudad de Mexico"
    # y no conserva el valor original cuando no coincide.
    #
    # Por eso todos los registros reciben ese valor.

    df = df.withColumn(
        "estado",
        lit("Ciudad de Mexico")
    )

    # If Null: garantiza el valor cuando el campo es nulo.

    df = df.withColumn(
        "estado",
        when(
            col("estado").isNull(),
            lit("Ciudad de Mexico")
        ).otherwise(col("estado"))
    )

    # ==========================================
    # RESULTADOS DEL PASO 06
    # ==========================================

    print("======================================")
    print("RESULTADOS DEL PASO 06")
    print("======================================")

    print("Total de registros:", df.count())

    print(
        "Estados con valor Ciudad de Mexico:",
        df.filter(
            col("estado") == "Ciudad de Mexico"
        ).count()
    )

    print(
        "Estados nulos:",
        df.filter(
            col("estado").isNull()
        ).count()
    )

    print(
        "Registros con apellido paterno:",
        df.filter(
            col("apellido_paterno").isNotNull()
        ).count()
    )

    print(
        "Registros con apellido materno:",
        df.filter(
            col("apellido_materno").isNotNull()
        ).count()
    )

    print(
        "Registros con nombres separados:",
        df.filter(
            col("nombres").isNotNull()
        ).count()
    )

    print("======================================")
    print("COLUMNAS GENERADAS")
    print("======================================")

    print([
        "nombre",
        "apellido_paterno",
        "apellido_materno",
        "nombres",
        "estado"
    ])

    print("======================================")
    print("PASO 06 FINALIZADO")
    print("======================================")

finally:

    sc.stop()
    print("SparkContext detenido.")
