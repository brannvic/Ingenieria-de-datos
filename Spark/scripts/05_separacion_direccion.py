
import os
import sys

# --------------------------------------------------
# CONFIGURACION DEL ENTORNO
# --------------------------------------------------

BASE_DIR = "/home/victoria/Proyecto_ETL/Spark"

JAVA_HOME = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["JAVA_HOME"] = JAVA_HOME
os.environ["PATH"] = JAVA_HOME + "/bin:" + os.environ["PATH"]

PYTHON_VENV = os.path.join(BASE_DIR, "entorno_python", "venv", "bin", "python")
os.environ["PYSPARK_PYTHON"] = PYTHON_VENV
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_VENV

# --------------------------------------------------
# IMPORTACIONES
# --------------------------------------------------

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
    count
)

# --------------------------------------------------
# CONFIGURACION DE SPARK
# --------------------------------------------------

conf = SparkConf().setAppName("Paso05_Separacion_Direccion")
conf = conf.setMaster("local[2]")

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# --------------------------------------------------
# LECTURA DEL CSV ORIGINAL
# --------------------------------------------------

ruta_csv = os.path.join(
    BASE_DIR,
    "datos",
    "Datos_Grupo - Hoja 1.csv"
)

print("======================================")
print("LECTURA DEL ARCHIVO ORIGINAL")
print("======================================")

df_original = sqlContext.read.csv(
    ruta_csv,
    header=True,
    inferSchema=True,
    encoding="UTF-8"
)

print("Registros originales:", df_original.count())

# --------------------------------------------------
# PASO 02: HOMOLOGACION DE COLUMNAS
# --------------------------------------------------

df = df_original.select(
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

df = df.withColumn("nombre", trim(col("nombre")))
df = df.withColumn("direccion", trim(col("direccion")))
df = df.withColumn("nombre_mascota", trim(col("nombre_mascota")))
df = df.withColumn("apodo_mejores_amigos", trim(col("apodo_mejores_amigos")))
df = df.withColumn("curp", trim(col("curp")))
df = df.withColumn("pasatiempo", trim(col("pasatiempo")))
df = df.withColumn("carrera", trim(col("carrera")))
df = df.withColumn("facultad", trim(col("facultad")))
df = df.withColumn("cantante_favorito", trim(col("cantante_favorito")))
df = df.withColumn("banda_favorita", trim(col("banda_favorita")))

# --------------------------------------------------
# PASO 03: SEPARACION DE MASCOTAS, APODOS Y BANDAS
# --------------------------------------------------

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
    trim(split(col("banda_favorita"), "\\+").getItem(0))
)

df = df.withColumn(
    "banda_2",
    trim(split(col("banda_favorita"), "\\+").getItem(1))
)

# --------------------------------------------------
# PASO 04: SEPARACION DE APELLIDOS Y NOMBRES
# --------------------------------------------------

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

# --------------------------------------------------
# PASO 05: NORMALIZACION DE LA DIRECCION
# --------------------------------------------------

print("======================================")
print("PASO 05: NORMALIZACION DE DIRECCIONES")
print("======================================")

# Eliminar la abreviatura Del. y los espacios posteriores.
df = df.withColumn(
    "_direccion_normalizada",
    regexp_replace(
        col("direccion"),
        r"(?i)\bDel\.\s*",
        ""
    )
)

# Homologar abreviaturas de vialidades y ubicaciones.
df = df.withColumn(
    "_direccion_normalizada",
    regexp_replace(
        col("_direccion_normalizada"),
        r"(?i)\b(Av|And|Col|Mz|Lt)\.\s*",
        "$1"
    )
)

# Convertir puntos en separadores de coma.
df = df.withColumn(
    "_direccion_normalizada",
    regexp_replace(
        col("_direccion_normalizada"),
        r"\.\s*",
        ","
    )
)

# Eliminar espacios sobrantes alrededor de las comas.
df = df.withColumn(
    "_direccion_normalizada",
    regexp_replace(
        col("_direccion_normalizada"),
        r"\s*,\s*",
        ","
    )
)

# --------------------------------------------------
# EXTRACCION DE CALLE
# --------------------------------------------------

patron_calle = (
    r"(?is)^(?!(?:Xochimilco|Coyoac[aá]n|Iztapalapa|"
    r"Tlalpan|Iztacalco|GAM)\s*(?:,|$))"
    r"([^,]+?)(?:\s*#?\s*(?:Mz\.?\s*\d+\s*Lt\.?\s*\d+|"
    r"lote\s*\d+|\d{1,4}[A-Za-z]?))?\s*(?:,.*)?$"
)

df = df.withColumn(
    "_calle_extraida",
    regexp_extract(
        col("_direccion_normalizada"),
        patron_calle,
        1
    )
)

df = df.withColumn(
    "calle",
    when(
        length(trim(col("_calle_extraida"))) > 0,
        trim(col("_calle_extraida"))
    ).otherwise(None)
)

# --------------------------------------------------
# EXTRACCION DE NUMERO
# --------------------------------------------------

patron_numero = (
    r"(?i)^[^,]*?(?:\s|#)"
    r"(Mz\.?\s*\d+\s*Lt\.?\s*\d+|\d{1,4}[A-Za-z]?)"
    r"\s*(?:,.*)?$"
)

df = df.withColumn(
    "_numero_extraido",
    regexp_extract(
        col("_direccion_normalizada"),
        patron_numero,
        1
    )
)

df = df.withColumn(
    "numero",
    when(
        length(trim(col("_numero_extraido"))) > 0,
        trim(col("_numero_extraido"))
    ).otherwise(None)
)

# --------------------------------------------------
# EXTRACCION DE COLONIA
# --------------------------------------------------

patron_colonia = (
    r"(?i)^[^,]+,\s*(?:lote\s*\d+,\s*)?"
    r"(?:col(?:onia)?\.?\s+)?"
    r"(?!\d|CDMX|Ciudad de|Mexico City|Estado de|GAM)"
    r"([^,]+?)\s*(?:,.*)?$"
)

df = df.withColumn(
    "_colonia_extraida",
    regexp_extract(
        col("_direccion_normalizada"),
        patron_colonia,
        1
    )
)

df = df.withColumn(
    "colonia",
    when(
        length(trim(col("_colonia_extraida"))) > 0,
        trim(col("_colonia_extraida"))
    ).otherwise(None)
)

# --------------------------------------------------
# EXTRACCION DE ALCALDIA
# --------------------------------------------------

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

df = df.withColumn(
    "_alcaldia_extraida",
    regexp_extract(
        col("_direccion_normalizada"),
        patron_alcaldia,
        1
    )
)

df = df.withColumn(
    "alcaldia",
    when(
        length(trim(col("_alcaldia_extraida"))) > 0,
        trim(col("_alcaldia_extraida"))
    ).otherwise(None)
)

# --------------------------------------------------
# EXTRACCION DE CODIGO POSTAL
# --------------------------------------------------

patron_cp = r"(?s)^.*?\b(\d{5})\b.*$"

df = df.withColumn(
    "_cp_extraido",
    regexp_extract(
        col("_direccion_normalizada"),
        patron_cp,
        1
    )
)

df = df.withColumn(
    "cp",
    when(
        length(trim(col("_cp_extraido"))) > 0,
        trim(col("_cp_extraido"))
    ).otherwise(None)
)

# --------------------------------------------------
# EXTRACCION DE ESTADO
# --------------------------------------------------

patron_estado = r"(?is)^.*(Estado de M[eé]xico|Huixquilucan).*$"

df = df.withColumn(
    "_estado_extraido",
    regexp_extract(
        col("_direccion_normalizada"),
        patron_estado,
        1
    )
)

df = df.withColumn(
    "estado",
    when(
        length(trim(col("_estado_extraido"))) > 0,
        trim(col("_estado_extraido"))
    ).otherwise(None)
)

# --------------------------------------------------
# ELIMINAR COLUMNAS AUXILIARES
# --------------------------------------------------

df_final = df.drop(
    "_direccion_normalizada",
    "_calle_extraida",
    "_numero_extraido",
    "_colonia_extraida",
    "_alcaldia_extraida",
    "_cp_extraido",
    "_estado_extraido"
)

# --------------------------------------------------
# VALIDACION DEL RESULTADO
# --------------------------------------------------

print("======================================")
print("RESULTADOS DEL PASO 05")
print("======================================")

print("Total de registros:", df_final.count())
print("Total de columnas:", len(df_final.columns))

print("Columnas finales:")
print(df_final.columns)

print("======================================")
print("VALIDACION DE CAMPOS DE DIRECCION")
print("======================================")

print(
    "Calles extraidas:",
    df_final.filter(col("calle").isNotNull()).count()
)

print(
    "Numeros extraidos:",
    df_final.filter(col("numero").isNotNull()).count()
)

print(
    "Colonias extraidas:",
    df_final.filter(col("colonia").isNotNull()).count()
)

print(
    "Alcaldias extraidas:",
    df_final.filter(col("alcaldia").isNotNull()).count()
)

print(
    "Codigos postales extraidos:",
    df_final.filter(col("cp").isNotNull()).count()
)

print(
    "Estados extraidos:",
    df_final.filter(col("estado").isNotNull()).count()
)

print("======================================")
print("ESTRUCTURA DEL DATAFRAME")
print("======================================")

df_final.printSchema()

print("======================================")
print("PASO 05 FINALIZADO")
print("======================================")

sc.stop()
print("SparkContext detenido.")