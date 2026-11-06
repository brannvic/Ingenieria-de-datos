
# PROCESO ETL COMPLETO CON PYSPARK 2.4.8
# Incluye 5 consultas SQL y visualizacion de la tabla completa

# 1. Configuracion del entorno
import os
import sys

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]
os.environ["PYSPARK_PYTHON"] = sys.executable

# 2. Importar Spark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import (
    col, trim, split, regexp_replace,
    regexp_extract, lit
)

# 3. Rutas de los archivos
ruta_entrada = "/home/victoria/Proyecto_ETL/Spark/datos/Datos_Grupo - Hoja 1.csv"

ruta_salida = "/home/victoria/Proyecto_ETL/Spark/resultados/datos_limpios_csv"

# 4. Configurar Spark
configuracion = SparkConf()
configuracion.setAppName("Proceso ETL Completo")
configuracion.setMaster("local[2]")

sc = SparkContext(conf=configuracion)
sqlContext = SQLContext(sc)

try:

    print("\nINICIO DEL PROCESO ETL")
    print("--------------------------------")

    # 5. Leer el archivo CSV
    datos = sqlContext.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("encoding", "UTF-8") \
        .csv(ruta_entrada)

    print("Registros originales:", datos.count())

    # 6. Cambiar nombres de columnas y limpiar espacios
    datos = datos.select(
        trim(col("Nombre")).alias("nombre"),
        trim(col("Direccion")).alias("direccion"),
        trim(col("Nombre de Mascota")).alias("nombre_mascota"),
        trim(col("Apodo de 2 mejores amigos")).alias("apodo_mejores_amigos"),
        trim(col("CURP")).alias("curp"),
        trim(col("Pasatiempo")).alias("pasatiempo"),
        trim(col("Carrera")).alias("carrera"),
        trim(col("Facultad")).alias("facultad"),
        trim(col("Cantante favorito")).alias("cantante_favorito"),
        col("Edad").cast("integer").alias("edad"),
        trim(col("Banda Favorita")).alias("banda_favorita")
    )

    # 7. Separar mascotas, apodos y bandas
    datos = datos.withColumn(
        "mascota_1", trim(split(col("nombre_mascota"), "y").getItem(0))
    ).withColumn(
        "mascota_2", trim(split(col("nombre_mascota"), "y").getItem(1))
    ).withColumn(
        "apodo_1", trim(split(col("apodo_mejores_amigos"), "y").getItem(0))
    ).withColumn(
        "apodo_2", trim(split(col("apodo_mejores_amigos"), "y").getItem(1))
    ).withColumn(
        "banda_1", trim(split(col("banda_favorita"), r"\+").getItem(0))
    ).withColumn(
        "banda_2", trim(split(col("banda_favorita"), r"\+").getItem(1))
    )

    # 8. Aplicar las correcciones de nombres de Apache Hop
    datos = datos.withColumn(
        "nombre",
        regexp_replace(
            col("nombre"),
            "(?i)Diego Varela Jimenez",
            "Varela Jimenez Diego"
        )
    )

    datos = datos.withColumn(
        "nombre",
        regexp_replace(
            col("nombre"),
            "(?i)Axel Zarza",
            "Zarza SIN_MATERNO Axel"
        )
    )

    datos = datos.withColumn(
        "nombre",
        regexp_replace(
            col("nombre"),
            "(?i)San Agustin",
            "San_Agustin"
        )
    )

    # 9. Separar apellidos y nombres
    datos = datos.withColumn(
        "apellido_paterno",
        regexp_extract(col("nombre"), r"^(\S+)\s+(\S+)\s+(.+)$", 1)
    ).withColumn(
        "apellido_materno",
        regexp_extract(col("nombre"), r"^(\S+)\s+(\S+)\s+(.+)$", 2)
    ).withColumn(
        "nombres",
        regexp_extract(col("nombre"), r"^(\S+)\s+(\S+)\s+(.+)$", 3)
    )

    # 10. Normalizar las direcciones
    datos = datos.withColumn(
        "dir_norm",
        regexp_replace(col("direccion"), r"(?i)\bDel\.\s*", "")
    )

    datos = datos.withColumn(
        "dir_norm",
        regexp_replace(
            col("dir_norm"),
            r"(?i)\b(Av|And|Col|Mz|Lt)\.\s*",
            "$1"
        )
    )

    datos = datos.withColumn(
        "dir_norm",
        regexp_replace(col("dir_norm"), r"\.\s*", ",")
    )

    datos = datos.withColumn(
        "dir_norm",
        regexp_replace(col("dir_norm"), r"\s*,\s*", ",")
    )

    # 11. Extraer calle
    patron_calle = (
        r"(?is)^(?!(?:Xochimilco|Coyoac[aá]n|Iztapalapa|Tlalpan|"
        r"Iztacalco|GAM)\s*(?:,|$))([^,]+?)"
        r"(?:\s*#?\s*(?:Mz\.?\s*\d+\s*Lt\.?\s*\d+|"
        r"lote\s*\d+|\d{1,4}[A-Za-z]?))?\s*(?:,.*)?$"
    )

    datos = datos.withColumn(
        "calle",
        regexp_extract(col("dir_norm"), patron_calle, 1)
    )

    # 12. Extraer numero
    datos = datos.withColumn(
        "numero",
        regexp_extract(
            col("dir_norm"),
            r"(?i)^[^,]*?(?:\s|#)(Mz\.?\s*\d+\s*Lt\.?\s*\d+|\d{1,4}[A-Za-z]?)\s*(?:,.*)?$",
            1
        )
    )

    # 13. Extraer colonia
    datos = datos.withColumn(
        "colonia",
        regexp_extract(
            col("dir_norm"),
            r"(?i)^[^,]+,\s*(?:lote\s*\d+,\s*)?(?:col(?:onia)?\.?\s+)?(?!\d|CDMX|Ciudad de|Mexico City|Estado de|GAM)([^,]+?)\s*(?:,.*)?$",
            1
        )
    )

    # 14. Extraer alcaldia
    datos = datos.withColumn(
        "alcaldia",
        regexp_extract(
            col("dir_norm"),
            r"(?isu)^(?:.*[\s,])?(Álvaro Obregón|Alvaro Obregon|Azcapotzalco|Benito Juárez|Benito Juarez|Coyoacán|Coyoacan|Cuajimalpa|Cuauhtémoc|Cuauhtemoc|GAM|Gustavo A\.? Madero|Iztacalco|Iztapalapa|Magdalena Contreras|Miguel Hidalgo|Milpa Alta|Tláhuac|Tlahuac|Tlalpan|Venustiano Carranza|Xochimilco|Huixquilucan)(?:[\s,].*)?$",
            1
        )
    )

    # 15. Extraer codigo postal
    datos = datos.withColumn(
        "cp",
        regexp_extract(
            col("dir_norm"),
            r"(?s)^.*?\b(\d{5})\b.*$",
            1
        )
    )

    # 16. Aplicar el valor predeterminado del flujo Hop
    datos = datos.withColumn(
        "estado",
        lit("Ciudad de Mexico")
    )

    # 17. Seleccionar las 21 columnas finales
    datos_finales = datos.select(
        "mascota_1",
        "mascota_2",
        "apodo_1",
        "apodo_2",
        "curp",
        "pasatiempo",
        "carrera",
        "facultad",
        "cantante_favorito",
        "edad",
        "banda_1",
        "banda_2",
        "apellido_paterno",
        "apellido_materno",
        "nombres",
        "calle",
        "numero",
        "colonia",
        "alcaldia",
        "cp",
        "estado"
    )

    # 18. Registrar el DataFrame como tabla SQL
    datos_finales.createOrReplaceTempView("datos_limpios")

    # 19. Validar el resultado antes de guardarlo
    print("\nRESULTADO DE LA TRANSFORMACION")
    print("--------------------------------")
    print("Registros:", datos_finales.count())
    print("Columnas:", len(datos_finales.columns))

    # 20. CONSULTA SQL 1: TOTAL DE REGISTROS
    print("\nCONSULTA SQL 1 - TOTAL DE REGISTROS")
    print("--------------------------------")

    consulta_1 = sqlContext.sql("""
        SELECT COUNT(*) AS total_registros
        FROM datos_limpios
    """)

    consulta_1.show()

    # 21. CONSULTA SQL 2: EDAD PROMEDIO
    print("\nCONSULTA SQL 2 - EDAD PROMEDIO")
    print("--------------------------------")

    consulta_2 = sqlContext.sql("""
        SELECT ROUND(AVG(edad), 2) AS edad_promedio
        FROM datos_limpios
    """)

    consulta_2.show()

    # 22. CONSULTA SQL 3: REGISTROS POR ALCALDIA
    print("\nCONSULTA SQL 3 - REGISTROS POR ALCALDIA")
    print("--------------------------------")

    consulta_3 = sqlContext.sql("""
        SELECT
            alcaldia,
            COUNT(*) AS total_registros
        FROM datos_limpios
        GROUP BY alcaldia
        ORDER BY total_registros DESC
    """)

    consulta_3.show(100, truncate=False)

    # 23. CONSULTA SQL 4: PASATIEMPOS MAS FRECUENTES
    print("\nCONSULTA SQL 4 - PASATIEMPOS MAS FRECUENTES")
    print("--------------------------------")

    consulta_4 = sqlContext.sql("""
        SELECT
            pasatiempo,
            COUNT(*) AS total_personas
        FROM datos_limpios
        WHERE pasatiempo IS NOT NULL
          AND TRIM(pasatiempo) <> ''
        GROUP BY pasatiempo
        ORDER BY total_personas DESC
    """)

    consulta_4.show(100, truncate=False)

    # 24. CONSULTA SQL 5: BANDAS FAVORITAS
    print("\nCONSULTA SQL 5 - BANDAS FAVORITAS")
    print("--------------------------------")

    consulta_5 = sqlContext.sql("""
        SELECT
            banda_1,
            COUNT(*) AS total_personas
        FROM datos_limpios
        WHERE banda_1 IS NOT NULL
          AND TRIM(banda_1) <> ''
        GROUP BY banda_1
        ORDER BY total_personas DESC
    """)

    consulta_5.show(100, truncate=False)

    # 25. MOSTRAR LA TABLA COMPLETA
    print("\nTABLA COMPLETA DE DATOS LIMPIOS")
    print("--------------------------------")
    print("Se muestran todos los registros y las 21 columnas.")

    datos_finales.show(100, truncate=False)

    # 26. Guardar el resultado como CSV
    datos_finales.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(ruta_salida)

    print("\nARCHIVO GUARDADO")
    print("--------------------------------")
    print("Ruta:", ruta_salida)

    # 27. Leer nuevamente el archivo para comprobarlo
    comprobacion = sqlContext.read \
        .schema(datos_finales.schema) \
        .option("header", "true") \
        .csv(ruta_salida)
finally:
    sc.stop()
    print("\nSparkContext detenido.")