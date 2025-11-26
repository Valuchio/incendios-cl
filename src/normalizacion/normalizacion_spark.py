# -*- coding: utf-8 -*-
# NORMALIZACION SPARK — VERSION FINAL Y ESTABLE
# Lee:   data/ingesta/ingesta.xlsx
# Escribe: data/normalizacion/normalizacion.xlsx
# Sin UDFs, sin unicode, urls limpias

import os
import sys
import pandas as pd
from pyspark.sql import SparkSession, functions as F

def get_arg(name, default=None):
    for a in sys.argv[1:]:
        if a.startswith(name + "="):
            return a.split("=", 1)[1]
    return os.environ.get(name.upper(), default)

BASE_DIR    = get_arg("base_dir", os.path.abspath("./data"))
TZ          = get_arg("tz", "America/Santiago")

# =====================================================
# DEFINICIONES DE UNIDADES
# =====================================================
UNIT_REGEX = r"(?i)^(AP|BR|RX|BX|UR|RB|BT|CJ|QR|MX|BM|RH|B|Q|H|Z|M|A|U|R|K|X|S)[-]?[0-9]{1,3}$"

UNIT_MAP = {
    "B": "Carro Bomba",
    "Q": "Escalera Mecanica",
    "BR": "Brigada de Rescate",
    "RX": "Unidad de Rescate",
    "H": "HazMat",
    "Z": "Unidad de Mando",
    "M": "Motobomba",
    "A": "Ambulancia",
    "U": "Unidad Logistica",
    "BX": "Carro Bomba BX",
    "AP": "Autopluma",
    "UR": "Unidad Rescate UR",
    "RB": "Rescate/Bomba",
    "BT": "Bomba/Tanque",
    "CJ": "Comando/Jefatura",
    "QR": "Respuesta Rapida",
    "R": "Rescate",
    "K": "Codigo Local K",
    "X": "Codigo Local X",
    "MX": "Motobomba MX",
    "BM": "Bomba Movil",
    "RH": "Rehidratacion",
    "S": "Soporte"
}

# =====================================================
# GUARDAR EXCEL
# =====================================================
def save_excel(df_spark, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    pdf = df_spark.limit(100000).toPandas()
    pdf.to_excel(output_path, index=False, engine="openpyxl")
    print("[OK] Excel guardado en:", output_path)

# =====================================================
# MAIN
# =====================================================
def main():
    spark = (
        SparkSession.builder
        .appName("normalizacion_spark_final")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Leer archivo de ingesta
    ingesta_excel = os.path.join(BASE_DIR, "ingesta", "ingesta.xlsx")
    print("[INFO] Leyendo Excel INGESTA:", ingesta_excel)

    if not os.path.isfile(ingesta_excel):
        raise RuntimeError("No existe ingesta.xlsx. Ejecuta la ingesta primero.")

    pdf_raw = pd.read_excel(ingesta_excel)
    df = spark.createDataFrame(pdf_raw)

    # =====================================================
    # FECHA
    # =====================================================
    if "fecha" not in df.columns:
        df = df.withColumn(
            "fecha",
            F.to_date(F.from_utc_timestamp(F.to_timestamp("created_at"), TZ))
        )

    # =====================================================
    # LIMPIEZA DE TEXTO (ELIMINAR URLS)
    # =====================================================
    df = df.withColumn(
        "text_clean",
        F.regexp_replace(F.col("text"), r"https?://\S+", "")
    )

    df = df.withColumn(
        "text_clean",
        F.regexp_replace(F.col("text_clean"), r"[–—−]", "-")
    )

    df = df.withColumn(
        "text_clean",
        F.regexp_replace(F.col("text_clean"), r"\s+", " ")
    )

    df = df.withColumn("text_clean", F.trim(F.col("text_clean")))

    # Remover SALE / SALEN ... A ...
    df = df.withColumn(
        "text_core",
        F.regexp_replace(F.col("text_clean"), r"(?i)^\s*SALEN?\b.*?\bA\b\s+", "")
    )

    # =====================================================
    # EXTRAER UNIDADES (SIN UDFs)
    # =====================================================

    df = df.withColumn("tokens", F.split(F.upper(F.col("text_core")), " "))

    df = df.withColumn(
        "unidades",
        F.filter(F.col("tokens"), lambda x: x.rlike(UNIT_REGEX))
    )

    df = df.withColumn(
        "unidades",
        F.transform(
            F.col("unidades"),
            lambda x: F.regexp_replace(x, r"^([A-Z]+)([0-9]+)$", r"\1-\2")
        )
    )

    df = df.withColumn(
        "unidad_prefix",
        F.transform(F.col("unidades"), lambda x: F.regexp_extract(x, r"^([A-Z]+)", 1))
    )

    mapping_expr = F.create_map(
        *[x for kv in UNIT_MAP.items() for x in (F.lit(kv[0]), F.lit(kv[1]))]
    )

    df = df.withColumn(
        "unidades_desc",
        F.transform(F.col("unidad_prefix"), lambda p: mapping_expr.getItem(p))
    )

    # =====================================================
    # CLASIFICACIÓN TIPO
    # =====================================================
    utext = F.upper(F.col("text_core"))

    df = df.withColumn(
        "tipo_incidente",
        F.when(utext.like("%FUGA DE GAS%"), "Fuga de gas")
         .when(utext.rlike("HAZMAT|PELIGRO"), "Materiales peligrosos")
         .when(utext.rlike("DERRUMBE|COLAPSO"), "Derrumbe / Colapso")
         .when(utext.like("%EMERGENCIA TECNICA%"), "Emergencia tecnica")
         .when(utext.like("%RESCATE%") | utext.like("%ATENCION MEDICA%"), "Rescate / Atencion medica")
         .when(utext.rlike("VEHICUL"), "Incendio vehicular")
         .when(utext.rlike("FORESTAL|PASTIZAL|MATORRAL"), "Incendio forestal")
         .when(utext.rlike("ESTRUCTURAL|CASA|VIVIENDA|DEPARTAMENTO"), "Incendio estructural")
         .when(utext.like("%INCENDIO%"), "Incendio")
         .otherwise("Incidente")
    )

    # =====================================================
    # UBICACION
    # =====================================================

    df = df.withColumn(
        "ubicacion_raw",
        F.regexp_replace(F.col("text_core"), r"(?i)10[- ]?[0-9]+(?:[- ]?[0-9]+)?", " ")
    )

    df = df.withColumn(
        "ubicacion_raw",
        F.regexp_replace(F.col("ubicacion_raw"), UNIT_REGEX, " ")
    )

    df = df.withColumn(
        "ubicacion_raw",
        F.regexp_replace(F.col("ubicacion_raw"),
                         r"(?i)INCENDIO|EMERGENCIA|FORESTAL|ESTRUCTURAL|RESCATE", " ")
    )

    df = df.withColumn(
        "ubicacion_raw",
        F.trim(F.regexp_replace(F.col("ubicacion_raw"), r"\s+", " "))
    )

    df = df.withColumn(
        "ubicacion",
        F.when(F.length(F.col("ubicacion_raw")) > 0,
               F.concat(F.col("ubicacion_raw"), F.lit(", Chile")))
    )

    # =====================================================
    # SALIDA
    # =====================================================
    df_final = df.select(
        "fecha",
        "username",
        "tipo_incidente",
        "ubicacion",
        F.concat_ws(", ", "unidades").alias("unidades"),
        "text_clean"
    )

    out_path = os.path.join(BASE_DIR, "normalizacion", "normalizacion.xlsx")
    print("[INFO] Exportando NORMALIZACION Excel ->", out_path)
    save_excel(df_final, out_path)

    print("[OK] Normalizacion completada.")

    spark.stop()


if __name__ == "__main__":
    main()
