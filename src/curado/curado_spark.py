# -*- coding: utf-8 -*-
# CURADO SPARK — VERSION FINAL Y ESTABLE
# Lee:   data/normalizacion/normalizacion.xlsx
# Escribe: data/curado/curado.xlsx
# Sin UDFs, sin unicode

import os
import sys
import pandas as pd
from pyspark.sql import SparkSession, functions as F

def get_arg(name, default=None):
    for a in sys.argv[1:]:
        if a.startswith(name + "="):
            return a.split("=", 1)[1]
    return default


def first_existing(df, names):
    for n in names:
        if n in df.columns:
            return F.col(n)
    return None


def save_excel(df_spark, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pdf = df_spark.limit(100000).toPandas()
    pdf.to_excel(path, index=False, engine="openpyxl")
    print("[OK] Excel guardado en:", path)


def main():
    BASE_DIR = get_arg("base_dir", os.path.abspath("./data"))

    spark = (
        SparkSession.builder
        .appName("curado_spark_final")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ======================================================
    # Leer normalizacion.xlsx
    # ======================================================
    refined_excel = os.path.join(BASE_DIR, "normalizacion", "normalizacion.xlsx")
    print("[INFO] Leyendo NORMALIZACION Excel:", refined_excel)

    if not os.path.isfile(refined_excel):
        raise RuntimeError("No existe normalizacion.xlsx. Ejecuta la normalizacion primero.")

    pdf = pd.read_excel(refined_excel)
    df = spark.createDataFrame(pdf)

    # ======================================================
    # Deduplicacion
    # ======================================================
    key_cols = [c for c in ["tweet_id", "id", "id_str", "tweetId"] if c in df.columns]

    if key_cols:
        print("[INFO] Deduplicando por:", key_cols)
        df = df.dropDuplicates(key_cols)
    else:
        fallback = [c for c in ["fecha", "username", "ubicacion", "text_clean"] if c in df.columns]
        print("[INFO] Deduplicando por fallback:", fallback)
        df = df.dropDuplicates(fallback)

    # ======================================================
    # Columnas finales
    # ======================================================

    col_username = first_existing(df, ["username"])
    if col_username is None:
        col_username = F.lit(None).alias("username")
    else:
        col_username = col_username.alias("username")

    col_texto = first_existing(df, ["text_clean", "texto", "text"])
    if col_texto is None:
        col_texto = F.lit(None).alias("texto")
    else:
        col_texto = col_texto.alias("texto")

    col_ubi = first_existing(df, ["ubicacion"])
    if col_ubi is None:
        col_ubi = F.lit(None).alias("ubicacion")
    else:
        col_ubi = col_ubi.alias("ubicacion")

    col_unidades = first_existing(df, ["unidades"])
    if col_unidades is None:
        col_unidades = F.lit(None).alias("unidades")
    else:
        col_unidades = col_unidades.alias("unidades")

    col_tipo = first_existing(df, ["tipo_incidente"])
    if col_tipo is None:
        col_tipo = F.lit("No clasificado").alias("tipo_incidente")
    else:
        col_tipo = col_tipo.alias("tipo_incidente")

    df_out = df.select(
        "fecha",
        col_username,
        col_tipo,
        col_ubi,
        col_unidades,
        col_texto
    )

    # ======================================================
    # GUARDAR
    # ======================================================
    out_path = os.path.join(BASE_DIR, "curado", "curado.xlsx")
    print("[INFO] Exportando CURADO Excel ->", out_path)
    save_excel(df_out, out_path)

    print("[OK] Curado completado.")
    spark.stop()


if __name__ == "__main__":
    main()
