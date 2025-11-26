# -*- coding: utf-8 -*-
# leer_curado.py actualizado a nueva estructura

import os
import sys
import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DateType, StringType
from pyspark.sql import Window


def get_arg(name, default=None):
    for a in sys.argv[1:]:
        if a.startswith(name + "="):
            return a.split("=", 1)[1]
    return default


def std_df(df):
    for c, t in [
        ("fecha", DateType()),
        ("username", StringType()),
        ("tipo_incidente", StringType()),
        ("ubicacion", StringType()),
        ("unidades", StringType()),
        ("texto", StringType())
    ]:
        df = df.withColumn(c, F.col(c).cast(t)) if c in df.columns else df.withColumn(c, F.lit(None).cast(t))

    return df.select("fecha", "username", "tipo_incidente", "ubicacion", "unidades")


def main():
    base_dir = get_arg("base_dir", "./data")
    n = int(get_arg("n", 50))

    spark = SparkSession.builder.appName("leer_curado").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    path_excel = os.path.join(base_dir, "curado", "curado.xlsx")
    print("[INFO] Leyendo Excel CURADO:", path_excel)

    pdf = pd.read_excel(path_excel)
    df = spark.createDataFrame(pdf)

    df = std_df(df)
    df_sorted = df.orderBy(F.col("fecha").desc())

    w = Window.orderBy(F.col("fecha").desc())
    df_enum = df_sorted.withColumn("n", F.row_number().over(w))

    df_enum.select("n", "fecha", "username", "tipo_incidente", "ubicacion", "unidades") \
        .show(n, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
