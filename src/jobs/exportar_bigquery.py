# -*- coding: utf-8 -*-
"""
Exporta CURATED -> BigQuery SIN bucket (writeMethod=direct).
Lee:   {base_dir}/curated/incidentes/run_ts=YYYY-MM-DD_HHMMSS/fecha=YYYY-MM-DD/*.parquet
Escribe: project.dataset.table (particionada por 'fecha', cluster por tipo_incidente, username)

Uso típico:
  spark-submit --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.0 ^
    src/jobs/exportar_bigquery.py ^
    project=sturdy-web-470301-r5 dataset=incendios_cl table=incidentes ^
    base_dir="C:/proyectos/incendios-cl/data" run_ts=latest
"""

import os, sys, re
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType, TimestampType, DateType

def arg(name, default):
    for a in sys.argv[1:]:
        if a.startswith(name + "="):
            return a.split("=", 1)[1]
    return default

def list_runs(root):
    """Lista las carpetas run_ts=* ordenadas (lex asc)."""
    out = []
    if os.path.isdir(root):
        for d in os.listdir(root):
            if d.startswith("run_ts="):
                ts = d.split("run_ts=", 1)[1]
                if re.match(r"^\d{4}-\d{2}-\d{2}_\d{6}$", ts) and os.path.isdir(os.path.join(root, d)):
                    out.append((ts, os.path.join(root, d)))
    out.sort(key=lambda x: x[0])
    return out

def std_df(df):
    """Estandariza columnas/tipos esperados y selecciona el esquema final."""
    # fecha (DATE)
    if "fecha" not in df.columns:
        if "created_at_ts" in df.columns:
            df = df.withColumn("fecha", F.to_date(F.col("created_at_ts")))
        else:
            df = df.withColumn("fecha", F.lit(None).cast(DateType()))
    else:
        df = df.withColumn("fecha", F.col("fecha").cast(DateType()))

    # created_at_ts (TIMESTAMP)
    if "created_at_ts" not in df.columns:
        df = df.withColumn(
            "created_at_ts",
            F.to_timestamp(F.concat_ws(" ", F.col("fecha").cast("string"), F.lit("00:00:00")))
        )
    else:
        df = df.withColumn("created_at_ts", F.col("created_at_ts").cast(TimestampType()))

    # username
    if "username" not in df.columns:
        df = df.withColumn("username", F.lit(None).cast(StringType()))
    else:
        df = df.withColumn("username", F.col("username").cast(StringType()))

    # tipo_incidente
    if "tipo_incidente" not in df.columns:
        df = df.withColumn("tipo_incidente", F.lit(None).cast(StringType()))
    else:
        df = df.withColumn("tipo_incidente", F.col("tipo_incidente").cast(StringType()))

    # ubicacion
    if "ubicacion" not in df.columns:
        df = df.withColumn("ubicacion", F.lit(None).cast(StringType()))
    else:
        df = df.withColumn("ubicacion", F.col("ubicacion").cast(StringType()))

    # unidades (string; si viene array -> csv)
    if "unidades" in df.columns:
        if dict(df.dtypes)["unidades"].startswith("array"):
            df = df.withColumn("unidades", F.concat_ws(", ", "unidades"))
        else:
            df = df.withColumn("unidades", F.col("unidades").cast(StringType()))
    else:
        df = df.withColumn("unidades", F.lit(None).cast(StringType()))

    # texto
    if "texto" not in df.columns:
        df = df.withColumn("texto", F.lit(None).cast(StringType()))
    else:
        df = df.withColumn("texto", F.col("texto").cast(StringType()))

    # Esquema final (incluye tweet_id si existe para futuros MERGE)
    cols = ["fecha", "created_at_ts", "username", "tipo_incidente", "ubicacion", "unidades", "texto"]
    if "tweet_id" in df.columns:
        cols.insert(2, "tweet_id")
    return df.select(*[c for c in cols if c in df.columns])

def main():
    BASE  = arg("base_dir", os.path.join(".", "data"))
    RUN   = arg("run_ts", "latest")   # latest | YYYY-MM-DD_HHMMSS
    PROJ  = arg("project", None)
    DATA  = arg("dataset", None)
    TABLE = arg("table", "incidentes")
    MODE  = arg("mode", "append")     # append | overwrite

    if not PROJ or not DATA:
        raise RuntimeError("Pasa project=<PROJECT_ID> y dataset=<DATASET>.")

    spark = (SparkSession.builder.appName("exportar_bigquery_direct").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    root = os.path.join(BASE, "curated", "incidentes")
    runs = list_runs(root)
    if not runs:
        raise RuntimeError(f"No hay run_ts en {root}")

    if RUN == "latest":
        ts, path = runs[-1]
    else:
        match = [p for ts, p in runs if ts == RUN]
        if not match:
            raise RuntimeError(f"No existe run_ts={RUN} en {root}")
        path = match[0]

    print(f"[INFO] Leyendo CURATED: {path}")
    df = spark.read.parquet(path)
    df = std_df(df)

    if df.rdd.isEmpty():
        print("[WARN] DataFrame vacío. Nada que exportar.")
        spark.stop(); return

    bq_opts = {
        "table": f"{PROJ}.{DATA}.{TABLE}",
        "writeMethod": "direct",         # SIN bucket
        "partitionField": "fecha",       # partición por fecha (DATE)
        "partitionType": "DAY",
        "allowFieldAddition": "true",
        "allowFieldRelaxation": "true",
        "clusteredFields": "tipo_incidente,username"
    }

    print(f"[INFO] Exportando → {bq_opts['table']}  (mode={MODE})")
    (df.write.format("bigquery")
       .mode(MODE)       # append por defecto
       .options(**bq_opts)
       .save())

    print("[OK] Exportación completada.")
    spark.stop()

if __name__ == "__main__":
    main()
