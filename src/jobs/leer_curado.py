# -*- coding: utf-8 -*-
"""
Lector de CURATED tolerante a particiones mezcladas.
- Por defecto lee SOLO el último run_ts.
- Con run_ts=all, lee TODOS los run_ts y convierte created_date -> fecha.

Uso:
  spark-submit src/jobs/leer_curado.py base_dir=C:/proyectos/incendios-cl/data n=50
  spark-submit src/jobs/leer_curado.py base_dir=C:/proyectos/incendios-cl/data n=50 run_ts=all
  spark-submit src/jobs/leer_curado.py base_dir=C:/proyectos/incendios-cl/data n=50 run_ts=2025-08-26_222909
"""

import os, sys, re
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType, DateType

def get_arg(name, default):
    for a in sys.argv[1:]:
        if a.startswith(name + "="):
            return a.split("=", 1)[1]
    return default

def list_run_dirs(root):
    """Devuelve [(run_ts_str, full_path)] ordenados por fecha asc."""
    out = []
    if not os.path.isdir(root):
        return out
    for d in os.listdir(root):
        if d.startswith("run_ts="):
            ts = d.split("run_ts=", 1)[1]
            full = os.path.join(root, d)
            # validar formato YYYY-MM-DD_HHMMSS
            if re.match(r"^\d{4}-\d{2}-\d{2}_\d{6}$", ts) and os.path.isdir(full):
                out.append((ts, full))
    out.sort(key=lambda x: x[0])  # lexicográfico sirve para este formato
    return out

def std_df(df):
    """Estandariza columnas para que todos los runs puedan unirse."""
    # fecha (DATE)
    if "fecha" not in df.columns:
        if "created_date" in df.columns:
            df = df.withColumn("fecha", F.col("created_date").cast(DateType()))
        elif "created_at_ts" in df.columns:
            df = df.withColumn("fecha", F.to_date(F.col("created_at_ts")))
        else:
            df = df.withColumn("fecha", F.lit(None).cast(DateType()))
    else:
        df = df.withColumn("fecha", F.col("fecha").cast(DateType()))

    # created_at_ts (TIMESTAMP)
    if "created_at_ts" not in df.columns:
        if "created_date" in df.columns:
            df = df.withColumn(
                "created_at_ts",
                F.to_timestamp(F.concat_ws(" ", F.col("created_date").cast("string"), F.lit("00:00:00")))
            )
        else:
            df = df.withColumn("created_at_ts", F.lit(None).cast(TimestampType()))
    else:
        df = df.withColumn("created_at_ts", F.col("created_at_ts").cast(TimestampType()))

    # username (STRING)
    if "username" not in df.columns:
        for cand in ["user","author_username"]:
            if cand in df.columns:
                df = df.withColumn("username", F.col(cand).cast(StringType()))
                break
        if "username" not in df.columns:
            df = df.withColumn("username", F.lit(None).cast(StringType()))
    else:
        df = df.withColumn("username", F.col("username").cast(StringType()))

    # tipo_incidente (STRING)
    if "tipo_incidente" not in df.columns:
        for cand in ["incident_type","tipo","clasificacion"]:
            if cand in df.columns:
                df = df.withColumn("tipo_incidente", F.col(cand).cast(StringType()))
                break
        if "tipo_incidente" not in df.columns:
            # derivar desde texto si existe
            base_text = None
            for cand in ["texto","text_clean","text","tweet","tweet_text"]:
                if cand in df.columns:
                    base_text = F.upper(F.col(cand))
                    break
            if base_text is not None:
                df = df.withColumn("tipo_incidente",
                                   F.when(base_text.contains(F.lit("INCENDIO")), F.lit("Incendio"))
                                    .otherwise(F.lit("No incendio (sin clasificar)")))
            else:
                df = df.withColumn("tipo_incidente", F.lit("No incendio (sin clasificar)"))
    else:
        df = df.withColumn("tipo_incidente", F.col("tipo_incidente").cast(StringType()))

    # ubicacion (STRING)
    if "ubicacion" not in df.columns:
        for cand in ["location","direccion"]:
            if cand in df.columns:
                df = df.withColumn("ubicacion", F.col(cand).cast(StringType()))
                break
        if "ubicacion" not in df.columns:
            df = df.withColumn("ubicacion", F.lit(None).cast(StringType()))
    else:
        df = df.withColumn("ubicacion", F.col("ubicacion").cast(StringType()))

    # unidades (STRING; si es array -> concat_ws)
    if "unidades" in df.columns:
        if dict(df.dtypes)["unidades"].startswith("array"):
            df = df.withColumn("unidades", F.concat_ws(", ", "unidades"))
        else:
            df = df.withColumn("unidades", F.col("unidades").cast(StringType()))
    else:
        df = df.withColumn("unidades", F.lit(None).cast(StringType()))

    # texto (STRING)
    if "texto" not in df.columns:
        for cand in ["text_clean","text","tweet","tweet_text"]:
            if cand in df.columns:
                df = df.withColumn("texto", F.col(cand).cast(StringType()))
                break
        if "texto" not in df.columns:
            df = df.withColumn("texto", F.lit(None).cast(StringType()))
    else:
        df = df.withColumn("texto", F.col("texto").cast(StringType()))

    # Selección final ordenada
    return df.select("fecha","created_at_ts","username","tipo_incidente","ubicacion","unidades","texto")

def main():
    BASE_DIR = get_arg("base_dir", os.path.join(".", "data"))
    N        = int(get_arg("n", "50"))
    RUN_SEL  = get_arg("run_ts", "latest")  # 'latest' | 'all' | 'YYYY-MM-DD_HHMMSS'

    spark = (SparkSession.builder.appName("leer_curado_incendios").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    root = os.path.join(BASE_DIR, "curated", "incidentes")
    if not os.path.isdir(root):
        raise RuntimeError(f"No existe la carpeta: {root}")

    runs = list_run_dirs(root)
    if not runs:
        raise RuntimeError(f"No encontré subcarpetas run_ts=* dentro de {root}")

    if RUN_SEL == "latest":
        run = runs[-1]  # último
        path = run[1]
        print(f"[INFO] Leyendo SOLO el último run_ts: {run[0]} -> {path}")
        df = spark.read.parquet(path)
        df = std_df(df)
    elif RUN_SEL == "all":
        print(f"[INFO] Leyendo y uniendo TODOS los run_ts ({len(runs)})\n" +
              ", ".join([r for r,_ in runs]))
        dfs = []
        for ts, path in runs:
            try:
                dfi = spark.read.parquet(path)
                dfs.append(std_df(dfi))
            except Exception as e:
                print(f"[WARN] No pude leer {path}: {e}")
        if not dfs:
            raise RuntimeError("No pude leer ningún run_ts.")
        # Unión segura por nombre (todas las dfs tienen las mismas columnas con std_df)
        df = dfs[0]
        for d in dfs[1:]:
            df = df.unionByName(d)
    else:
        # leer un run_ts específico
        matches = [p for ts,p in runs if ts == RUN_SEL]
        if not matches:
            raise RuntimeError(f"No existe el run_ts={RUN_SEL} en {root}")
        path = matches[0]
        print(f"[INFO] Leyendo run_ts={RUN_SEL} -> {path}")
        df = spark.read.parquet(path)
        df = std_df(df)

    if df.rdd.isEmpty():
        print("[WARN] CURATED está vacío tras estandarizar.")
        spark.stop()
        return

    # Mostrar
    print(f"[INFO] Mostrando {N} filas (orden: fecha desc, created_at_ts desc)")
    (df.orderBy(F.col("fecha").desc(), F.col("created_at_ts").desc())
       .show(N, truncate=False))

    total = df.count()
    print(f"[OK] Filas totales: {total}")
    spark.stop()

if __name__ == "__main__":
    main()
