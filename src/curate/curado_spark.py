# -*- coding: utf-8 -*-
"""
CURADO robusto:
- Quita 'codigo_incidente' / 'evento_del_dia'
- Deriva 'fecha' (DATE) desde created_at_ts (UTC->TZ) o fallback created_date
- Si falta 'tipo_incidente', lo deriva del texto (INCENDIO => "Incendio")
- DEDUP se hace ANTES del select final (evita error de tweet_id faltante)
- Escribe CURATED particionado por 'fecha'

Uso:
  spark-submit src/curate/curado_spark.py base_dir=C:/proyectos/incendios-cl/data dataset=x tz=America/Santiago
"""

import os, sys
from datetime import datetime, timezone
from pyspark.sql import SparkSession, functions as F

def get_arg(name, default):
    for a in sys.argv[1:]:
        if a.startswith(name + "="):
            return a.split("=", 1)[1]
    return default

def first_col_name(df, names):
    """Devuelve el primer NOMBRE de columna existente en df o None."""
    for n in names:
        if n in df.columns:
            return n
    return None

def first_col(df, names):
    """Devuelve la primera COLUMNA existente en df o None."""
    n = first_col_name(df, names)
    return F.col(n) if n else None

def main():
    BASE_DIR = get_arg("base_dir", os.path.join(".", "data"))
    DATASET  = get_arg("dataset", "x")
    TZ       = get_arg("tz", "America/Santiago")
    RUN_TS   = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d_%H%M%S")

    spark = (SparkSession.builder.appName("curado_incidentes").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    refined_root = os.path.join(BASE_DIR, "refined", DATASET)
    print(f"[INFO] Leyendo REFINED desde: {refined_root}")
    df = spark.read.parquet(refined_root)

    if df.rdd.isEmpty():
        print("[WARN] REFINED está vacío. Nada que curar.")
        spark.stop()
        return

    # 1) Quitar columnas que no van a BI
    drop_cols = [c for c in ["codigo_incidente","incident_code","evento_del_dia","incident_seq"] if c in df.columns]
    if drop_cols:
        print(f"[INFO] Eliminando columnas: {drop_cols}")
        df = df.drop(*drop_cols)

    # 2) Derivar 'fecha' y asegurar 'created_at_ts'
    if "created_at_ts" in df.columns:
        df = df.withColumn("fecha", F.to_date(F.from_utc_timestamp(F.col("created_at_ts"), TZ)))
    elif "created_date" in df.columns:
        df = df.withColumn("fecha", F.col("created_date").cast("date"))
        df = df.withColumn("created_at_ts",
                           F.to_timestamp(F.concat_ws(" ", F.col("fecha").cast("string"), F.lit("00:00:00"))))
    else:
        raise RuntimeError("No encuentro 'created_at_ts' ni 'created_date' en REFINED para derivar 'fecha'.")

    # 3) DEDUP ANTES del select final
    key_candidates = [c for c in ["tweet_id","id","id_str","tweetId"] if c in df.columns]
    if key_candidates:
        print(f"[INFO] Deduplicando por {key_candidates}")
        df = df.dropDuplicates(key_candidates)
    else:
        # fallback: usar columnas disponibles
        user_name_col = first_col_name(df, ["username","user","author_username"])
        fallback_keys = [c for c in ["created_at_ts", user_name_col, "ubicacion", "texto", "text_clean", "text"] if c and c in df.columns]
        print(f"[INFO] Deduplicando por fallback {fallback_keys}")
        if fallback_keys:
            df = df.dropDuplicates(fallback_keys)

    # 4) Columnas base con alias/fallbacks
    username_c = first_col(df, ["username","user","author_username"])
    if username_c is None:
        raise RuntimeError("No encuentro columna de usuario ('username'/'user'/'author_username').")
    username = username_c.cast("string").alias("username")

    raw_text_c = first_col(df, ["texto","text_clean","text","tweet","tweet_text"])
    texto = (raw_text_c.cast("string").alias("texto")) if raw_text_c is not None else F.lit(None).cast("string").alias("texto")

    ubicacion_c = first_col(df, ["ubicacion","location","direccion"])
    ubicacion = (ubicacion_c.cast("string").alias("ubicacion")) if ubicacion_c is not None else F.lit(None).cast("string").alias("ubicacion")

    # unidades: array<string> o string
    if "unidades" in df.columns and dict(df.dtypes)["unidades"].startswith("array"):
        unidades = F.concat_ws(", ", F.col("unidades")).alias("unidades")
    else:
        unidades = (F.col("unidades").cast("string").alias("unidades")) if "unidades" in df.columns else F.lit(None).cast("string").alias("unidades")

    # tipo_incidente: usar columna si existe, si no derivar del texto
    tipo_directo_c = first_col(df, ["tipo_incidente","incident_type","tipo","clasificacion"])
    if tipo_directo_c is not None:
        tipo_inc = tipo_directo_c.cast("string").alias("tipo_incidente")
    else:
        base_text = first_col(df, ["texto","text_clean","text","tweet","tweet_text"])
        if base_text is not None:
            tipo_inc = (F.when(F.upper(base_text).contains(F.lit("INCENDIO")), F.lit("Incendio"))
                          .otherwise(F.lit("No incendio (sin clasificar)"))
                          .alias("tipo_incidente"))
        else:
            tipo_inc = F.lit("No incendio (sin clasificar)").alias("tipo_incidente")

    # 5) SELECT final (ya deduplicado)
    df_out = df.select(
        F.col("created_at_ts"),
        F.col("fecha"),
        username,
        tipo_inc,
        ubicacion,
        unidades,
        texto
    )

    # 6) Escribir CURATED particionado por fecha
    out_cur = os.path.join(BASE_DIR, "curated", "incidentes", f"run_ts={RUN_TS}")
    print(f"[INFO] Escribiendo CURATED -> {out_cur}")
    (df_out.repartition("fecha")
          .write.mode("overwrite")
          .partitionBy("fecha")
          .parquet(out_cur))

    print("[OK] CURATED escrito correctamente.")
    spark.stop()

if __name__ == "__main__":
    main()
