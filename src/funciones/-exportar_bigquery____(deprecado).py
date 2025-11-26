# -*- coding: utf-8 -*-
import os, sys
from pyspark.sql import SparkSession

def arg(name, default=None):
    for a in sys.argv[1:]:
        if a.startswith(name + "="):
            return a.split("=", 1)[1]
    return default

def latest_run_ts(root):
    if not os.path.isdir(root):
        return None
    runs = [d for d in os.listdir(root) if d.startswith("run_ts=")]
    if not runs:
        return None
    runs.sort(reverse=True)
    return runs[0].split("=", 1)[1]

def die(msg):
    print("[ERROR]", msg); sys.exit(1)

def main():
    BASE = arg("base_dir") or die("base_dir=... requerido")
    RUN  = arg("run_ts", "latest")
    PROJ = arg("project") or die("project=... requerido")
    DSET = arg("dataset") or die("dataset=... requerido")
    TAB  = arg("table")   or die("table=... requerido")
    CRED = arg("cred")    or die("cred=... requerido (ruta JSON)")

    if not os.path.isfile(CRED):
        die(f"cred no existe: {CRED}")

    # Refuerzos de entorno (ADC)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CRED
    os.environ["GOOGLE_CLOUD_PROJECT"] = PROJ
    os.environ["GCLOUD_PROJECT"] = PROJ

    curated_root = os.path.join(BASE, "curated", "incidentes")
    if RUN in ("", "latest", None):
        RUN = latest_run_ts(curated_root) or die(f"No hay run_ts=* en {curated_root}")

    in_path = os.path.join(curated_root, f"run_ts={RUN}")
    if not os.path.isdir(in_path):
        die(f"No existe run_ts={RUN} en {curated_root}")

    print("[INFO] Leyendo CURATED:", in_path)

    spark = (
        SparkSession.builder
        .appName("exportar_bigquery_direct")
        .config("spark.bigquery.project", PROJ)
        .config("spark.bigquery.parentProject", PROJ)
        .config("spark.bigquery.credentialsFile", CRED)
        .config("spark.bigquery.writeMethod", "direct")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # redundancia por si acaso
    for k, v in {
        "spark.bigquery.project": PROJ,
        "spark.bigquery.parentProject": PROJ,
        "spark.bigquery.credentialsFile": CRED,
    }.items():
        spark.conf.set(k, v)

    df = spark.read.parquet(in_path)

    wanted = ["fecha", "username", "tipo_incidente", "ubicacion", "unidades", "texto"]
    missing = [c for c in wanted if c not in df.columns]
    if missing:
        die("Faltan columnas en CURATED: " + ", ".join(missing))
    df = df.select(*wanted)

    # Tabla totalmente calificada
    table_fq = f"{PROJ}.{DSET}.{TAB}"

    print(f"[INFO] Exportando a {table_fq} (append)")

    (df.write
        .format("bigquery")
        .option("table", table_fq)              # project.dataset.table
        .option("project", PROJ)
        .option("parentProject", PROJ)
        .option("credentialsFile", CRED)
        .mode("append")
        .save())

    print("[OK] Exportación finalizada.")
    spark.stop()

if __name__ == "__main__":
    main()
