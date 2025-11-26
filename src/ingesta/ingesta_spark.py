# -*- coding: utf-8 -*-
# INGESTA SPARK — VERSION CON PARAMETROS (COMPATIBLE CON spark-submit y .BAT)
# Para ejecutar:
# spark-submit src/ingesta/ingesta_spark.py bearer="TOKEN" user="USUARIO" max=100 base_dir="C:\...\data"

import os
import sys
import requests
import pandas as pd
from datetime import datetime, timezone
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StringType, BooleanType


# ================================================
# LECTURA DE PARAMETROS
# ================================================
def get_arg(name, default=None):
    for a in sys.argv[1:]:
        if a.startswith(name + "="):
            return a.split("=", 1)[1]
    return default


BEARER = get_arg("bearer")
USER_SINGLE = get_arg("user")
MAX_RESULTS = get_arg("max")
BASE_DIR = get_arg("base_dir", os.path.abspath("./data"))
TZ = get_arg("tz", "America/Santiago")


# ================================================
# VALIDACIONES
# ================================================
if not BEARER:
    raise RuntimeError("Falta el parametro bearer=TOKEN")

if not USER_SINGLE:
    raise RuntimeError("Falta el parametro user=USUARIO")

if not MAX_RESULTS:
    raise RuntimeError("Falta el parametro max=NUMERO")

MAX_RESULTS = int(MAX_RESULTS)

USERS = [USER_SINGLE]

print("==============================================")
print(" INGESTA CONFIGURADA")
print(" BEARER (oculto)  :", BEARER[:6] + "...")
print(" USUARIO          :", USERS)
print(" MAX_RESULTS      :", MAX_RESULTS)
print(" BASE_DIR         :", BASE_DIR)
print("==============================================\n")


# ================================================
# FUNCIONES AUXILIARES
# ================================================
def save_excel(df_spark, excel_path):
    """Guarda el resultado a Excel."""
    os.makedirs(os.path.dirname(excel_path), exist_ok=True)
    pdf = df_spark.toPandas()
    pdf.to_excel(excel_path, index=False, engine="openpyxl")
    print("[OK] Excel guardado en:", excel_path)


def http_get(url, params):
    """Request a la API de X (Twitter)."""
    r = requests.get(url, headers={"Authorization": f"Bearer {BEARER}"}, params=params)
    if r.status_code != 200:
        raise RuntimeError(f"Error HTTP {r.status_code}: {r.text}")
    return r.json()


RAW_SCHEMA = (
    StructType()
        .add("tweet_id", StringType())
        .add("author_id", StringType())
        .add("username", StringType())
        .add("name", StringType())
        .add("verified", BooleanType())
        .add("created_at", StringType())
        .add("text", StringType())
        .add("lang", StringType())
        .add("ingest_ts", StringType())
        .add("source_username", StringType())
)


# ================================================
# MAIN
# ================================================
def main():
    spark = (
        SparkSession.builder
        .appName("ingesta_parametrizada")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    rows = []

    for u in USERS:
        print("[INFO] Consultando usuario:", u)

        # Obtener ID del usuario
        user_data = http_get(
            f"https://api.x.com/2/users/by/username/{u}",
            {"user.fields": "username,name,verified"}
        )["data"]

        uid = user_data["id"]

        # Obtener tweets
        print("[INFO] Obteniendo tweets de:", u)

        payload = http_get(
            f"https://api.x.com/2/users/{uid}/tweets",
            {
                "max_results": MAX_RESULTS,
                "exclude": "retweets,replies",
                "tweet.fields": "created_at,lang",
                "expansions": "author_id",
                "user.fields": "username,name,verified",
            }
        )

        users_map = {x["id"]: x for x in payload.get("includes", {}).get("users", [])}

        for t in payload.get("data", []):
            author = users_map.get(t.get("author_id"), user_data)

            rows.append({
                "tweet_id": t.get("id"),
                "author_id": t.get("author_id"),
                "username": author.get("username"),
                "name": author.get("name"),
                "verified": author.get("verified"),
                "created_at": t.get("created_at"),
                "text": (t.get("text") or "").replace("\n", " ").strip(),
                "lang": t.get("lang"),
                "ingest_ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "source_username": u
            })

    df = spark.createDataFrame(rows, RAW_SCHEMA)

    # Agregar columna fecha local
    df = df.withColumn(
        "fecha",
        F.to_date(
            F.from_utc_timestamp(
                F.to_timestamp("created_at"),
                TZ
            )
        )
    )

    # Guardar Excel final
    out_excel = os.path.join(BASE_DIR, "ingesta", "ingesta.xlsx")
    save_excel(df, out_excel)

    print("\n[OK] Ingesta completada.\n")

    spark.stop()


if __name__ == "__main__":
    main()
