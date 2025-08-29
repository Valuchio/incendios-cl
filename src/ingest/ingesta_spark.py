# -*- coding: utf-8 -*-
# Ingesta mínima desde X (solo tweets; sin métricas)
# Guarda RAW en Parquet particionado por fecha y run_ts.

import os, sys, time, requests
from datetime import datetime, timezone
from typing import Dict, Any, List
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from pyspark.sql import functions as F

# ============================
# CONFIGURACIÓN MANUAL (EDITA AQUÍ)
# ============================
BEARER = "AAAAAAAAAAAAAAAAAAAAAHG13gEAAAAAmIu%2BZWCVeINn0r3BtJXsT3cgXVU%3DMkDIet2TOVKzt8Dk6RI04jUIVD7ppvjJbqZr70XY0pMy4PLO6y"  # <-- Token
USERS  = ["CentralCBS"]              # <-- Uno o varios usuarios (lista)
MAX_RESULTS = 17                          # <-- Cuántos tweets traer por usuario
# ============================

def get_arg(name, default=None):
    for a in sys.argv[1:]:
        if a.startswith(name + "="):
            return a.split("=", 1)[1]
    return os.environ.get(name.upper(), default)

BASE_DIR = get_arg("base_dir", os.path.abspath("./data"))
# ✅ FIX: run_ts correcto en UTC con formato YYYYMMDDTHHMMSSZ
RUN_TS = get_arg("run_ts", datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))

if not BEARER or BEARER == "PON_AQUI_TU_TOKEN":
    raise RuntimeError("Configura tu BEARER en el archivo (variable BEARER).")

HEADERS = {"Authorization": f"Bearer {BEARER}"}
X_API_BASE = "https://api.x.com/2"
TWEET_FIELDS = "created_at,lang"
EXCLUDE = "retweets,replies"
EXPANSIONS = "author_id"
USER_FIELDS = "username,name,verified"

def http_get(url: str, params: Dict[str, Any], retries=3, backoff=10) -> Dict[str, Any]:
    for attempt in range(1, retries + 1):
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 500, 502, 503, 504) and attempt < retries:
            time.sleep(backoff * attempt); continue
        r.raise_for_status()
    raise RuntimeError("HTTP error inesperado")

def get_user(username: str) -> Dict[str, Any]:
    return http_get(f"{X_API_BASE}/users/by/username/{username}",
                    {"user.fields": USER_FIELDS})["data"]

def get_tweets(user_id: str) -> Dict[str, Any]:
    return http_get(f"{X_API_BASE}/users/{user_id}/tweets", {
        "max_results": MAX_RESULTS,
        "exclude": EXCLUDE,
        "tweet.fields": TWEET_FIELDS,
        "expansions": EXPANSIONS,
        "user.fields": USER_FIELDS
    })

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

def main():
    spark = SparkSession.builder.appName("x-ingest-minimal").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    all_rows: List[Dict[str, Any]] = []
    for u in USERS:
        try:
            user = get_user(u)
            uid = user["id"]
            payload = get_tweets(uid)
            users_map = {x["id"]: x for x in payload.get("includes", {}).get("users", [])}
            for t in payload.get("data", []):
                au = users_map.get(t.get("author_id"), user)
                all_rows.append({
                    "tweet_id": t.get("id"),
                    "author_id": t.get("author_id"),
                    "username": au.get("username"),
                    "name": au.get("name"),
                    "verified": au.get("verified"),
                    "created_at": t.get("created_at"),
                    "text": (t.get("text") or "").replace("\n", " ").strip(),
                    "lang": t.get("lang"),
                    # ✅ FIX: timestamp de ingesta en UTC y bien formateado
                    "ingest_ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "source_username": u
                })
        except Exception as e:
            print(f"[WARN] Fallo @{u}: {e}")

    if not all_rows:
        print("[INFO] No se obtuvieron tweets."); spark.stop(); return

    df = (spark.createDataFrame(all_rows, RAW_SCHEMA)
              .withColumn("date_str", F.substring("created_at", 1, 10)))  # YYYY-MM-DD

    out_raw = os.path.join(BASE_DIR, "raw", "x", f"run_ts={RUN_TS}")
    (df.repartition("date_str")
       .write.mode("overwrite")
       .partitionBy("date_str")
       .parquet(out_raw))

    print(f"[OK] RAW -> {out_raw}")
    spark.stop()

if __name__ == "__main__":
    main()
