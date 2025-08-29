# -*- coding: utf-8 -*-
# Normalización v3.6 (corregida)
# Lee : {base_dir}/raw/x/run_ts=<RUN_TS>/
# Escribe: {base_dir}/refined/x/run_ts=<RUN_TS>/created_date=YYYY-MM-DD/
#
# Cambios clave:
# - Limpieza de 'ubicacion': quita ", ,", comas/espacios finales, normaliza '/', colapsa espacios, corrige O\' -> O'
# - Si una unidad (B-9, BX-3, Q-7, etc.) quedó pegada al final de 'ubicacion', la extrae y la agrega a 'unidades'
# - **FIX**: normalización de unidad usa reemplazo con $1-$2$3 (Spark/Java regex), evitando valores "\1-\2\3"

import os, sys, re
from pyspark.sql import SparkSession, functions as F, types as T

# -------- args & utils --------
def get_arg(name, default=None):
    for a in sys.argv[1:]:
        if a.startswith(name + "="):
            return a.split("=", 1)[1]
    return os.environ.get(name.upper(), default)

BASE_DIR    = get_arg("base_dir", os.path.abspath("./data"))
RUN_TS      = get_arg("run_ts", None)
LANG_FILTER = get_arg("lang", "")   # opcional: filtrar por idioma (ej. es)

def latest_run_ts(base_dir: str, layer="raw"):
    root = os.path.join(base_dir, layer, "x")
    if not os.path.isdir(root): return None
    runs = [d for d in os.listdir(root) if d.startswith("run_ts=")]
    if not runs: return None
    runs.sort(reverse=True)
    return runs[0].split("=", 1)[1]

# -------- reglas --------
# Código 10-x-y con guiones variables
RE_CODE_FLEX = r'(?i)\b10\s*[-–—−]\s*(\d)\s*[-–—−]\s*(\d{1,2})\b'

# Prefijos: 1 letra -> requiere número; 2+ letras -> número opcional
SINGLE_PREFIXES = ['B','Q','H','Z','M','A','U','R','K','X']
MULTI_PREFIXES  = ['AP','BR','RX','BX','UR','RB','BT','CJ','QR']

UNIT_RE = (
    r'(?i)(?<![A-Z0-9])(?:'
    r'(?:' + '|'.join(MULTI_PREFIXES) + r')(?:-?\d{1,3})?'   # AP, BR-8, RX-3, BX-2…
    r'|'
    r'(?:' + '|'.join(SINGLE_PREFIXES) + r')-?\d{1,3}'      # B-9, Q-7, A-2, K-4, X-1…
    r')\b'
)

UNIT_DICT = {
    "B":  "Carro Bomba (pumper)",
    "Q":  "Escalera Mecánica (aerial)",
    "BR": "Brigada de Rescate",
    "RX": "Unidad de Rescate (similar a BR)",
    "H":  "HazMat (Materiales Peligrosos)",
    "Z":  "Unidad de Mando / Comunicaciones",
    "M":  "Motobomba / Unidad Ligera",
    "A":  "Ambulancia (algunas instituciones)",
    "U":  "Unidad de Soporte/Logística",
    # variantes locales / por confirmar:
    "BX": "Carro Bomba (variante) — por confirmar",
    "AP": "Autopluma/Plataforma — por confirmar",
    "UR": "Unidad de Rescate (variante) — por confirmar",
    "RB": "Rescate/Bomba — por confirmar",
    "BT": "Bomba/Tanque — por confirmar",
    "CJ": "Comando/Jefatura — por confirmar",
    "QR": "Respuesta rápida — por confirmar",
    "R":  "Rescate (genérico) — por confirmar",
    "K":  "Código local — por confirmar",
    "X":  "Código local — por confirmar",
}

INCIDENT_MAP = {
  "0": "Incendio estructural",
  "1": "Incendio vehicular",
  "2": "Incendio forestal",
  "3": "Rescate / Atención médica",
  "4": "Materiales peligrosos (HazMat)",
  "5": "Derrumbe / Colapso",
  "6": "Emergencia técnica",
  "7": "Fuga de gas",
  "8": "Emergencia acuática",
  "9": "Alerta preventiva",
}

# -------- UDFs --------
@F.udf(T.ArrayType(T.StringType()))
def extract_units_any(s):
    """Extrae unidades en cualquier parte del texto y normaliza BX2->BX-2, Q7->Q-7."""
    if not s:
        return []
    t = re.sub(r'\s+', ' ', s).upper()
    raw = re.findall(UNIT_RE, t)
    out = []
    for u in raw:
        u = u.replace(' ', '')
        u = re.sub(r'^([A-Z]+)(\d+)$', r'\1-\2', u)  # sin guion -> con guion
        out.append(u)
    # De-dup preservando orden
    seen, norm = set(), []
    for u in out:
        if u not in seen:
            seen.add(u); norm.append(u)
    return norm

# -------- helpers limpieza ubicacion --------
def clean_ubicacion_col(col):
    c = F.regexp_replace(col, r"\\'", "'")             # O\'HIGGINS -> O'HIGGINS
    c = F.regexp_replace(c, r"\s*/\s*", " / ")         # normalizar '/'
    c = F.regexp_replace(c, r"\s*,\s*,\s*", ", ")      # ", ," -> ", "
    c = F.regexp_replace(c, r"[\s,;]+$", "")           # quitar coma/espacios finales
    c = F.regexp_replace(c, r"\s+", " ")               # colapsar espacios
    return F.trim(c)

# -------- main --------
def main():
    spark = (SparkSession.builder.appName("x-normalize-v3.6").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    run_ts = RUN_TS or latest_run_ts(BASE_DIR, "raw")
    if not run_ts:
        raise RuntimeError("No hay corridas RAW. Ejecuta primero la ingesta.")

    in_raw = os.path.join(BASE_DIR, "raw", "x", f"run_ts={run_ts}")
    df = spark.read.parquet(in_raw)

    # Campos de tiempo y filtro idioma
    df2 = (df
        .withColumn("created_at_ts", F.to_timestamp("created_at"))
        .withColumn("ingest_ts_ts",  F.to_timestamp("ingest_ts"))
    )
    if LANG_FILTER:
        df2 = df2.filter(F.col("lang") == LANG_FILTER)

    # Limpieza base de texto
    url_regex = r"(http|https)://\S+"
    df2 = (df2
        .withColumn("text_clean", F.regexp_replace(F.col("text"), url_regex, ""))
        .withColumn("text_clean", F.regexp_replace("text_clean", r"[–—−]", "-"))
        .withColumn("text_clean", F.trim(F.regexp_replace("text_clean", r"\s+", " ")))
    )

    # Detectar "SALE / SALEN ... A ..." y texto core (para ubicación)
    SALE_PREFIX = r'(?i)^\s*SALEN?\b.*?\bA\b\s+'
    df2 = df2.withColumn(
        "is_sale",
        F.expr("upper(text_clean) like 'SALE %' OR upper(text_clean) like 'SALEN %'")
    )
    df2 = df2.withColumn("text_core", F.regexp_replace(F.col("text_clean"), SALE_PREFIX, ""))

    # UNIDADES (del texto completo y del core)
    df2 = (df2
        .withColumn("unidades_full", extract_units_any(F.col("text_clean")))
        .withColumn("unidades_core", extract_units_any(F.col("text_core")))
        .withColumn("unidades", F.array_distinct(F.concat(F.col("unidades_full"), F.col("unidades_core"))))
    )

    # Código 10-x-y
    code_full = F.regexp_extract(F.col("text_core"), RE_CODE_FLEX, 0)
    code_x    = F.regexp_extract(F.col("text_core"), RE_CODE_FLEX, 1)
    code_y    = F.regexp_extract(F.col("text_core"), RE_CODE_FLEX, 2)

    # UBICACIÓN a partir de lo que sigue del código, sin tokens de unidades
    text_after_code = F.regexp_replace(
        F.col("text_core"),
        r'(?is)^.*?\b10\s*[-–—−]\s*\d\s*[-–—−]\s*\d{1,2}\b\s*',
        ''
    )
    text_no_units   = F.regexp_replace(text_after_code, UNIT_RE, '')
    ubicacion_from_code = F.trim(text_no_units)

    # Fallback a patrón "calle / calle"
    fallback_cross = F.trim(F.regexp_extract(
        F.col("text_core"),
        r'([A-ZÁÉÍÓÚÑ0-9 .\'°#-]+ / [A-ZÁÉÍÓÚÑ0-9 .\'°#-]+)',
        1
    ))

    ubicacion = F.when(F.length(code_full) > 0, ubicacion_from_code).otherwise(fallback_cross)

    # Tipo por código o keywords
    mp_inc = F.create_map(*[x for kv in INCIDENT_MAP.items() for x in (F.lit(kv[0]), F.lit(kv[1]))])
    tipo_por_codigo = mp_inc.getItem(code_x)
    utext = F.upper(F.col("text_core"))

    veh_regex = r'(INCENDIO\s*(DE\s*)?(VEHICULAR|VEHICULO|VEHÍCULO))|(\bAUTO\S*|\bCAMION(?:\b|ES)?|\bCAMIÓN(?:\b|ES)?|\bBUS(?:\b|ES)?|\bMOTO\S*|\bTAXI\b)'
    for_regex = r'(INCENDIO\s*(FORESTAL))|(\bPASTIZAL(?:ES)?\b|\bMATORRAL(?:ES)?\b|\bVEGETACI[ÓO]N\b|\bCERRO\b|\bQUEBRADA\b|\bPARQUE\b|\bPASTOS\b)'
    est_regex = r'(INCENDIO\s*(ESTRUCTURAL))|(\bCASA\b|\bVIVIENDA\b|\bEDIFICIO\b|\bDEPTO\.?\b|\bDEPARTAMENTO\b|\bLOCAL\b|\bBODEGA\b|\bGALP[ÓO]N\b|\bNAVE\b|\bF[ÁA]BRICA\b|\bCOLEGIO\b|\bHOSPITAL\b|\bUNIVERSIDAD\b|\bCOMERCIAL\b|\bINDUSTRIAL\b)'

    tipo_kw = (
        F.when(utext.like("%FUGA DE GAS%"),                     F.lit("Fuga de gas"))
         .when(utext.rlike(r'\bHAZMAT\b|MATERIAL\S*\s+PELIGR'), F.lit("Materiales peligrosos (HazMat)"))
         .when(utext.rlike(r'DERRUMBE|COLAPSO'),                F.lit("Derrumbe / Colapso"))
         .when(utext.like("%EMERGENCIA TECNICA%") | utext.like("%EMERGENCIA TÉCNICA%"),
                                                                F.lit("Emergencia técnica"))
         .when(utext.like("%RESCATE%") | utext.like("%ATENCION MEDICA%") | utext.like("%ATENCIÓN MÉDICA%"),
                                                                F.lit("Rescate / Atención médica"))
         .when(utext.rlike(veh_regex),                          F.lit("Incendio vehicular"))
         .when(utext.rlike(for_regex),                          F.lit("Incendio forestal"))
         .when(utext.rlike(est_regex),                          F.lit("Incendio estructural"))
         .when(utext.like("%INCENDIO%"),                        F.lit("Incendio"))
         .otherwise(F.lit(None))
    )

    unit_map = F.create_map(*[x for kv in UNIT_DICT.items() for x in (F.lit(kv[0]), F.lit(kv[1]))])

    df3 = (df2
        .withColumn("incident_code",  F.when(F.length(code_full) > 0, F.regexp_replace(code_full, r"\s+", "")))
        .withColumn("incident_class", F.when(F.length(code_x) > 0, code_x))
        .withColumn("incident_seq",   F.when(F.length(code_y) > 0, code_y.cast("int")))
        .withColumn("incident_type",  F.coalesce(tipo_por_codigo, tipo_kw))
        .withColumn("ubicacion",      F.when(F.length(ubicacion) > 0, ubicacion))
        .withColumn("unidades_prefix",
            F.transform(F.col("unidades"), lambda u: F.regexp_extract(u, r'^([A-Z]+)', 1))
        )
        .withColumn("unidades_desc",
            F.transform(F.col("unidades_prefix"), lambda p: unit_map.getItem(p))
        )
        .withColumn("unidades_desconocidas",
            F.array_distinct(F.filter(F.col("unidades_prefix"), lambda p: unit_map.getItem(p).isNull()))
        )
        .withColumn("created_date",   F.to_date("created_at_ts"))
        .drop("unidades_full","unidades_core")
        .dropDuplicates(["tweet_id"])
    )

    # ---------- LIMPIEZA 'ubicacion' + EXTRACCIÓN DE UNIDAD PEGADA AL FINAL ----------
    if "ubicacion" in df3.columns:
        # 1) Higiene base
        df3 = df3.withColumn("ubicacion", clean_ubicacion_col(F.col("ubicacion")))

        # 2) Detectar unidad al final (BX2, BX-2, Q7, B-9A, etc.)
        UNIT_TAIL_RE = r'(?i)([A-Z]{1,3}\s*-?\s*\d{1,3}[A-Z]?)\s*$'
        df3 = df3.withColumn("unit_tail", F.regexp_extract(F.upper(F.col("ubicacion")), UNIT_TAIL_RE, 1))

        # 3) Eliminar esa unidad del final de 'ubicacion'
        df3 = df3.withColumn(
            "ubicacion",
            F.when(
                F.col("unit_tail") != "",
                F.regexp_replace(F.col("ubicacion"),
                                 r"[\s,;/-]*[A-Za-z]{1,3}\s*-?\s*\d{1,3}[A-Za-z]?\s*$",
                                 "")
            ).otherwise(F.col("ubicacion"))
        )

        # 4) Normalizar unidad detectada (***FIX: usa $1-$2$3***) y agregar a 'unidades'
        unit_tail_norm = F.regexp_replace(
            F.regexp_replace(F.upper(F.col("unit_tail")), r"\s+", ""),
            r"^([A-Z]+)-?(\d+)([A-Z]?)$", "$1-$2$3"
        )
        df3 = (df3
            .withColumn("unit_tail_norm",
                        F.when(F.col("unit_tail") != "", unit_tail_norm).otherwise(F.lit(None)))
            .withColumn(
                "unidades",
                F.when(
                    F.col("unit_tail_norm").isNotNull(),
                    F.array_distinct(F.concat(F.col("unidades"), F.array(F.col("unit_tail_norm"))))
                ).otherwise(F.col("unidades"))
            )
            .drop("unit_tail","unit_tail_norm")
        )

        # 5) Re-limpieza final por si quedaron signos tras el recorte
        df3 = df3.withColumn("ubicacion", clean_ubicacion_col(F.col("ubicacion")))
    # ---------- FIN LIMPIEZA / EXTRACCIÓN ----------

    out_ref = os.path.join(BASE_DIR, "refined", "x", f"run_ts={run_ts}")
    (df3.repartition("created_date")
        .write.mode("overwrite")
        .partitionBy("created_date")
        .parquet(out_ref))

    print(f"[OK] REFINED -> {out_ref}")
    spark.stop()

if __name__ == "__main__":
    main()
