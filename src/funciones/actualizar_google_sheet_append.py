# -*- coding: utf-8 -*-
"""
APPEND a Google Sheet sin borrar datos anteriores,
manteniendo TODAS las filas reales del Excel (excepto encabezados)
y formateando fechas como YYYY-MM-DD.
"""

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

CREDENTIALS_PATH = r"C:\proyectos\incendios-cl\secrets\credentials.json"

def append_to_google_sheet(local_excel_path, sheet_id, credentials_path=CREDENTIALS_PATH):

    # 1. Autenticación Google
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(credentials_path, scopes=scopes)
    client = gspread.authorize(creds)

    # 2. Leer Excel (pandas ya omite la fila 1 como encabezados)
    df = pd.read_excel(local_excel_path)

    # 2.1 Convertir fechas a YYYY-MM-DD
    df = df.applymap(lambda x: x.strftime("%Y-%m-%d") if hasattr(x, "strftime") else x)

    # 2.2 Convertir todo a string
    df = df.astype(str)

    # -> NO eliminar filas
    # -> NO usar iloc[1:], porque pandas ya usa la primera fila como header

    # 3. Abrir Google Sheet
    sh = client.open_by_key(sheet_id)
    ws = sh.sheet1

    # 4. Obtener fila libre
    existing = ws.get_all_values()
    next_row = len(existing) + 1 if existing else 1

    # 5. Exportar los datos
    data_to_append = df.values.tolist()

    ws.update(
        range_name=f"A{next_row}",
        values=data_to_append
    )

    print(f"✔ {len(data_to_append)} filas agregadas.")


if __name__ == "__main__":

    EXCEL_LOCAL = r"C:\proyectos\incendios-cl\data\curado\curado.xlsx"
    GOOGLE_SHEET_ID = "1iQu1cbxEBzbYmHPUCPsquTLHcHy0BuiynkGQW3dbRAU"

    append_to_google_sheet(EXCEL_LOCAL, GOOGLE_SHEET_ID)
