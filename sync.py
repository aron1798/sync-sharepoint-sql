import os
import msal
import requests
import pandas as pd
import psycopg2
import io
from supabase import create_client

# ── Credenciales ──────────────────────────────────────────
CLIENT_ID      = os.environ["MS_CLIENT_ID"]
TENANT_ID      = os.environ["MS_TENANT_ID"]
REFRESH_TOKEN  = os.environ["MS_REFRESH_TOKEN"]
PG_HOST        = os.environ["PG_HOST"]
PG_USER        = os.environ["PG_USER"]
PG_PASS        = os.environ["PG_PASS"]
PG_DB          = os.environ["PG_DB"]
SUPABASE_URL   = os.environ["SUPABASE_URL2"]
SUPABASE_KEY   = os.environ["SUPABASE_KEY2"]

SHAREPOINT_SITE = "escuelarefrigeracion.sharepoint.com"
SITE_PATH       = "/sites/ASESORASCOMERCIALES"
SCOPES          = ["Sites.Read.All", "Files.Read.All"]

COLUMNAS = [
    "Ejecutivo", "Telefono", "Fechacreada", "Sede", "Programa",
    "Turno", "Codigo", "Canal", "Intervalo", "Medio",
    "Contacto", "Interesado", "Estado", "Objecion", "Observacion"
]

# ── SharePoint ────────────────────────────────────────────
def get_access_token():
    app = msal.PublicClientApplication(
        CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}"
    )
    result = app.acquire_token_by_refresh_token(REFRESH_TOKEN, scopes=SCOPES)
    if "access_token" not in result:
        raise Exception(f"Error token: {result.get('error_description')}")
    return result["access_token"]

def get_site_id(token):
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/sites/{SHAREPOINT_SITE}:{SITE_PATH}"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()["id"]

def list_excel_files(token, site_id):
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root/children"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    items = r.json().get("value", [])
    return [f for f in items if f["name"].endswith((".xlsx", ".xls"))]

def download_excel(token, site_id, file_id, file_name):
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{file_id}/content"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    df = pd.read_excel(io.BytesIO(r.content))
    print(f"  ✅ {file_name}: {len(df)} filas")
    return df

# ── PostgreSQL ────────────────────────────────────────────
def get_postgres_data():
    conn = psycopg2.connect(
        host=PG_HOST, user=PG_USER,
        password=PG_PASS, dbname=PG_DB, port=5432
    )
    query = """
        SELECT c.phone_number, c.created_at, u.name
        FROM contacts AS c
        LEFT JOIN conversations AS con ON c.id = con.contact_id
        LEFT JOIN users AS u ON con.assignee_id = u.id
        WHERE u.name IS NOT NULL
        ORDER BY c.created_at DESC
    """
    df = pd.read_sql(query, conn)
    conn.close()

    # Limpiar teléfono
    df["phone_number"] = df["phone_number"].astype(str)
    df["phone_number"] = df["phone_number"].str.replace(r"^\+51", "", regex=True)
    df["phone_number"] = df["phone_number"].str.replace("+", "", regex=False)

    # Formatear fecha solo como DD/MM/YYYY sin hora
    df["created_at"] = pd.to_datetime(df["created_at"]).dt.strftime("%d/%m/%Y")

    # Mapear a columnas estándar
    df_mapped = pd.DataFrame("-", index=df.index, columns=COLUMNAS)
    df_mapped["Ejecutivo"]   = df["name"]
    df_mapped["Telefono"]    = df["phone_number"]
    df_mapped["Fechacreada"] = df["created_at"]
    df_mapped["Canal"]       = "COPITO"

    print(f"  ✅ PostgreSQL: {len(df_mapped)} filas")
    return df_mapped

# ── MAIN ──────────────────────────────────────────────────
print("="*50)
print("🚀 Iniciando sincronización...")
print("="*50)

# 1. Leer Excel de SharePoint
print("\n📁 Leyendo Excel de SharePoint...")
token   = get_access_token()
site_id = get_site_id(token)
excels  = list_excel_files(token, site_id)

all_dfs = []
for file in excels:
    df = download_excel(token, site_id, file["id"], file["name"])
    for col in COLUMNAS:
        if col not in df.columns:
            df[col] = "-"
    all_dfs.append(df[COLUMNAS])

# 2. Leer PostgreSQL
print("\n🗄️  Leyendo PostgreSQL...")
df_pg = get_postgres_data()
all_dfs.append(df_pg)

# 3. Unir todo
df_final = pd.concat(all_dfs, ignore_index=True)
df_final = df_final.fillna("-")
df_final = df_final.astype(str)
df_final = df_final.replace("nan", "-")
print(f"\n✅ Total unificado: {len(df_final)} filas")

# 4. Borrar datos anteriores en Supabase
print("\n🗑️  Limpiando tabla anterior...")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
supabase.table("datos_unificados").delete().neq("id", 0).execute()

# 5. Subir en lotes de 500
print("\n⬆️  Subiendo a Supabase...")
records = df_final.to_dict(orient="records")
batch_size = 500
for i in range(0, len(records), batch_size):
    batch = records[i:i+batch_size]
    supabase.table("datos_unificados").insert(batch).execute()
    print(f"  📤 Subidos {min(i+batch_size, len(records))}/{len(records)} registros")

print("\n🎉 ¡Sincronización completada exitosamente!")
