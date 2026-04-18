import os
import msal
import requests
import pandas as pd
import io

CLIENT_ID = os.environ["MS_CLIENT_ID"]
TENANT_ID = os.environ["MS_TENANT_ID"]
REFRESH_TOKEN = os.environ["MS_REFRESH_TOKEN"]
SHAREPOINT_SITE = "escuelarefrigeracion.sharepoint.com"
SITE_PATH = "/sites/ASESORASCOMERCIALES"
SCOPES = ["Sites.Read.All", "Files.Read.All"]

def get_access_token():
    app = msal.PublicClientApplication(
        CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}"
    )
    result = app.acquire_token_by_refresh_token(REFRESH_TOKEN, scopes=SCOPES)
    if "access_token" not in result:
        raise Exception(f"Error renovando token: {result.get('error_description')}")
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
    excels = [f for f in items if f["name"].endswith((".xlsx", ".xls"))]
    return excels

def download_excel(token, site_id, file_id, file_name):
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{file_id}/content"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    df = pd.read_excel(io.BytesIO(r.content))
    print(f"  ✅ {file_name}: {len(df)} filas, {len(df.columns)} columnas")
    return df

print("🔑 Obteniendo token de acceso...")
token = get_access_token()
print("✅ Token obtenido\n")

print("🔍 Buscando sitio de SharePoint...")
site_id = get_site_id(token)
print(f"✅ Sitio encontrado: {site_id}\n")

print("📁 Listando archivos Excel...")
excels = list_excel_files(token, site_id)
print(f"✅ Encontrados {len(excels)} archivos Excel:\n")

all_dfs = []
for file in excels:
    print(f"  📥 Descargando: {file['name']}")
    df = download_excel(token, site_id, file["id"], file["name"])
    all_dfs.append(df)

if all_dfs:
    df_combined = pd.concat(all_dfs, ignore_index=True)
    print(f"\n✅ TOTAL combinado: {len(df_combined)} filas")
    print(f"   Columnas: {list(df_combined.columns)}")
else:
    print("⚠️ No se encontraron archivos Excel")
