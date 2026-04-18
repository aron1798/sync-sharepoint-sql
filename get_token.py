# Este script lo corres solo una vez en tu PC para obtener el refresh token
# Luego ese token lo guardas en GitHub Secrets

import msal
import json

CLIENT_ID = "bdb5663a-a80c-4810-924b-f9592e23f690"
TENANT_ID = "139871b4-a9ba-4747-9384-22ff9b390611"
SCOPES = ["Sites.Read.All", "Files.Read.All", "offline_access"]

app = msal.PublicClientApplication(
    CLIENT_ID,
    authority=f"https://login.microsoftonline.com/{TENANT_ID}"
)

print("Iniciando autenticación con tu cuenta Microsoft...")
flow = app.initiate_device_flow(scopes=SCOPES)

if "user_code" not in flow:
    raise Exception(f"Error al iniciar: {flow}")

# Te mostrará algo como:
# "Ve a https://microsoft.com/devicelogin y escribe el código: ABC123456"
print("\n" + flow["message"])
print("\nEsperando que completes el login (tienes 15 minutos)...\n")

result = app.acquire_token_by_device_flow(flow)

if "refresh_token" in result:
    print("✅ Token obtenido exitosamente!")
    print("\n" + "="*60)
    print("COPIA ESTE VALOR y guárdalo como GitHub Secret 'MS_REFRESH_TOKEN':")
    print("="*60)
    print(result["refresh_token"])
    print("="*60)
    
    # También lo guarda en archivo local por si acaso
    with open("token_local.json", "w") as f:
        json.dump(result, f, indent=2)
    print("\nTambién guardado en token_local.json (NO subas este archivo a GitHub)")
else:
    print(f"❌ Error: {result.get('error_description', result)}")
