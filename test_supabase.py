import os
from supabase import create_client

SUPABASE_URL = "https://uztqscimtsihrzgybsyb.supabase.co"
SUPABASE_KEY = "tu_secret_key_aqui"  # Pega tu secret key directamente

try:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("✅ Conexión exitosa!")
    
    # Probar listar tablas
    response = supabase.table('vendedoras_data').select('*').limit(1).execute()
    print("✅ Tabla accesible!")
    
except Exception as e:
    print(f"❌ Error: {e}")
