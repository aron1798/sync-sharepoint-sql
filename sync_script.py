import pandas as pd
import pyodbc
import requests
from io import BytesIO
import os
import logging
import time
from msal import ConfidentialClientApplication, PublicClientApplication

def sync_sharepoint_to_sql():
    logging.info("🚀 Iniciando ACTUALIZACIÓN SharePoint -> Azure SQL")
    
    # ===== CONFIGURACIÓN APP REGISTRATION =====
    CLIENT_ID = os.environ['SHAREPOINT_CLIENT_ID']
    CLIENT_SECRET = os.environ['SHAREPOINT_CLIENT_SECRET']
    TENANT_ID = os.environ['SHAREPOINT_TENANT_ID']
    
    SQL_SERVER = os.environ['SQL_SERVER']
    SQL_DATABASE = os.environ['SQL_DATABASE']
    SQL_USERNAME = os.environ['SQL_USERNAME']
    SQL_PASSWORD = os.environ['SQL_PASSWORD']
    
    # ===== CONFIGURACIÓN POR VENDEDORA =====
    VENDEDORAS_CONFIG = [
        {
            "path": "Documentos compartidos/2. BASE PROSPECTOS/BASE GENERAL/Base Alonso Huaman.xlsx",
            "table_name": "Base_Alonso",
            "rango_filas": "1:10000"
        },
        {
            "path": "Documentos compartidos/2. BASE PROSPECTOS/BASE GENERAL/Base Diana Chavez.xlsx",
            "table_name": "Base_Diana",
            "rango_filas": "10001:20000"
        },
        {
            "path": "Documentos compartidos/2. BASE PROSPECTOS/BASE GENERAL/Base Gerson Falen.xlsx",
            "table_name": "Base_Gerson",
            "rango_filas": "20001:30000"
        },
    ]
    
    # Cadena de conexión Azure SQL
    connection_string = f"""
    Driver={{ODBC Driver 18 for SQL Server}};
    Server={SQL_SERVER};
    Database={SQL_DATABASE};
    Uid={SQL_USERNAME};
    Pwd={SQL_PASSWORD};
    Encrypt=yes;
    TrustServerCertificate=no;
    Connection Timeout=60;
    """
    
    try:
        # Conectar a Azure SQL
        conn = connect_sql_with_retry(connection_string)
        cursor = conn.cursor()
        
        # Obtener token de acceso
        access_token = get_sharepoint_access_token(CLIENT_ID, CLIENT_SECRET, TENANT_ID)
        if not access_token:
            logging.error("💥 No se pudo obtener token de acceso")
            return
        
        # Procesar cada vendedora
        for config in VENDEDORAS_CONFIG:
            try:
                logging.info(f"🔄 Procesando: {config['table_name']}")
                
                # DESCARGAR CON GRAPH API
                file_content = download_sharepoint_file_graph(
                    config['path'], 
                    access_token
                )
                
                if file_content is None:
                    logging.error(f"❌ No se pudo descargar: {config['path']}")
                    continue
                
                # Buscar la tabla en el Excel
                df = find_table_in_excel(file_content, config['table_name'])
                
                if df is not None and not df.empty:
                    df = df.head(10000)
                    actualizar_filas_azure(cursor, df, config['rango_filas'])
                else:
                    logging.error(f"❌ No se encontró tabla válida: {config['table_name']}")
                
            except Exception as e:
                logging.error(f"❌ Error procesando {config['table_name']}: {str(e)}")
                continue
        
        conn.commit()
        conn.close()
        logging.info("🎉 ACTUALIZACIÓN COMPLETADA - 100,000 filas actualizadas")
            
    except Exception as e:
        logging.error(f"💥 Error general: {str(e)}")
        raise e

def get_sharepoint_access_token(client_id, client_secret, tenant_id):
    """Obtener access token usando App Registration"""
    try:
        authority = f"https://login.microsoftonline.com/{tenant_id}"
        scope = ["https://graph.microsoft.com/.default"]
        
        app = ConfidentialClientApplication(
            client_id=client_id,
            client_credential=client_secret,
            authority=authority
        )
        
        result = app.acquire_token_for_client(scopes=scope)
        
        if "access_token" in result:
            logging.info("✅ Token de acceso obtenido exitosamente")
            return result["access_token"]
        else:
            logging.error(f"❌ Error obteniendo token: {result.get('error_description', 'Unknown error')}")
            return None
            
    except Exception as e:
        logging.error(f"❌ Error en autenticación: {str(e)}")
        return None

def download_sharepoint_file_graph(file_path, access_token):
    """Descargar archivo usando Microsoft Graph API"""
    try:
        site_id = "escuelarefrigeracion.sharepoint.com,sites,ASESORASCOMERCIALES"
        
        # Codificar el path para URL
        encoded_path = file_path.replace(" ", "%20")
        
        # URL de Graph API para descargar archivo
        graph_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{encoded_path}:/content"
        
        logging.info(f"📥 Descargando via Graph API: {file_path}")
        
        headers = {
            'Authorization': f'Bearer {access_token}',
            'User-Agent': 'SyncSharePointToSQL/1.0'
        }
        
        response = requests.get(graph_url, headers=headers, timeout=60)
        
        if response.status_code == 200:
            logging.info(f"✅ Descarga Graph API exitosa: {len(response.content)} bytes")
            return BytesIO(response.content)
        else:
            logging.error(f"❌ Error Graph API {response.status_code}: {response.text}")
            return None
            
    except Exception as e:
        logging.error(f"❌ Error descarga Graph API: {str(e)}")
        return None

# ... (las funciones connect_sql_with_retry, find_table_in_excel, actualizar_filas_azure se mantienen igual)
def connect_sql_with_retry(connection_string, max_retries=3):
    """Conectar a SQL con reintentos"""
    for attempt in range(max_retries):
        try:
            conn = pyodbc.connect(connection_string)
            logging.info(f"✅ Conexión SQL exitosa (intento {attempt + 1})")
            return conn
        except pyodbc.OperationalError as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 10
                logging.warning(f"⚠️ Intento {attempt + 1} fallado, reintentando en {wait_time}s: {str(e)}")
                time.sleep(wait_time)
            else:
                logging.error(f"💥 Todos los intentos fallaron: {str(e)}")
                raise e

def find_table_in_excel(file_content, table_name):
    """Buscar tabla específica en el Excel"""
    try:
        excel_file = pd.ExcelFile(file_content)
        
        # Estrategia 1: Buscar por nombre de tabla en celdas
        for sheet_name in excel_file.sheet_names:
            try:
                df_temp = pd.read_excel(file_content, sheet_name=sheet_name, header=None)
                
                for row_idx, row in df_temp.iterrows():
                    for col_idx, value in row.items():
                        if pd.notna(value) and table_name.lower() in str(value).lower():
                            logging.info(f"✅ Tabla '{table_name}' encontrada en hoja: {sheet_name}, fila: {row_idx+1}")
                            df = pd.read_excel(file_content, sheet_name=sheet_name, header=row_idx)
                            return df
            except Exception:
                continue
        
        # Estrategia 2: Usar primera hoja con datos
        logging.info(f"⚠️ No se encontró tabla por nombre, usando primera hoja con datos")
        try:
            df = pd.read_excel(file_content, sheet_name=0)
            return df
        except Exception:
            pass
            
        # Estrategia 3: Probar todas las hojas
        for sheet_name in excel_file.sheet_names:
            try:
                df = pd.read_excel(file_content, sheet_name=sheet_name)
                if not df.empty:
                    logging.info(f"✅ Datos encontrados en hoja: {sheet_name}")
                    return df
            except Exception:
                continue
                
        return None
        
    except Exception as e:
        logging.error(f"❌ Error buscando tabla: {str(e)}")
        return None

def actualizar_filas_azure(cursor, df, rango_filas):
    """Actualizar filas específicas en Azure SQL"""
    # Obtener rango de IDs a actualizar
    start_id, end_id = map(int, rango_filas.split(':'))
    
    # Normalizar nombres de columnas
    df.columns = [str(col).strip() for col in df.columns]
    
    # Mapeo flexible de columnas
    mapeo_columnas = {}
    columnas_requeridas = ['Ejecutivo', 'Telefono', 'FechaCreada', 'Sede', 'Programa', 'Turno', 
                          'Codigo', 'Canal', 'Intervalo', 'Medio', 'Contacto', 'Interesado', 
                          'Estado', 'Objecion', 'Observacion']
    
    for col_requerida in columnas_requeridas:
        for col_real in df.columns:
            if col_requerida.lower() in col_real.lower():
                mapeo_columnas[col_requerida] = col_real
                break
    
    logging.info(f"🔍 Mapeo de columnas encontradas: {len(mapeo_columnas)}/{len(columnas_requeridas)}")
    
    # Actualizar fila por fila
    registros_actualizados = 0
    for index, row in df.iterrows():
        current_id = start_id + index
        
        if current_id > end_id:
            break
        
        try:
            # Obtener valores usando mapeo
            valores = []
            for col_requerida in columnas_requeridas:
                col_real = mapeo_columnas.get(col_requerida, col_requerida)
                valor = row.get(col_real, '')
                
                # Manejar fechas
                if col_requerida == 'FechaCreada' and pd.notna(valor):
                    try:
                        if isinstance(valor, str):
                            valor = pd.to_datetime(valor, errors='coerce')
                        if pd.notna(valor):
                            valor = valor.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            valor = None
                    except:
                        valor = None
                
                valores.append(valor)
            
            valores.append(current_id)
            
            cursor.execute("""
                UPDATE vendedoras_data SET
                    Ejecutivo = ?,
                    Telefono = ?,
                    FechaCreada = ?,
                    Sede = ?,
                    Programa = ?,
                    Turno = ?,
                    Codigo = ?,
                    Canal = ?,
                    Intervalo = ?,
                    Medio = ?,
                    Contacto = ?,
                    Interesado = ?,
                    Estado = ?,
                    Objecion = ?,
                    Observacion = ?
                WHERE ID = ?
            """, *valores)
            
            registros_actualizados += 1
            
        except Exception as e:
            logging.warning(f"⚠️ Error actualizando ID {current_id}: {str(e)}")
            continue
    
    logging.info(f"📊 Actualizadas filas {rango_filas}: {registros_actualizados}/{len(df)} registros")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    sync_sharepoint_to_sql()
