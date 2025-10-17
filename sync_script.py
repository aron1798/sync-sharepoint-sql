import pandas as pd
import pyodbc
import requests
from io import BytesIO
import os
import logging
import time
import urllib3

# Deshabilitar warnings de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def sync_sharepoint_to_sql():
    logging.info("🚀 Iniciando SINCRONIZACIÓN SharePoint -> Azure SQL")
    
    # Configuración
    SHAREPOINT_USERNAME = os.environ['SHAREPOINT_USERNAME']
    SHAREPOINT_PASSWORD = os.environ['SHAREPOINT_PASSWORD']
    SQL_SERVER = os.environ['SQL_SERVER']
    SQL_DATABASE = os.environ['SQL_DATABASE']
    SQL_USERNAME = os.environ['SQL_USERNAME']
    SQL_PASSWORD = os.environ['SQL_PASSWORD']
    
    # Configuración vendedoras
    VENDEDORAS_CONFIG = [
        {
            "public_link": "https://escuelarefrigeracion.sharepoint.com/:x:/s/ASESORASCOMERCIALES/EaIlXhIcpYBFkaxzXu7aQIQBAu_zaldlNLgtz7y6bOMyCA?e=yVU2iw",
            "table_name": "Base_Alonso",
            "rango_filas": "1:10000"
        },
        {
            "public_link": "https://escuelarefrigeracion.sharepoint.com/:x:/s/ASESORASCOMERCIALES/EeRBRnXXABpPhWkYk87UcjoB-VltTBFz6MRSQ-VEbucP8Q?e=bvUv7V",
            "table_name": "Base_Diana",
            "rango_filas": "10001:20000"
        },
        {
            "public_link": "https://escuelarefrigeracion.sharepoint.com/:x:/s/ASESORASCOMERCIALES/EQGtk5_fCslJowZlY8g7kTEBLfD29swdE4DK_0nDfBZ7qw?e=36cm8P",
            "table_name": "Base_Gerson", 
            "rango_filas": "20001:30000"
        },
    ]
    
    # Conexión Azure SQL
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
        
        # Procesar cada archivo
        total_registros = 0
        for config in VENDEDORAS_CONFIG:
            try:
                logging.info(f"🔄 Procesando: {config['table_name']}")
                
                # Descargar archivo
                file_content = download_sharepoint_file(config['public_link'], SHAREPOINT_USERNAME, SHAREPOINT_PASSWORD)
                
                if file_content:
                    # Procesar Excel
                    df = process_excel_file(file_content, config['table_name'])
                    
                    if df is not None and not df.empty:
                        df = df.head(10000)
                        
                        # ACTUALIZAR SQL
                        registros_procesados = update_database(cursor, df, config['rango_filas'])
                        total_registros += registros_procesados
                        logging.info(f"✅ {config['table_name']}: {registros_procesados} registros")
                    else:
                        logging.error(f"❌ No se encontraron datos en: {config['table_name']}")
                else:
                    logging.error(f"❌ No se pudo descargar: {config['table_name']}")
                    
            except Exception as e:
                logging.error(f"❌ Error procesando {config['table_name']}: {str(e)}")
                continue
        
        # Confirmar cambios en SQL
        conn.commit()
        conn.close()
        logging.info(f"🎉 SINCRONIZACIÓN COMPLETADA - {total_registros} filas actualizadas")
        
    except Exception as e:
        logging.error(f"💥 Error general: {str(e)}")
        raise

def download_sharepoint_file(public_link, username, password):
    """Descargar archivo de SharePoint"""
    try:
        session = requests.Session()
        session.auth = (username, password)
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': '*/*'
        }
        
        response = session.get(public_link, headers=headers, timeout=30, verify=False, allow_redirects=True)
        
        if response.status_code == 200:
            content = response.content
            if len(content) > 1000 and content[:4] == b'PK\x03\x04':
                logging.info(f"✅ Archivo descargado: {len(content)} bytes")
                return BytesIO(content)
            else:
                logging.error("❌ No es archivo Excel válido")
        else:
            logging.error(f"❌ Error HTTP: {response.status_code}")
            
        return None
        
    except Exception as e:
        logging.error(f"❌ Error descargando: {str(e)}")
        return None

def process_excel_file(file_content, table_name):
    """Procesar archivo Excel"""
    try:
        file_content.seek(0)
        
        for engine in ['openpyxl', 'xlrd']:
            try:
                df = pd.read_excel(file_content, engine=engine, sheet_name=0)
                if not df.empty and len(df.columns) > 1:
                    logging.info(f"✅ Excel procesado: {len(df)} filas")
                    return clean_dataframe(df)
            except:
                file_content.seek(0)
                continue
        
        return None
        
    except Exception as e:
        logging.error(f"❌ Error procesando Excel: {str(e)}")
        return None

def clean_dataframe(df):
    """Limpiar DataFrame"""
    df_clean = df.dropna(how='all').dropna(axis=1, how='all')
    df_clean.columns = [str(col).strip() for col in df_clean.columns]
    return df_clean

def update_database(cursor, df, rango_filas):
    """ACTUALIZAR BASE DE DATOS SQL"""
    start_id, end_id = map(int, rango_filas.split(':'))
    
    df.columns = [str(col).strip() for col in df.columns]
    
    # Mapeo de columnas
    mapeo_columnas = {}
    columnas_requeridas = ['Ejecutivo', 'Telefono', 'FechaCreada', 'Sede', 'Programa', 'Turno', 
                          'Codigo', 'Canal', 'Intervalo', 'Medio', 'Contacto', 'Interesado', 
                          'Estado', 'Objecion', 'Observacion']
    
    for col_requerida in columnas_requeridas:
        for col_real in df.columns:
            if col_requerida.lower() in col_real.lower():
                mapeo_columnas[col_requerida] = col_real
                break
    
    registros_actualizados = 0
    
    for index, row in df.iterrows():
        current_id = start_id + index
        if current_id > end_id:
            break
        
        try:
            valores = []
            for col_requerida in columnas_requeridas:
                col_real = mapeo_columnas.get(col_requerida, col_requerida)
                valor = row.get(col_real, '')
                
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
                elif pd.isna(valor):
                    valor = None
                
                valores.append(valor)
            
            valores.append(current_id)
            
            # EJECUTAR UPDATE EN SQL
            cursor.execute("""
                UPDATE vendedoras_data SET
                    Ejecutivo = ?, Telefono = ?, FechaCreada = ?, Sede = ?,
                    Programa = ?, Turno = ?, Codigo = ?, Canal = ?, Intervalo = ?,
                    Medio = ?, Contacto = ?, Interesado = ?, Estado = ?,
                    Objecion = ?, Observacion = ?
                WHERE ID = ?
            """, *valores)
            
            registros_actualizados += 1
            
        except Exception as e:
            logging.warning(f"⚠️ Error actualizando ID {current_id}: {str(e)}")
            continue
    
    return registros_actualizados

def connect_sql_with_retry(connection_string, max_retries=3):
    """Conectar a SQL con reintentos"""
    for attempt in range(max_retries):
        try:
            conn = pyodbc.connect(connection_string)
            logging.info(f"✅ Conexión SQL exitosa (intento {attempt + 1})")
            return conn
        except pyodbc.OperationalError as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5
                logging.warning(f"⚠️ Intento {attempt + 1} fallado, reintentando en {wait_time}s: {str(e)}")
                time.sleep(wait_time)
            else:
                logging.error(f"💥 Todos los intentos fallaron: {str(e)}")
                raise e

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    sync_sharepoint_to_sql()
