import pandas as pd
import pyodbc
import requests
from io import BytesIO
import os
import logging
import time
import urllib3

# Deshabilitar warnings de SSL (temporal para pruebas)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def sync_sharepoint_to_sql():
    logging.info("🚀 Iniciando ACTUALIZACIÓN SharePoint -> Azure SQL")
    
    # ===== CONFIGURACIÓN =====
    SHAREPOINT_USERNAME = os.environ['SHAREPOINT_USERNAME']
    SHAREPOINT_PASSWORD = os.environ['SHAREPOINT_PASSWORD']
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
            "rango_filas": "1:10000"
        },
        {
            "path": "Documentos compartidos/2. BASE PROSPECTOS/BASE GENERAL/Base Gerson Falen.xlsx",
            "table_name": "Base_Gerson",
            "rango_filas": "1:10000"
        },
        # ... AGREGA LAS 10 VENDEDORAS
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
        # Conectar a Azure SQL con reintentos
        conn = connect_sql_with_retry(connection_string)
        cursor = conn.cursor()
        
        # Procesar cada vendedora
        for config in VENDEDORAS_CONFIG:
            try:
                logging.info(f"🔄 Procesando: {config['table_name']}")
                
                # PRIMERO: Intentar con URL directa de descarga
                file_content = download_sharepoint_file_direct(
                    config['path'], 
                    SHAREPOINT_USERNAME, 
                    SHAREPOINT_PASSWORD
                )
                
                # SI FALLA: Intentar con método simple
                if file_content is None:
                    logging.warning("🔄 Intentando con método simple...")
                    file_content = download_sharepoint_file_simple(
                        config['path'], 
                        SHAREPOINT_USERNAME, 
                        SHAREPOINT_PASSWORD
                    )
                
                if file_content is None:
                    logging.error(f"❌ No se pudo descargar: {config['path']}")
                    continue
                
                # Buscar la tabla en el Excel
                df = find_table_in_excel(file_content, config['table_name'])
                
                if df is not None and not df.empty:
                    # Limitar a 10,000 filas máximo
                    df = df.head(10000)
                    
                    # ACTUALIZAR Azure SQL
                    actualizar_filas_azure(cursor, df, config['rango_filas'])
                else:
                    logging.error(f"❌ No se encontró tabla válida: {config['table_name']}")
                
            except Exception as e:
                logging.error(f"❌ Error procesando {config['table_name']}: {str(e)}")
                continue
        
        # Confirmar todos los cambios
        conn.commit()
        conn.close()
        logging.info("🎉 ACTUALIZACIÓN COMPLETADA - 100,000 filas actualizadas")
            
    except Exception as e:
        logging.error(f"💥 Error general: {str(e)}")
        raise e

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

def download_sharepoint_file_direct(file_path, username, password):
    """Usar URL directa de descarga de SharePoint"""
    try:
        site_url = "https://escuelarefrigeracion.sharepoint.com/sites/ASESORASCOMERCIALES"
        
        # URL alternativa de descarga directa de SharePoint
        direct_url = f"{site_url}/_layouts/15/download.aspx?SourceUrl=/{file_path}"
        
        logging.info(f"📥 Descargando via URL directa: {file_path}")
        
        session = requests.Session()
        session.auth = (username, password)
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        }
        
        response = session.get(direct_url, headers=headers, timeout=60, verify=False, allow_redirects=True)
        
        # VERIFICAR SI ES UN EXCEL VÁLIDO
        if response.status_code == 200:
            content = response.content
            
            # Verificar si es un Excel válido
            if len(content) > 1000:  # Archivo razonablemente grande
                # Verificar signature de Excel (PK zip header)
                if content[:4] == b'PK\x03\x04' or b'xl/' in content[:100] or b'workbook' in content[:1000]:
                    logging.info(f"✅ Excel válido descargado (URL directa): {len(content)} bytes")
                    return BytesIO(content)
                else:
                    # Verificar si es HTML de error
                    content_str = content[:500].decode('utf-8', errors='ignore')
                    if any(keyword in content_str.lower() for keyword in ['<html', 'login', 'error', 'microsoft']):
                        logging.error(f"❌ Se descargó página HTML/error, no el Excel")
                        logging.debug(f"Primeros caracteres: {content_str[:200]}")
                    else:
                        logging.warning(f"⚠️ Archivo no parece Excel, pero continuando...")
                        return BytesIO(content)  # Intentar de todos modos
            else:
                logging.error(f"❌ Archivo demasiado pequeño: {len(content)} bytes")
                return None
        else:
            logging.error(f"❌ Error URL directa HTTP {response.status_code}")
            return None
            
    except Exception as e:
        logging.error(f"❌ Error descarga directa: {str(e)}")
        return None

def download_sharepoint_file_simple(file_path, username, password):
    """Método simple alternativo"""
    try:
        site_url = "https://escuelarefrigeracion.sharepoint.com/sites/ASESORASCOMERCIALES"
        full_url = f"{site_url}/{file_path}"
        
        logging.info(f"📥 Descargando via método simple: {file_path}")
        
        session = requests.Session()
        session.auth = (username, password)
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': '*/*'
        }
        
        response = session.get(full_url, headers=headers, timeout=60, verify=False, allow_redirects=True)
        
        if response.status_code == 200 and len(response.content) > 1000:
            logging.info(f"✅ Descarga simple exitosa: {len(response.content)} bytes")
            return BytesIO(response.content)
        else:
            logging.error(f"❌ Error método simple: HTTP {response.status_code}")
            return None
            
    except Exception as e:
        logging.error(f"❌ Error descarga simple: {str(e)}")
        return None

def find_table_in_excel(file_content, table_name):
    """Buscar tabla específica en el Excel"""
    try:
        # Especificar engine explícitamente para evitar errores
        excel_file = pd.ExcelFile(file_content, engine='openpyxl')
        
        # Estrategia 1: Buscar por nombre de tabla en celdas
        for sheet_name in excel_file.sheet_names:
            try:
                df_temp = pd.read_excel(file_content, sheet_name=sheet_name, header=None, engine='openpyxl')
                
                for row_idx, row in df_temp.iterrows():
                    for col_idx, value in row.items():
                        if pd.notna(value) and table_name.lower() in str(value).lower():
                            logging.info(f"✅ Tabla '{table_name}' encontrada en hoja: {sheet_name}, fila: {row_idx+1}")
                            df = pd.read_excel(file_content, sheet_name=sheet_name, header=row_idx, engine='openpyxl')
                            return df
            except Exception as e:
                logging.warning(f"⚠️ Error en hoja {sheet_name}: {str(e)}")
                continue
        
        # Estrategia 2: Usar primera hoja con datos
        logging.info(f"⚠️ No se encontró tabla por nombre, usando primera hoja con datos")
        try:
            df = pd.read_excel(file_content, sheet_name=0, engine='openpyxl')
            if not df.empty:
                return df
        except Exception as e:
            logging.warning(f"⚠️ Error primera hoja: {str(e)}")
            pass
            
        # Estrategia 3: Probar todas las hojas
        for sheet_name in excel_file.sheet_names:
            try:
                df = pd.read_excel(file_content, sheet_name=sheet_name, engine='openpyxl')
                if not df.empty:
                    logging.info(f"✅ Datos encontrados en hoja: {sheet_name}")
                    return df
            except Exception as e:
                logging.warning(f"⚠️ Error hoja {sheet_name}: {str(e)}")
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
