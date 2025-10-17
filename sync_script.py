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
    logging.info("🚀 Iniciando SINCRONIZACIÓN CON API OFICIAL SharePoint")
    
    # ===== CONFIGURACIÓN =====
    SHAREPOINT_SITE = "https://escuelarefrigeracion.sharepoint.com/sites/ASESORASCOMERCIALES"
    SHAREPOINT_USERNAME = os.environ['SHAREPOINT_USERNAME']
    SHAREPOINT_PASSWORD = os.environ['SHAREPOINT_PASSWORD']
    SQL_SERVER = os.environ['SQL_SERVER']
    SQL_DATABASE = os.environ['SQL_DATABASE']
    SQL_USERNAME = os.environ['SQL_USERNAME']
    SQL_PASSWORD = os.environ['SQL_PASSWORD']
    
    # ===== CONFIGURACIÓN VENDEDORAS =====
    VENDEDORAS_CONFIG = [
        {
            "path": "Shared Documents/2. BASE PROSPECTOS/BASE GENERAL/Base Alonso Huaman.xlsx",
            "table_name": "Base_Alonso",
            "rango_filas": "1:10000"
        },
        {
            "path": "Shared Documents/2. BASE PROSPECTOS/BASE GENERAL/Base Diana Chavez.xlsx",
            "table_name": "Base_Diana",
            "rango_filas": "10001:20000"
        },
        {
            "path": "Shared Documents/2. BASE PROSPECTOS/BASE GENERAL/Base Gerson Falen.xlsx", 
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
        # 1. CONECTAR A AZURE SQL
        conn = connect_sql_with_retry(connection_string)
        cursor = conn.cursor()
        
        # 2. PROCESAR CADA ARCHIVO
        total_registros = 0
        for config in VENDEDORAS_CONFIG:
            try:
                logging.info(f"🔄 Procesando: {config['table_name']}")
                
                # DESCARGAR ARCHIVO USANDO MÉTODO DIRECTO
                file_content = download_sharepoint_direct(
                    config['path'], 
                    SHAREPOINT_USERNAME, 
                    SHAREPOINT_PASSWORD
                )
                
                if file_content:
                    # PROCESAR EXCEL
                    df = process_excel_file(file_content, config['table_name'])
                    
                    if df is not None and not df.empty:
                        df = df.head(10000)  # Limitar a 10,000 filas
                        
                        # ACTUALIZAR BASE DE DATOS
                        registros_procesados = update_database(cursor, df, config['rango_filas'])
                        total_registros += registros_procesados
                        logging.info(f"✅ {config['table_name']}: {registros_procesados} registros")
                    else:
                        logging.error(f"❌ No se encontraron datos en: {config['table_name']}")
                else:
                    logging.error(f"❌ No se pudo descargar: {config['path']}")
                    
            except Exception as e:
                logging.error(f"❌ Error procesando {config['table_name']}: {str(e)}")
                continue
        
        # CONFIRMAR CAMBIOS
        conn.commit()
        conn.close()
        logging.info(f"🎉 SINCRONIZACIÓN COMPLETADA - {total_registros} filas actualizadas")
        
    except Exception as e:
        logging.error(f"💥 Error general: {str(e)}")
        raise e

def download_sharepoint_direct(file_path, username, password):
    """MÉTODO DIRECTO Y SIMPLE para descargar de SharePoint"""
    try:
        logging.info(f"📥 Intentando descargar: {file_path}")
        
        # Construir URL directa
        site_url = "https://escuelarefrigeracion.sharepoint.com/sites/ASESORASCOMERCIALES"
        file_url = f"{site_url}/_api/web/GetFileByServerRelativeUrl('/sites/ASESORASCOMERCIALES/{file_path}')/$value"
        
        logging.info(f"🔗 URL: {file_url}")
        
        # Headers para API SharePoint
        headers = {
            'Accept': 'application/json;odata=verbose',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        # Hacer la petición con autenticación básica
        response = requests.get(
            file_url,
            auth=(username, password),
            headers=headers,
            timeout=30,
            verify=False
        )
        
        logging.info(f"📊 Response: HTTP {response.status_code}, Size: {len(response.content)} bytes")
        
        if response.status_code == 200:
            content = response.content
            
            # Verificar que sea un Excel válido
            if len(content) > 1000:
                # Verificar firma de archivo Excel
                if (content[:4] == b'PK\x03\x04' or  # Firma ZIP
                    b'[Content_Types]' in content[:2000] or 
                    b'xl/' in content[:1000]):
                    logging.info(f"✅ Excel válido descargado: {len(content)} bytes")
                    return BytesIO(content)
                else:
                    # Verificar si es error HTML
                    content_preview = content[:500].decode('utf-8', errors='ignore')
                    if any(keyword in content_preview.lower() for keyword in ['<html', 'error', 'login']):
                        logging.error(f"❌ SharePoint devolvió error HTML: {content_preview[:200]}")
                        return None
                    else:
                        logging.warning("⚠️ Contenido no reconocido, pero intentando procesar...")
                        return BytesIO(content)
            else:
                logging.error("❌ Archivo demasiado pequeño")
                return None
        else:
            logging.error(f"❌ Error HTTP {response.status_code}")
            if response.content:
                error_content = response.content[:500].decode('utf-8', errors='ignore')
                logging.info(f"📄 Contenido error: {error_content[:200]}")
            return None
            
    except Exception as e:
        logging.error(f"❌ Error en descarga directa: {str(e)}")
        return None

def process_excel_file(file_content, table_name):
    """Procesar archivo Excel de forma robusta"""
    try:
        # Reiniciar el cursor del archivo
        file_content.seek(0)
        
        # Intentar con diferentes engines
        engines = ['openpyxl', 'xlrd']
        
        for engine in engines:
            try:
                # Leer primera hoja
                df = pd.read_excel(file_content, engine=engine, sheet_name=0)
                
                if not df.empty and len(df.columns) > 1:
                    logging.info(f"✅ Excel procesado con {engine}: {len(df)} filas, {len(df.columns)} columnas")
                    
                    # Limpiar datos
                    df = clean_dataframe(df)
                    return df
                    
            except Exception as e:
                logging.warning(f"⚠️ Engine {engine} falló: {str(e)}")
                file_content.seek(0)  # Reiniciar para siguiente engine
                continue
        
        # Si ambos engines fallan, intentar método de fuerza bruta
        logging.warning("🔄 Intentando método de fuerza bruta...")
        file_content.seek(0)
        
        for sheet_name in [0, 1, 2]:  # Probar primeras 3 hojas
            for engine in engines:
                try:
                    df = pd.read_excel(file_content, engine=engine, sheet_name=sheet_name, header=None)
                    if not df.empty and len(df.columns) > 3:  # Debe tener al menos 4 columnas
                        # Buscar fila de encabezados
                        for header_row in range(min(5, len(df))):
                            try:
                                df_with_header = pd.read_excel(file_content, engine=engine, sheet_name=sheet_name, header=header_row)
                                if not df_with_header.empty:
                                    logging.info(f"✅ Datos encontrados en hoja {sheet_name}, fila header {header_row}")
                                    return clean_dataframe(df_with_header)
                            except:
                                continue
                except:
                    continue
        
        logging.error("❌ No se pudo procesar el Excel con ningún método")
        return None
        
    except Exception as e:
        logging.error(f"❌ Error procesando Excel: {str(e)}")
        return None

def clean_dataframe(df):
    """Limpiar y normalizar DataFrame"""
    try:
        # Eliminar filas completamente vacías
        df_clean = df.dropna(how='all')
        
        # Eliminar columnas completamente vacías
        df_clean = df_clean.dropna(axis=1, how='all')
        
        # Normalizar nombres de columnas
        df_clean.columns = [str(col).strip().replace('\n', ' ').replace('\r', '') for col in df_clean.columns]
        
        logging.info(f"🧹 DataFrame limpiado: {len(df_clean)} filas, {len(df_clean.columns)} columnas")
        return df_clean
        
    except Exception as e:
        logging.error(f"❌ Error limpiando DataFrame: {str(e)}")
        return df

def update_database(cursor, df, rango_filas):
    """Actualizar base de datos"""
    start_id, end_id = map(int, rango_filas.split(':'))
    
    # Normalizar columnas
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
    
    logging.info(f"🔍 Columnas mapeadas: {len(mapeo_columnas)}/{len(columnas_requeridas)}")
    
    registros_actualizados = 0
    
    for index, row in df.iterrows():
        current_id = start_id + index
        
        if current_id > end_id:
            break
        
        try:
            # Obtener valores
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
                elif pd.isna(valor):
                    valor = None
                
                valores.append(valor)
            
            valores.append(current_id)
            
            # Actualizar registro
            cursor.execute("""
                UPDATE vendedoras_data SET
                    Ejecutivo = ?, Telefono = ?, FechaCreada = ?, Sede = ?,
                    Programa = ?, Turno = ?, Codigo = ?, Canal = ?, Intervalo = ?,
                    Medio = ?, Contacto = ?, Interesado = ?, Estado = ?,
                    Objecion = ?, Observacion = ?
                WHERE ID = ?
            """, *valores)
            
            registros_actualizados += 1
            
            # Log cada 100 registros
            if registros_actualizados % 100 == 0:
                logging.info(f"📊 Progreso {current_id}: {registros_actualizados} registros")
            
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
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('sharepoint_sync.log', encoding='utf-8')
        ]
    )
    
    start_time = time.time()
    sync_sharepoint_to_sql()
    end_time = time.time()
    
    logging.info(f"⏱️ Tiempo total: {end_time - start_time:.2f} segundos")
