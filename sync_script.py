import pandas as pd
import pyodbc
import requests
from io import BytesIO
import os
import logging
import time
import urllib3
import random
from datetime import datetime

# Deshabilitar warnings de SSL (temporal para pruebas)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def sync_sharepoint_to_sql():
    logging.info("🚀 Iniciando ACTUALIZACIÓN OPTIMIZADA SharePoint -> Azure SQL")
    
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
            "rango_filas": "10001:20000"
        },
        {
            "path": "Documentos compartidos/2. BASE PROSPECTOS/BASE GENERAL/Base Gerson Falen.xlsx",
            "table_name": "Base_Gerson",
            "rango_filas": "20001:30000"
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
        
        # OPTIMIZACIÓN: Crear tabla temporal para bulk insert
        cursor.execute("""
            IF OBJECT_ID('tempdb..#TempVendedorasData') IS NOT NULL
                DROP TABLE #TempVendedorasData
            
            CREATE TABLE #TempVendedorasData (
                ID INT,
                Ejecutivo NVARCHAR(100),
                Telefono NVARCHAR(50),
                FechaCreada DATETIME,
                Sede NVARCHAR(100),
                Programa NVARCHAR(100),
                Turno NVARCHAR(50),
                Codigo NVARCHAR(50),
                Canal NVARCHAR(100),
                Intervalo NVARCHAR(50),
                Medio NVARCHAR(100),
                Contacto NVARCHAR(100),
                Interesado NVARCHAR(100),
                Estado NVARCHAR(100),
                Objecion NVARCHAR(500),
                Observacion NVARCHAR(1000)
            )
        """)
        
        # Procesar cada vendedora
        total_registros = 0
        for config in VENDEDORAS_CONFIG:
            try:
                logging.info(f"🔄 Procesando: {config['table_name']}")
                
                # SOLUCIÓN HÍBRIDA: Links públicos + autenticación
                file_content = download_sharepoint_file_public_with_auth(
                    config['path'], 
                    SHAREPOINT_USERNAME, 
                    SHAREPOINT_PASSWORD
                )
                
                if file_content is None:
                    logging.error(f"❌ No se pudo descargar: {config['path']}")
                    continue
                
                # Buscar la tabla en el Excel
                df = find_table_in_excel_optimized(file_content, config['table_name'])
                
                if df is not None and not df.empty:
                    # Limitar a 10,000 filas máximo
                    df = df.head(10000)
                    
                    # OPTIMIZACIÓN: Insertar en tabla temporal
                    registros_procesados = insert_to_temp_table(cursor, df, config['rango_filas'])
                    total_registros += registros_procesados
                    
                    logging.info(f"✅ {config['table_name']}: {registros_procesados} registros preparados")
                else:
                    logging.error(f"❌ No se encontró tabla válida: {config['table_name']}")
                
            except Exception as e:
                logging.error(f"❌ Error procesando {config['table_name']}: {str(e)}")
                continue
        
        # OPTIMIZACIÓN: Actualización masiva desde tabla temporal
        if total_registros > 0:
            logging.info(f"🔄 Realizando actualización masiva de {total_registros} registros...")
            actualizacion_masiva(cursor)
        
        # Confirmar todos los cambios
        conn.commit()
        conn.close()
        logging.info(f"🎉 ACTUALIZACIÓN COMPLETADA - {total_registros} filas actualizadas")
            
    except Exception as e:
        logging.error(f"💥 Error general: {str(e)}")
        raise e

def download_sharepoint_file_public_with_auth(file_path, username, password):
    """SOLUCIÓN HÍBRIDA OPTIMIZADA: Links públicos + autenticación"""
    
    # LINKS PÚBLICOS ÚNICOS
    public_links = {
        "Base Alonso Huaman.xlsx": "https://escuelarefrigeracion.sharepoint.com/:x:/s/ASESORASCOMERCIALES/EaIlXhIcpYBFkaxzXu7aQIQBAu_zaldlNLgtz7y6bOMyCA?e=yVU2iw",
        "Base Diana Chavez.xlsx": "https://escuelarefrigeracion.sharepoint.com/:x:/s/ASESORASCOMERCIALES/EeRBRnXXABpPhWkYk87UcjoB-VltTBFz6MRSQ-VEbucP8Q?e=bvUv7V",
        "Base Gerson Falen.xlsx": "https://escuelarefrigeracion.sharepoint.com/:x:/s/ASESORASCOMERCIALES/EQGtk5_fCslJowZlY8g7kTEBLfD29swdE4DK_0nDfBZ7qw?e=36cm8P"
    }
    
    filename = file_path.split('/')[-1]
    
    if filename in public_links:
        try:
            logging.info(f"🔗 Descargando link público CON AUTENTICACIÓN: {filename}")
            
            session = requests.Session()
            session.auth = (username, password)  # ⬅️ CLAVE: Agregar autenticación
            
            # Headers optimizados
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
                'Accept': '*/*',
                'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8'
            }
            
            # OPTIMIZACIÓN: Timeout más agresivo y seguimiento de redirecciones
            response = session.get(
                public_links[filename], 
                headers=headers, 
                timeout=30,  # ⬅️ Reducido de 60 a 30 segundos
                verify=False,
                allow_redirects=True  # ⬅️ IMPORTANTE: Seguir redirecciones
            )
            
            logging.info(f"📊 Response final: HTTP {response.status_code}, Size: {len(response.content)} bytes")
            
            if response.status_code == 200:
                content = response.content
                
                # Verificación robusta de contenido Excel
                if len(content) > 1000:
                    content_start = content[:500].decode('utf-8', errors='ignore')
                    
                    # Si es HTML, falló la autenticación
                    if any(keyword in content_start.lower() for keyword in ['<!doctype', '<html', 'login', 'redirect', 'microsoft']):
                        logging.error("❌ Autenticación falló - SharePoint devolvió página HTML")
                        logging.debug(f"Contenido inicial: {content_start[:200]}")
                        return None
                    
                    # Verificar firma de archivo Excel
                    if (content[:4] == b'PK\x03\x04' or  # Firma ZIP de Office
                        b'[Content_Types]' in content[:2000] or 
                        b'xl/' in content[:1000]):
                        logging.info(f"✅ ÉXITO: Excel válido detectado - {len(content)} bytes")
                        return BytesIO(content)
                    else:
                        # Intentar procesar de todos modos
                        logging.warning(f"⚠️ Firma Excel no estándar, intentando procesar...")
                        return BytesIO(content)
                else:
                    logging.error(f"❌ Archivo demasiado pequeño: {len(content)} bytes")
                    return None
            else:
                logging.error(f"❌ Error HTTP {response.status_code}")
                return None
                
        except requests.exceptions.Timeout:
            logging.error("❌ Timeout en descarga de SharePoint")
            return None
        except Exception as e:
            logging.error(f"❌ Error en descarga con autenticación: {str(e)}")
            return None
    else:
        logging.warning(f"⚠️ No hay link público configurado para: {filename}")
        return None

def find_table_in_excel_optimized(file_content, table_name):
    """Búsqueda optimizada de tabla en Excel"""
    try:
        # OPTIMIZACIÓN: Probar solo openpyxl (más rápido para .xlsx)
        try:
            excel_file = pd.ExcelFile(file_content, engine='openpyxl')
            
            # Estrategia 1: Buscar en primera hoja (caso más común)
            try:
                df = pd.read_excel(file_content, sheet_name=0, engine='openpyxl')
                if not df.empty and len(df.columns) > 1:
                    logging.info("✅ Datos encontrados en primera hoja")
                    return clean_dataframe(df)
            except Exception as e:
                logging.warning(f"⚠️ Error en primera hoja: {str(e)}")
                pass
            
            # Estrategia 2: Buscar en todas las hojas
            for sheet_name in excel_file.sheet_names:
                try:
                    df = pd.read_excel(file_content, sheet_name=sheet_name, engine='openpyxl')
                    if not df.empty and len(df.columns) > 1:
                        logging.info(f"✅ Datos encontrados en hoja: {sheet_name}")
                        return clean_dataframe(df)
                except Exception as e:
                    logging.warning(f"⚠️ Error en hoja {sheet_name}: {str(e)}")
                    continue
                    
        except Exception as e:
            logging.warning(f"⚠️ Openpyxl falló: {str(e)}")
            # Fallback a xlrd si es necesario
            try:
                df = pd.read_excel(file_content, engine='xlrd')
                return clean_dataframe(df)
            except:
                pass
                
        logging.error("❌ No se pudo leer el archivo con ningún engine")
        return None
        
    except Exception as e:
        logging.error(f"❌ Error buscando tabla: {str(e)}")
        return None

def clean_dataframe(df):
    """Limpieza y normalización del DataFrame"""
    # Eliminar filas completamente vacías
    df = df.dropna(how='all')
    
    # Normalizar nombres de columnas
    df.columns = [str(col).strip().replace('\n', ' ').replace('\r', '') for col in df.columns]
    
    # Eliminar columnas completamente vacías
    df = df.dropna(axis=1, how='all')
    
    return df

def insert_to_temp_table(cursor, df, rango_filas):
    """Inserción optimizada en tabla temporal"""
    start_id, end_id = map(int, rango_filas.split(':'))
    
    # Normalizar nombres de columnas
    df.columns = [str(col).strip() for col in df.columns]
    
    # Mapeo optimizado de columnas
    mapeo_columnas = {}
    columnas_requeridas = ['Ejecutivo', 'Telefono', 'FechaCreada', 'Sede', 'Programa', 'Turno', 
                          'Codigo', 'Canal', 'Intervalo', 'Medio', 'Contacto', 'Interesado', 
                          'Estado', 'Objecion', 'Observacion']
    
    for col_requerida in columnas_requeridas:
        for col_real in df.columns:
            if col_requerida.lower() in col_real.lower():
                mapeo_columnas[col_requerida] = col_real
                break
    
    registros_insertados = 0
    batch_data = []
    
    for index, row in df.iterrows():
        current_id = start_id + index
        
        if current_id > end_id:
            break
        
        try:
            # Obtener valores usando mapeo
            valores = [current_id]  # ID primero
            
            for col_requerida in columnas_requeridas:
                col_real = mapeo_columnas.get(col_requerida, col_requerida)
                valor = row.get(col_real, '')
                
                # Manejo optimizado de fechas
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
            
            batch_data.append(valores)
            registros_insertados += 1
            
            # OPTIMIZACIÓN: Inserción por lotes cada 100 registros
            if len(batch_data) >= 100:
                insert_batch(cursor, batch_data)
                batch_data = []
                
        except Exception as e:
            logging.warning(f"⚠️ Error procesando fila {index}: {str(e)}")
            continue
    
    # Insertar lote final
    if batch_data:
        insert_batch(cursor, batch_data)
    
    return registros_insertados

def insert_batch(cursor, batch_data):
    """Inserción por lotes optimizada"""
    try:
        placeholders = ','.join(['?'] * 16)  # 16 columnas
        sql = f"INSERT INTO #TempVendedorasData VALUES ({placeholders})"
        cursor.executemany(sql, batch_data)
    except Exception as e:
        logging.error(f"❌ Error en inserción por lote: {str(e)}")

def actualizacion_masiva(cursor):
    """Actualización masiva desde tabla temporal"""
    try:
        # OPTIMIZACIÓN: Single UPDATE con JOIN
        cursor.execute("""
            UPDATE v 
            SET 
                v.Ejecutivo = t.Ejecutivo,
                v.Telefono = t.Telefono,
                v.FechaCreada = t.FechaCreada,
                v.Sede = t.Sede,
                v.Programa = t.Programa,
                v.Turno = t.Turno,
                v.Codigo = t.Codigo,
                v.Canal = t.Canal,
                v.Intervalo = t.Intervalo,
                v.Medio = t.Medio,
                v.Contacto = t.Contacto,
                v.Interesado = t.Interesado,
                v.Estado = t.Estado,
                v.Objecion = t.Objecion,
                v.Observacion = t.Observacion
            FROM vendedoras_data v
            INNER JOIN #TempVendedorasData t ON v.ID = t.ID
        """)
        
        filas_afectadas = cursor.rowcount
        logging.info(f"📊 Actualización masiva completada: {filas_afectadas} filas afectadas")
        
    except Exception as e:
        logging.error(f"❌ Error en actualización masiva: {str(e)}")
        raise

def connect_sql_with_retry(connection_string, max_retries=3):
    """Conectar a SQL con reintentos optimizado"""
    for attempt in range(max_retries):
        try:
            conn = pyodbc.connect(connection_string)
            logging.info(f"✅ Conexión SQL exitosa (intento {attempt + 1})")
            return conn
        except pyodbc.OperationalError as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5  # ⬅️ Reducido de 10 a 5 segundos
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
            logging.FileHandler('sync_sharepoint.log')
        ]
    )
    
    start_time = time.time()
    sync_sharepoint_to_sql()
    end_time = time.time()
    
    logging.info(f"⏱️ Tiempo total de ejecución: {end_time - start_time:.2f} segundos")
