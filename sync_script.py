import pandas as pd
import pyodbc
import requests
from io import BytesIO
import os
import logging
import time
import urllib3
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
import base64

# Deshabilitar warnings de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def sync_sharepoint_to_sql():
    logging.info("🚀 Iniciando SINCRONIZACIÓN CON SELENIUM SharePoint")
    
    # ===== CONFIGURACIÓN =====
    SHAREPOINT_USERNAME = os.environ['SHAREPOINT_USERNAME']
    SHAREPOINT_PASSWORD = os.environ['SHAREPOINT_PASSWORD']
    SQL_SERVER = os.environ['SQL_SERVER']
    SQL_DATABASE = os.environ['SQL_DATABASE']
    SQL_USERNAME = os.environ['SQL_USERNAME']
    SQL_PASSWORD = os.environ['SQL_PASSWORD']
    
    # ===== CONFIGURACIÓN VENDEDORAS =====
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
    
    # Cadena de conexión Azure SQL
    connection_string = f"""
    Driver={{ODBC Driver 17 for SQL Server}};
    Server={SQL_SERVER};
    Database={SQL_DATABASE};
    Uid={SQL_USERNAME};
    Pwd={SQL_PASSWORD};
    Encrypt=yes;
    TrustServerCertificate=no;
    Connection Timeout=60;
    """
    
    # Configurar Selenium
    driver = None
    try:
        # 1. CONECTAR A AZURE SQL
        conn = connect_sql_with_retry(connection_string)
        cursor = conn.cursor()
        
        # 2. INICIAR NAVEGADOR SELENIUM
        driver = setup_selenium_driver()
        
        # 3. INICIAR SESIÓN EN SHAREPOINT
        if not login_to_sharepoint(driver, SHAREPOINT_USERNAME, SHAREPOINT_PASSWORD):
            logging.error("💥 No se pudo iniciar sesión en SharePoint")
            return
        
        # 4. PROCESAR CADA ARCHIVO
        total_registros = 0
        for config in VENDEDORAS_CONFIG:
            try:
                logging.info(f"🔄 Procesando: {config['table_name']}")
                
                # DESCARGAR ARCHIVO USANDO SELENIUM
                file_content = download_with_selenium(driver, config['public_link'], config['table_name'])
                
                if file_content:
                    # PROCESAR EXCEL
                    df = process_excel_file(file_content, config['table_name'])
                    
                    if df is not None and not df.empty:
                        df = df.head(10000)
                        
                        # ACTUALIZAR BASE DE DATOS
                        registros_procesados = update_database(cursor, df, config['rango_filas'])
                        total_registros += registros_procesados
                        logging.info(f"✅ {config['table_name']}: {registros_procesados} registros")
                    else:
                        logging.error(f"❌ No se encontraron datos en: {config['table_name']}")
                else:
                    logging.error(f"❌ No se pudo descargar: {config['table_name']}")
                    
                # Pequeña pausa entre descargas
                time.sleep(3)
                    
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
    finally:
        # Cerrar navegador
        if driver:
            driver.quit()

def setup_selenium_driver():
    """Configurar ChromeDriver para Selenium"""
    try:
        chrome_options = Options()
        
        # Configuración para entorno headless (sin interfaz gráfica)
        chrome_options.add_argument('--headless=new')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-plugins')
        chrome_options.add_argument('--disable-images')
        chrome_options.add_argument('--blink-settings=imagesEnabled=false')
        
        # Configuración para mejor rendimiento
        chrome_options.add_argument('--disable-javascript')
        chrome_options.add_argument('--disable-background-timer-throttling')
        chrome_options.add_argument('--disable-backgrounding-occluded-windows')
        chrome_options.add_argument('--disable-renderer-backgrounding')
        
        driver = webdriver.Chrome(options=chrome_options)
        driver.implicitly_wait(15)
        
        logging.info("✅ Navegador Chrome configurado")
        return driver
        
    except Exception as e:
        logging.error(f"❌ Error configurando Selenium: {str(e)}")
        raise

def login_to_sharepoint(driver, username, password):
    """Iniciar sesión en SharePoint"""
    try:
        logging.info("🔐 Iniciando sesión en SharePoint...")
        
        # Ir a página principal de SharePoint
        login_url = "https://escuelarefrigeracion.sharepoint.com/sites/ASESORASCOMERCIALES"
        driver.get(login_url)
        
        time.sleep(5)
        
        # Esperar y completar formulario de login
        try:
            email_field = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.ID, "i0116"))
            )
            email_field.clear()
            email_field.send_keys(username)
            logging.info("✅ Email ingresado")
            
            next_button = driver.find_element(By.ID, "idSIButton9")
            next_button.click()
            logging.info("✅ Click en siguiente")
        except Exception as e:
            logging.error(f"❌ Error en campo email: {str(e)}")
            return False
        
        time.sleep(3)
        
        # Esperar campo de password
        try:
            password_field = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.ID, "i0118"))
            )
            password_field.clear()
            password_field.send_keys(password)
            logging.info("✅ Password ingresado")
            
            signin_button = driver.find_element(By.ID, "idSIButton9")
            signin_button.click()
            logging.info("✅ Click en iniciar sesión")
        except Exception as e:
            logging.error(f"❌ Error en campo password: {str(e)}")
            return False
        
        time.sleep(5)
        
        # Esperar posible pantalla de "Mantener sesión iniciada"
        try:
            stay_signed_in = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "idSIButton9"))
            )
            stay_signed_in.click()
            logging.info("✅ Click en mantener sesión")
            time.sleep(3)
        except:
            logging.info("ℹ️ No apareció pantalla de mantener sesión")
            pass
        
        # Verificar que el login fue exitoso - esperar a que cargue algún elemento de SharePoint
        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Verificar que no estamos en página de login
            current_url = driver.current_url
            if "login.microsoftonline.com" not in current_url and "login.live.com" not in current_url:
                logging.info("✅ Sesión iniciada exitosamente en SharePoint")
                return True
            else:
                logging.error("❌ Still on login page after authentication")
                return False
                
        except Exception as e:
            logging.error(f"❌ Error verificando login: {str(e)}")
            return False
        
    except Exception as e:
        logging.error(f"❌ Error en login: {str(e)}")
        return False

def download_with_selenium(driver, public_link, table_name):
    """Descargar archivo usando Selenium - Método directo via URL"""
    try:
        logging.info(f"📥 Descargando: {table_name}")
        
        # Método DIRECTO: Construir URL de descarga directa
        # Extraer el ID único del link público
        if "?e=" in public_link:
            base_url = public_link.split("?e=")[0]
            
        # Construir URL de descarga directa
        download_url = public_link.replace("/:x:", "/:x:/t:")
        download_url += "&download=1"
        
        logging.info(f"🔗 URL de descarga: {download_url}")
        
        # Navegar a la URL de descarga
        driver.get(download_url)
        time.sleep(8)  # Esperar a que procese la descarga
        
        # Verificar si se descargó contenido
        current_url = driver.current_url
        page_source = driver.page_source
        
        # Si la página contiene datos de Excel, intentar extraerlos
        if "PK" in page_source[:1000] or "xl/" in page_source:
            logging.info("✅ Contenido Excel detectado en página")
            # Extraer el contenido binario
            script = """
            var xhr = new XMLHttpRequest();
            xhr.open('GET', arguments[0], false);
            xhr.responseType = 'arraybuffer';
            xhr.send();
            
            if (xhr.status === 200) {
                var arrayBuffer = xhr.response;
                var base64 = btoa(String.fromCharCode.apply(null, new Uint8Array(arrayBuffer)));
                return base64;
            }
            return null;
            """
            
            try:
                file_base64 = driver.execute_script(script, download_url)
                if file_base64:
                    file_content = base64.b64decode(file_base64)
                    if len(file_content) > 1000:
                        logging.info(f"✅ Archivo descargado: {len(file_content)} bytes")
                        return BytesIO(file_content)
            except:
                pass
        
        # Método alternativo: usar requests con cookies de Selenium
        try:
            cookies = driver.get_cookies()
            session = requests.Session()
            
            for cookie in cookies:
                session.cookies.set(cookie['name'], cookie['value'])
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = session.get(download_url, headers=headers, timeout=30, verify=False)
            
            if response.status_code == 200 and len(response.content) > 1000:
                # Verificar que sea Excel
                if response.content[:4] == b'PK\x03\x04' or b'xl/' in response.content[:1000]:
                    logging.info(f"✅ Archivo descargado via requests: {len(response.content)} bytes")
                    return BytesIO(response.content)
                else:
                    # Verificar si es HTML de error
                    content_preview = response.content[:500].decode('utf-8', errors='ignore')
                    if '<html' in content_preview.lower() or 'error' in content_preview.lower():
                        logging.error(f"❌ SharePoint devolvió error HTML")
                    else:
                        logging.warning("⚠️ Contenido no reconocido, intentando procesar...")
                        return BytesIO(response.content)
            else:
                logging.error(f"❌ Error en descarga: HTTP {response.status_code}, tamaño: {len(response.content)}")
                
        except Exception as e:
            logging.error(f"❌ Error en método alternativo: {str(e)}")
        
        return None
        
    except Exception as e:
        logging.error(f"❌ Error en descarga Selenium: {str(e)}")
        return None

def process_excel_file(file_content, table_name):
    """Procesar archivo Excel"""
    try:
        file_content.seek(0)
        
        # Verificar que el contenido sea válido
        content_preview = file_content.read(100)
        file_content.seek(0)
        
        if b'PK' not in content_preview and b'xl' not in content_preview:
            logging.warning("⚠️ El contenido no parece ser un archivo Excel válido")
        
        for engine in ['openpyxl', 'xlrd']:
            try:
                df = pd.read_excel(file_content, engine=engine, sheet_name=0)
                if not df.empty and len(df.columns) > 1:
                    logging.info(f"✅ Excel procesado con {engine}: {len(df)} filas, {len(df.columns)} columnas")
                    return clean_dataframe(df)
            except Exception as e:
                logging.warning(f"⚠️ Engine {engine} falló: {str(e)}")
                file_content.seek(0)
                continue
        
        # Intentar con todas las hojas
        file_content.seek(0)
        for engine in ['openpyxl', 'xlrd']:
            try:
                excel_file = pd.ExcelFile(file_content, engine=engine)
                for sheet_name in excel_file.sheet_names:
                    try:
                        df = pd.read_excel(file_content, sheet_name=sheet_name, engine=engine)
                        if not df.empty and len(df.columns) > 1:
                            logging.info(f"✅ Datos encontrados en hoja {sheet_name}: {len(df)} filas")
                            return clean_dataframe(df)
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
    """Limpiar DataFrame"""
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
    batch_data = []
    
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
