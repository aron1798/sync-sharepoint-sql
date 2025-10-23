import pandas as pd
import pyodbc
import os
import logging
import time
from io import StringIO

def sync_excel_to_sql():
    logging.info("🚀 Sincronizando Excel -> Azure SQL (MODO RÁPIDO)")
    
    # Configuración
    SQL_SERVER = os.environ['SQL_SERVER']
    SQL_DATABASE = os.environ['SQL_DATABASE']
    SQL_USERNAME = os.environ['SQL_USERNAME']
    SQL_PASSWORD = os.environ['SQL_PASSWORD']
    
    EXCEL_PATH = "base_combinada.xlsx"
    
    try:
        # 1. LEER EXCEL
        logging.info(f"📖 Leyendo Excel: {EXCEL_PATH}")
        df = pd.read_excel(EXCEL_PATH, sheet_name=0)
        
        if df.empty:
            logging.error("❌ Excel está vacío")
            return
            
        logging.info(f"✅ Excel leído: {len(df)} filas, {len(df.columns)} columnas")
        
        # 2. CONECTAR A SQL
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
        
        conn = connect_sql_with_retry(connection_string)
        cursor = conn.cursor()
        
        # 3. VERIFICAR SI LA TABLA TIENE DATOS
        cursor.execute("SELECT COUNT(*) FROM vendedoras_data")
        count = cursor.fetchone()[0]
        
        if count == 0:
            logging.info("🆕 Tabla vacía - INSERT RÁPIDO con BULK")
            registros_procesados = fast_bulk_insert(cursor, conn, df)
            logging.info(f"🎉 INSERT RÁPIDO COMPLETADO: {registros_procesados} registros en segundos")
        else:
            logging.info(f"🔄 Tabla tiene {count} registros - TRUNCATE + BULK INSERT")
            registros_procesados = fast_truncate_and_insert(cursor, conn, df)
            logging.info(f"🎉 ACTUALIZACIÓN RÁPIDA COMPLETADA: {registros_procesados} registros")
        
        conn.close()
        
    except Exception as e:
        logging.error(f"💥 Error: {str(e)}")
        raise

def fast_bulk_insert(cursor, conn, df):
    """INSERT RÁPIDO usando tabla temporal y BULK OPERATIONS"""
    start_time = time.time()
    
    # Limpiar y preparar datos
    df_clean = df.dropna(how='all').dropna(axis=1, how='all')
    df_clean.columns = [str(col).strip() for col in df_clean.columns]
    
    # Agregar columna ID
    df_clean['ID'] = range(1, len(df_clean) + 1)
    
    # 1. CREAR TABLA TEMPORAL
    cursor.execute("""
        CREATE TABLE #TempVendedoras (
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
    
    # 2. INSERT MASIVO usando executemany (MUCHO más rápido)
    placeholders = ','.join(['?'] * 16)
    sql = f"INSERT INTO #TempVendedoras VALUES ({placeholders})"
    
    # Preparar datos para bulk insert
    bulk_data = []
    for index, row in df_clean.iterrows():
        try:
            # Mapear columnas automáticamente
            valores = [
                row.get('ID', index + 1),
                str(row.get('Ejecutivo', ''))[:100],
                str(row.get('Telefono', ''))[:50],
                parse_fecha(row.get('FechaCreada')),
                str(row.get('Sede', ''))[:100],
                str(row.get('Programa', ''))[:100],
                str(row.get('Turno', ''))[:50],
                str(row.get('Codigo', ''))[:50],
                str(row.get('Canal', ''))[:100],
                str(row.get('Intervalo', ''))[:50],
                str(row.get('Medio', ''))[:100],
                str(row.get('Contacto', ''))[:100],
                str(row.get('Interesado', ''))[:100],
                str(row.get('Estado', ''))[:100],
                str(row.get('Objecion', ''))[:500],
                str(row.get('Observacion', ''))[:1000]
            ]
            bulk_data.append(valores)
        except Exception as e:
            logging.warning(f"⚠️ Error procesando fila {index}: {e}")
            continue
    
    # 3. EJECUTAR BULK INSERT (TODO DE UNA VEZ)
    logging.info(f"⚡ Insertando {len(bulk_data)} registros de una vez...")
    cursor.fast_executemany = True  # 🔥 ACTIVAR MODO RÁPIDO
    cursor.executemany(sql, bulk_data)
    
    # 4. COPIAR DE TEMPORAL A TABLA REAL
    cursor.execute("""
        INSERT INTO vendedoras_data 
        SELECT * FROM #TempVendedoras
    """)
    
    # 5. LIMPIAR TABLA TEMPORAL
    cursor.execute("DROP TABLE #TempVendedoras")
    
    conn.commit()
    
    end_time = time.time()
    logging.info(f"⏱️ Tiempo total: {end_time - start_time:.2f} segundos")
    
    return len(bulk_data)

def fast_truncate_and_insert(cursor, conn, df):
    """MÁS RÁPIDO: Borrar todo e insertar de nuevo"""
    start_time = time.time()
    
    # 1. BORRAR TODOS LOS DATOS EXISTENTES
    cursor.execute("TRUNCATE TABLE vendedoras_data")
    logging.info("🗑️ Tabla limpiada (TRUNCATE)")
    
    # 2. INSERTAR NUEVOS DATOS (más rápido que update)
    registros = fast_bulk_insert(cursor, conn, df)
    
    end_time = time.time()
    logging.info(f"⏱️ Tiempo total TRUNCATE+INSERT: {end_time - start_time:.2f} segundos")
    
    return registros

def parse_fecha(valor):
    """Manejo rápido de fechas"""
    if pd.isna(valor):
        return None
    try:
        if isinstance(valor, str):
            return pd.to_datetime(valor, errors='coerce').strftime('%Y-%m-%d %H:%M:%S')
        else:
            return valor.strftime('%Y-%m-%d %H:%M:%S')
    except:
        return None

def connect_sql_with_retry(connection_string, max_retries=3):
    for attempt in range(max_retries):
        try:
            conn = pyodbc.connect(connection_string)
            conn.timeout = 300  # Timeout más largo para operaciones largas
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
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    sync_excel_to_sql()
