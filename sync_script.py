import pandas as pd
import pyodbc
import os
import logging
import time

def sync_powerquery_excel_to_sql():
    logging.info("🚀 Sincronizando Excel con Power Query -> Azure SQL")
    
    # Configuración
    SQL_SERVER = os.environ['SQL_SERVER']
    SQL_DATABASE = os.environ['SQL_DATABASE']
    SQL_USERNAME = os.environ['SQL_USERNAME']
    SQL_PASSWORD = os.environ['SQL_PASSWORD']
    
    # Ruta de tu Excel con Power Query
    EXCEL_PATH = r"C:\Users\ASEBASTIAN\Desktop\pruebas\BuscadorBase.xlsx"
    
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
        # 1. LEER EXCEL ACTUALIZADO POR POWER QUERY
        logging.info(f"📖 Leyendo Excel con Power Query: {EXCEL_PATH}")
        
        # Leer la hoja donde Power Query pone los datos combinados
        df = pd.read_excel(EXCEL_PATH, sheet_name='Base_Azure')  # Ajusta el nombre de la hoja
        
        if df.empty:
            logging.error("❌ Excel está vacío - Power Query no ha cargado datos")
            return
            
        logging.info(f"✅ Excel leído: {len(df)} filas, {len(df.columns)} columnas")
        logging.info(f"📋 Columnas encontradas: {list(df.columns)}")
        
        # 2. CONECTAR A SQL
        conn = connect_sql_with_retry(connection_string)
        cursor = conn.cursor()
        
        # 3. ACTUALIZAR SQL CON LOS DATOS ACTUALIZADOS DE POWER QUERY
        registros_actualizados = update_database(cursor, df)
        
        # 4. CONFIRMAR CAMBIOS
        conn.commit()
        conn.close()
        
        logging.info(f"🎉 SINCRONIZACIÓN COMPLETADA: {registros_actualizados} registros actualizados en Azure SQL")
        
    except Exception as e:
        logging.error(f"💥 Error: {str(e)}")
        raise

def update_database(cursor, df):
    """Actualizar Azure SQL con datos actualizados de Power Query"""
    # Limpiar datos
    df_clean = df.dropna(how='all').dropna(axis=1, how='all')
    df_clean.columns = [str(col).strip() for col in df_clean.columns]
    
    # Mapeo de columnas
    mapeo_columnas = {}
    columnas_requeridas = ['Ejecutivo', 'Telefono', 'FechaCreada', 'Sede', 'Programa', 'Turno', 
                          'Codigo', 'Canal', 'Intervalo', 'Medio', 'Contacto', 'Interesado', 
                          'Estado', 'Objecion', 'Observacion']
    
    for col_requerida in columnas_requeridas:
        for col_real in df_clean.columns:
            if col_requerida.lower() in col_real.lower():
                mapeo_columnas[col_requerida] = col_real
                break
    
    logging.info(f"🔍 Columnas mapeadas: {len(mapeo_columnas)}/{len(columnas_requeridas)}")
    
    registros_actualizados = 0
    
    for index, row in df_clean.iterrows():
        try:
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
            
            # ID para el UPDATE
            current_id = index + 1
            valores.append(current_id)
            
            # EJECUTAR UPDATE EN AZURE SQL
            cursor.execute("""
                UPDATE vendedoras_data SET
                    Ejecutivo=?, Telefono=?, FechaCreada=?, Sede=?,
                    Programa=?, Turno=?, Codigo=?, Canal=?, Intervalo=?,
                    Medio=?, Contacto=?, Interesado=?, Estado=?,
                    Objecion=?, Observacion=?
                WHERE ID = ?
            """, *valores)
            
            registros_actualizados += 1
            
            # Log cada 500 registros
            if registros_actualizados % 500 == 0:
                logging.info(f"📊 Progreso: {registros_actualizados} registros")
            
        except Exception as e:
            logging.warning(f"⚠️ Error actualizando fila {index}: {str(e)}")
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
            logging.FileHandler('powerquery_sync.log', encoding='utf-8')
        ]
    )
    
    start_time = time.time()
    sync_powerquery_excel_to_sql()
    end_time = time.time()
    
    logging.info(f"⏱️ Tiempo total: {end_time - start_time:.2f} segundos")
