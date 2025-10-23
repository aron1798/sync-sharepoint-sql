import pandas as pd
import pyodbc
import os
import logging
import time

def sync_excel_to_sql():
    logging.info("🚀 Sincronizando Excel -> Azure SQL")
    
    # Configuración
    SQL_SERVER = os.environ['SQL_SERVER']
    SQL_DATABASE = os.environ['SQL_DATABASE']
    SQL_USERNAME = os.environ['SQL_USERNAME']
    SQL_PASSWORD = os.environ['SQL_PASSWORD']
    
    # Excel DENTRO del repositorio
    EXCEL_PATH = "base_combinada.xlsx"
    
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
        # 1. LEER EXCEL DEL REPOSITORIO
        logging.info(f"📖 Leyendo Excel: {EXCEL_PATH}")
        df = pd.read_excel(EXCEL_PATH, sheet_name='Base_Azure')
        
        if df.empty:
            logging.error("❌ Excel está vacío")
            return
            
        logging.info(f"✅ Excel leído: {len(df)} filas, {len(df.columns)} columnas")
        
        # 2. CONECTAR A SQL
        conn = connect_sql_with_retry(connection_string)
        cursor = conn.cursor()
        
        # 3. ACTUALIZAR SQL
        registros_actualizados = update_database(cursor, df)
        
        # 4. CONFIRMAR
        conn.commit()
        conn.close()
        
        logging.info(f"🎉 ACTUALIZACIÓN COMPLETADA: {registros_actualizados} registros")
        
    except Exception as e:
        logging.error(f"💥 Error: {str(e)}")
        raise

def update_database(cursor, df):
    """Actualizar Azure SQL con datos del Excel"""
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
    
    registros_actualizados = 0
    
    for index, row in df_clean.iterrows():
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
            
            current_id = index + 1
            valores.append(current_id)
            
            # EJECUTAR UPDATE
            cursor.execute("""
                UPDATE vendedoras_data SET
                    Ejecutivo=?, Telefono=?, FechaCreada=?, Sede=?,
                    Programa=?, Turno=?, Codigo=?, Canal=?, Intervalo=?,
                    Medio=?, Contacto=?, Interesado=?, Estado=?,
                    Objecion=?, Observacion=?
                WHERE ID = ?
            """, *valores)
            
            registros_actualizados += 1
            
        except Exception as e:
            logging.warning(f"⚠️ Error fila {index}: {str(e)}")
            continue
    
    return registros_actualizados

def connect_sql_with_retry(connection_string, max_retries=3):
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
    sync_excel_to_sql()
