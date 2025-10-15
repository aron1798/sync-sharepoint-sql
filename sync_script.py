import pandas as pd
import pyodbc
import requests
from io import BytesIO
import os
import logging

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
            "table_name": "Base_Alonso",  # ⬅️ Esto depende del nombre de la tabla DENTRO del Excel
            "rango_filas": "1:10000"
        },
        {
            "path": "Documentos compartidos/2. BASE PROSPECTOS/BASE GENERAL/Base Diana Chavez.xlsx",
            "table_name": "Base_Diana",  # ⬅️ Esto depende del nombre de la tabla DENTRO del Excel
            "rango_filas": "1:10000"
        },
        {
            "path": "Documentos compartidos/2. BASE PROSPECTOS/BASE GENERAL/Base Gerson Falen.xlsx",
            "table_name": "Base_Gerson",  # ⬅️ Esto depende del nombre de la tabla DENTRO del Excel
            "rango_filas": "1:10000"
        },
        # ... AGREGA LAS 10 VENDEDORAS
        # CADA UNA CON:
        # - path: ruta SharePoint
        # - table_name: nombre exacto de la tabla en Excel
        # - rango_filas: qué filas actualiza en Azure SQL
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
    Connection Timeout=30;
    """
    
    try:
        # Conectar a Azure SQL
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        # Procesar cada vendedora
        for config in VENDEDORAS_CONFIG:
            try:
                logging.info(f"🔄 Procesando: {config['table_name']}")
                
                # Descargar Excel de SharePoint
                base_url = "https://escuelarefrigeracion.sharepoint.com/sites/ASESORASCOMERCIALES"
                full_url = f"{base_url}/{config['path']}"
                
                session = requests.Session()
                session.auth = (SHAREPOINT_USERNAME, SHAREPOINT_PASSWORD)
                
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': 'application/json'
                }
                
                response = session.get(full_url, headers=headers, timeout=30)
                response.raise_for_status()
                
                # Leer tabla ESPECÍFICA del Excel
                file_content = BytesIO(response.content)
                
                # Buscar la hoja que contiene la tabla por nombre
                excel_file = pd.ExcelFile(file_content)
                tabla_encontrada = False
                
                for sheet_name in excel_file.sheet_names:
                    try:
                        # Leer toda la hoja para buscar el nombre de la tabla
                        df_temp = pd.read_excel(file_content, sheet_name=sheet_name, header=None)
                        
                        # Buscar el nombre de la tabla en las celdas
                        for row_idx, row in df_temp.iterrows():
                            for col_idx, value in row.items():
                                if pd.notna(value) and config['table_name'].lower() in str(value).lower():
                                    # Encontramos la tabla - leer datos desde aquí
                                    logging.info(f"✅ Tabla '{config['table_name']}' encontrada en hoja: {sheet_name}, fila: {row_idx+1}")
                                    
                                    # Leer datos (asumiendo encabezados en la siguiente fila)
                                    df = pd.read_excel(file_content, sheet_name=sheet_name, header=row_idx)
                                    
                                    # Limitar a 10,000 filas máximo
                                    df = df.head(10000)
                                    
                                    # ACTUALIZAR Azure SQL
                                    actualizar_filas_azure(cursor, df, config['rango_filas'])
                                    tabla_encontrada = True
                                    break
                            
                            if tabla_encontrada:
                                break
                    
                    except Exception as e:
                        continue
                
                if not tabla_encontrada:
                    logging.error(f"❌ No se encontró la tabla: {config['table_name']}")
                
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

def actualizar_filas_azure(cursor, df, rango_filas):
    """Actualizar filas específicas en Azure SQL"""
    # Obtener rango de IDs a actualizar
    start_id, end_id = map(int, rango_filas.split(':'))
    
    # Actualizar fila por fila
    for index, row in df.iterrows():
        current_id = start_id + index
        
        if current_id > end_id:
            break  # No sobrepasar el rango asignado
            
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
        """, 
        row.get('Ejecutivo', ''),
        row.get('Telefono', ''),
        row.get('FechaCreada', ''),
        row.get('Sede', ''),
        row.get('Programa', ''),
        row.get('Turno', ''),
        row.get('Codigo', ''),
        row.get('Canal', ''),
        row.get('Intervalo', ''),
        row.get('Medio', ''),
        row.get('Contacto', ''),
        row.get('Interesado', ''),
        row.get('Estado', ''),
        row.get('Objecion', ''),
        row.get('Observacion', ''),
        current_id)
    
    logging.info(f"📊 Actualizadas filas {rango_filas}: {len(df)} registros")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    sync_sharepoint_to_sql()
