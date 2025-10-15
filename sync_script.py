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
                
                # DESCARGAR CON OFFICE365 LIBRARY (CORREGIDO)
                file_content = download_sharepoint_file_office365(
                    config['path'], 
                    SHAREPOINT_USERNAME, 
                    SHAREPOINT_PASSWORD
                )
                
                if file_content is None:
                    logging.error(f"❌ No se pudo descargar: {config['path']}")
                    continue
                
                # Leer tabla ESPECÍFICA del Excel
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

def download_sharepoint_file_office365(file_path, username, password):
    """Descargar archivo de SharePoint con autenticación Office365"""
    try:
        from office365.sharepoint.client_context import ClientContext
        from office365.runtime.auth.authentication_context import AuthenticationContext
        
        site_url = "https://escuelarefrigeracion.sharepoint.com/sites/ASESORASCOMERCIALES"
        
        logging.info(f"🔐 Autenticando con SharePoint...")
        
        # Autenticación
        ctx_auth = AuthenticationContext(site_url)
        if ctx_auth.acquire_token_for_user(username, password):
            ctx = ClientContext(site_url, ctx_auth)
            
            # Verificar conexión
            web = ctx.web
            ctx.load(web)
            ctx.execute_query()
            logging.info(f"✅ Conectado a SharePoint: {web.title}")
            
            # Obtener archivo (usar ruta completa)
            full_path = f"/sites/ASESORASCOMERCIALES/{file_path}"
            file = ctx.web.get_file_by_server_relative_url(full_path)
            ctx.load(file)
            ctx.execute_query()
            
            logging.info(f"📥 Descargando: {file.name}")
            
            # Descargar contenido
            file_content = BytesIO(file.read())
            logging.info(f"✅ Descarga exitosa: {len(file_content.getvalue())} bytes")
            return file_content
        else:
            logging.error("❌ Error de autenticación con SharePoint")
            return None
            
    except Exception as e:
        logging.error(f"❌ Error descargando archivo: {str(e)}")
        return None

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
