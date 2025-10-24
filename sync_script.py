import pandas as pd
import os
import logging
import time
from supabase import create_client, Client

def sync_excel_to_supabase():
    logging.info("🚀 Sincronizando Excel -> Supabase (MODO MASIVO)")
    
    # Configuración Supabase
    SUPABASE_URL = os.environ['SUPABASE_URL']
    SUPABASE_KEY = os.environ['SUPABASE_KEY']
    EXCEL_PATH = "base_combinada.xlsx"
    
    try:
        # 1. CONECTAR A SUPABASE
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        logging.info("✅ Conectado a Supabase")
        
        # 2. LEER EXCEL
        logging.info(f"📖 Leyendo Excel: {EXCEL_PATH}")
        df = pd.read_excel(EXCEL_PATH, sheet_name=0)
        
        if df.empty:
            logging.error("❌ Excel está vacío")
            return
            
        logging.info(f"✅ Excel leído: {len(df)} filas, {len(df.columns)} columnas")
        
        # 3. PREPARAR DATOS PARA SUPABASE
        datos_supabase = preparar_datos_supabase(df)
        
        # 4. SINCRONIZAR CON SUPABASE
        if datos_supabase:
            registros_procesados = sync_supabase_data(supabase, datos_supabase)
            logging.info(f"🎉 SINCRONIZACIÓN COMPLETADA: {registros_procesados} registros")
        else:
            logging.error("❌ No hay datos para sincronizar")
        
    except Exception as e:
        logging.error(f"💥 Error: {str(e)}")
        raise

def preparar_datos_supabase(df):
    """Convertir DataFrame de Excel a formato Supabase"""
    df_clean = df.dropna(how='all').dropna(axis=1, how='all')
    df_clean.columns = [str(col).strip() for col in df_clean.columns]
    
    datos = []
    
    for index, row in df_clean.iterrows():
        try:
            dato = {
                'ejecutivo': str(row.get('Ejecutivo', ''))[:100],
                'telefono': str(row.get('Telefono', ''))[:50],
                'fecha_creada': parse_fecha_supabase(row.get('FechaCreada')),
                'sede': str(row.get('Sede', ''))[:100],
                'programa': str(row.get('Programa', ''))[:100],
                'turno': str(row.get('Turno', ''))[:50],
                'codigo': str(row.get('Codigo', ''))[:50],
                'canal': str(row.get('Canal', ''))[:100],
                'intervalo': str(row.get('Intervalo', ''))[:50],
                'medio': str(row.get('Medio', ''))[:100],
                'contacto': str(row.get('Contacto', ''))[:100],
                'interesado': str(row.get('Interesado', ''))[:100],
                'estado': str(row.get('Estado', ''))[:100],
                'objecion': str(row.get('Objecion', ''))[:500],
                'observacion': str(row.get('Observacion', ''))[:1000]
            }
            datos.append(dato)
        except Exception as e:
            logging.warning(f"⚠️ Error fila {index}: {e}")
            continue
    
    logging.info(f"📦 Datos preparados para Supabase: {len(datos)} registros")
    return datos

def parse_fecha_supabase(valor):
    """Convertir fecha para Supabase"""
    if pd.isna(valor):
        return None
    try:
        if isinstance(valor, str):
            fecha = pd.to_datetime(valor, errors='coerce')
            return fecha.isoformat() if pd.notna(fecha) else None
        else:
            return valor.isoformat()
    except:
        return None

def sync_supabase_data(supabase, datos):
    """Sincronizar datos con Supabase - INTENTA TODO DE UNA VEZ"""
    start_time = time.time()
    
    try:
        # PRIMER INTENTO: Insertar TODO de una vez
        logging.info("🗑️ Eliminando datos anteriores...")
        delete_response = supabase.table('vendedoras_data').delete().neq('id', 0).execute()
        
        logging.info(f"⚡ Insertando {len(datos)} registros de UNA VEZ...")
        insert_response = supabase.table('vendedoras_data').insert(datos).execute()
        
        if hasattr(insert_response, 'data') and insert_response.data:
            registros_insertados = len(insert_response.data)
            end_time = time.time()
            logging.info(f"🎉 INSERCIÓN MASIVA EXITOSA: {registros_insertados} registros en {end_time - start_time:.2f}s")
            return registros_insertados
        else:
            raise Exception("Inserción masiva falló")
            
    except Exception as e:
        # SEGUNDO INTENTO: Por lotes (si falla la masiva)
        logging.warning(f"⚠️ Inserción masiva falló: {str(e)}")
        logging.info("🔄 Intentando inserción por lotes...")
        return sync_supabase_data_batch(supabase, datos)

def sync_supabase_data_batch(supabase, datos):
    """Inserción por lotes de 1000 (backup)"""
    start_time = time.time()
    
    try:
        # Borrar todo
        delete_response = supabase.table('vendedoras_data').delete().neq('id', 0).execute()
        
        # Insertar por lotes
        registros_insertados = 0
        batch_size = 1000
        
        for i in range(0, len(datos), batch_size):
            batch = datos[i:i + batch_size]
            insert_response = supabase.table('vendedoras_data').insert(batch).execute()
            
            if hasattr(insert_response, 'data') and insert_response.data:
                registros_insertados += len(insert_response.data)
                logging.info(f"📦 Lote {i//batch_size + 1}: {len(batch)} registros")
            else:
                logging.error(f"❌ Error en lote {i//batch_size + 1}")
        
        end_time = time.time()
        logging.info(f"⏱️ Tiempo por lotes: {end_time - start_time:.2f} segundos")
        
        return registros_insertados
        
    except Exception as e:
        logging.error(f"💥 Error en inserción por lotes: {str(e)}")
        return 0

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    sync_excel_to_supabase()
