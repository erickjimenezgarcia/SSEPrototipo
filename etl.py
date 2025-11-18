# etl_incremental.py
import asyncio
import json, os
from datetime import datetime
import mysql.connector
import oracledb as cx_Oracle
from typing import Optional, List, Dict
from dotenv import load_dotenv

load_dotenv()

ORACLE_DSN = os.getenv("ORACLE_DSN")
ORACLE_USER = os.getenv("ORACLE_USER")
ORACLE_PASS = os.getenv("ORACLE_PASS")

HOST_MYSQL = os.getenv("HOST_MYSQL")
USER_MYSQL = os.getenv("USER_MYSQL")
PASS_MYSQL = os.getenv("PASS_MYSQL")
DB_MYSQL = os.getenv("DB_MYSQL")

class ETLIncremental:
    def __init__(self):
        # Configuración MySQL
        self.mysql_config = {
            "host": HOST_MYSQL,
            "user": USER_MYSQL,
            "password": PASS_MYSQL,
            "database": DB_MYSQL
        }
        
        # Configuración Oracle
        self.oracle_config = {
            "user": ORACLE_USER,
            "password": ORACLE_PASS,
            "dsn": ORACLE_DSN
        }
    
    def get_ultimo_id_procesado(self) -> int:
        """Obtiene el último ID procesado desde Oracle"""
        conn = cx_Oracle.connect(**self.oracle_config)
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT ultimo_id_procesado 
                FROM etl_control 
                WHERE proceso = 'sunass_etl'
            """)
            
            result = cursor.fetchone()
            ultimo_id = result[0] if result else 0
            
            return ultimo_id
        finally:
            cursor.close()
            conn.close()
    
    def obtener_datos_nuevos(self, ultimo_id: int) -> List[Dict]:
        """Consulta solo registros nuevos desde MySQL"""
        conn = mysql.connector.connect(**self.mysql_config)
        cursor = conn.cursor(dictionary=True)
        
        # Esta es tu query EXACTA con el filtro de ID incremental
        query = """
            SELECT css.id_numero_servicios , css.fecha, dpto.departamento , prov.provincia , dist.distrito ,
               cs2.descripcion as tipologia , css.lat, css.lng 
            FROM crm_servicios_sunass css 
            INNER JOIN ubdepartamento dpto on css.departamento = dpto.idDepa
            INNER JOIN ubprovincia prov on css.departamento = prov.idDepa and css.provincia = prov.idProv
            INNER JOIN ubdistrito dist on css.departamento = prov.idDepa and css.provincia = dist.idProv and css.distrito = dist.idDist 
            INNER JOIN crm_paleta_subtipo2 cs2 on css.subtipologia_2 = cs2.id
            WHERE css.subtipologia_1 = 25  
            AND css.subtipologia_2 IN (177,173,167,163)
            AND DATE(css.fecha) >= '2025-10-01'
            AND DATE(css.fecha) <= '2025-12-31'
            AND css.id_numero_servicios > %s
            ORDER BY css.id_numero_servicios ASC;
        """
        
        try:
            cursor.execute(query, (ultimo_id,))
            datos = cursor.fetchall()
            
            print(f"Encontrados {len(datos)} registros nuevos desde ID {ultimo_id}")
            return datos
        finally:
            cursor.close()
            conn.close()
    
    def insertar_en_oracle(self, datos: List[Dict]) -> int:
        """Inserta datos nuevos en Oracle"""
        if not datos:
            return 0
        
        conn = cx_Oracle.connect(**self.oracle_config)
        cursor = conn.cursor()
        
        try:
            # Query de inserción
            insert_query = """
                INSERT INTO concateck_datos 
                (id_numero_servicios, fecha, departamento, provincia, distrito, 
                 tipologia, lat, lng)
                VALUES (:1, :2, :3, :4, :5, :6, :7, :8)
            """
            
            # Prepara datos para inserción batch
            batch_data = []
            max_id = 0
            
            for row in datos:
                batch_data.append((
                    row['id_numero_servicios'],
                    row['fecha'],
                    row['departamento'],
                    row['provincia'],
                    row['distrito'],
                    row['tipologia'],
                    float(row['lat']) if row['lat'] else None,
                    float(row['lng']) if row['lng'] else None
                ))
                max_id = max(max_id, row['id_numero_servicios'])
            
            # Inserción batch (más eficiente)
            cursor.executemany(insert_query, batch_data)
            
            # Actualiza control
            cursor.execute("""
                UPDATE etl_control 
                SET ultimo_id_procesado = :1,
                    ultima_ejecucion = SYSTIMESTAMP,
                    registros_procesados = registros_procesados + :2
                WHERE proceso = 'sunass_etl'
            """, (max_id, len(datos)))
            
            conn.commit()
            
            print(f"Insertados {len(datos)} registros en Oracle")
            print(f"Último ID procesado: {max_id}")
            
            return len(datos)
            
        except Exception as e:
            conn.rollback()
            print(f" Error al insertar en Oracle: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    async def ejecutar_etl(self):
        """Ejecuta un ciclo del ETL"""
        try:
            print(f"\n{'='*60}")
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Iniciando ETL...")
            print(f"{'='*60}")
            
            # 1. Obtener último ID procesado
            ultimo_id = self.get_ultimo_id_procesado()
            print(f"Último ID procesado: {ultimo_id}")
            
            # 2. Obtener datos nuevos desde MySQL
            datos_nuevos = self.obtener_datos_nuevos(ultimo_id)
            
            # 3. Insertar en Oracle si hay datos nuevos
            if datos_nuevos:
                registros = self.insertar_en_oracle(datos_nuevos)
                print(f"ETL completado: {registros} registros procesados")
            else:
                print(" No hay datos nuevos para procesar")
            
            print(f"{'='*60}\n")
                
        except Exception as e:
            print(f"Error en ETL: {e}")
            import traceback
            traceback.print_exc()
    
    async def loop_etl(self, intervalo_segundos: int = 300):
        """Loop principal del ETL cada N segundos"""
        print(f"Iniciando ETL incremental cada {intervalo_segundos} segundos (5 minutos)")
        
        while True:
            try:
                await self.ejecutar_etl()
            except Exception as e:
                print(f"Error en ciclo ETL: {e}")
            
            # Espera antes del próximo ciclo
            print(f"Esperando {intervalo_segundos} segundos para próxima ejecución...")
            await asyncio.sleep(intervalo_segundos)

# ===== Integración con FastAPI =====
async def iniciar_etl_background():
    """Función para iniciar ETL en background"""
    etl = ETLIncremental()
    await etl.loop_etl(intervalo_segundos=300)  # 5 minutos
