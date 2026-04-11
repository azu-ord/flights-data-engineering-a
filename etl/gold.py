# etl/gold.py

import argparse
import gc
import pandas as pd
import awswrangler as wr

wr.engine.set("python")
# Configuración del logger
import sys
import logging

# ──────────────────────────────────────────────
# Configuración del logger
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Constantes
# ──────────────────────────────────────────────
DATABASE_NAME = "flights_gold"
TABLE_NAME = "vuelos_analitica"

def reader(BUCKET_NAME: str, table: str) -> pd.DataFrame:
    """Lee el dataframe desde S3 usando PyArrow, cargando solo las columnas necesarias.
    Returns:
        pd.DataFrame: Dataframe leído desde S3.
    """
    s3_path = f"s3://{BUCKET_NAME}/flights/bronze/{table}/"
    logger.info("── READING ──────────────────────────────────")
    df = pd.read_parquet(s3_path)
    logger.info(f"Dataframe leído desde {s3_path} con {len(df)} filas.")
    return df

def build_with_ctas(BUCKET_NAME: str, DATABASE_NAME: str, TABLE_NAME: str) -> None:
    """Construye la tabla Gold usando una consulta CTAS en Athena.
    Args:
        BUCKET_NAME (str): Nombre del bucket S3 donde se encuentran los datos.
        DATABASE_NAME (str): Nombre de la base de datos en Athena.
        TABLE_NAME (str): Nombre de la tabla a crear.
    """
    logger.info("── BUILDING GOLD TABLE WITH CTAS ──────────────────────────────────")
    query_ctas = f"""
    CREATE TABLE flights_gold.vuelos_analitica AS (
        SELECT
            f.year,
            f.month,
            f.day,
            f.origin_airport,
            ap_orig.airport     AS origin_airport_name,
            ap_orig.city        AS origin_city,
            ap_orig.state       AS origin_state,
            f.destination_airport,
            ap_dest.airport     AS destination_airport_name,
            al.airline          AS airline_name,
            f.departure_delay,
            f.arrival_delay,
            f.cancelled,
            f.cancellation_reason,
            f.distance,
            f.air_system_delay,
            f.airline_delay,
            f.weather_delay,
            f.late_aircraft_delay,
            f.security_delay
        FROM flights_bronze.flights f
        LEFT JOIN flights_bronze.airlines al
            ON f.airline = al.iata_code
        LEFT JOIN flights_bronze.airports ap_orig
            ON f.origin_airport = ap_orig.iata_code
        LEFT JOIN flights_bronze.airports ap_dest
            ON f.destination_airport = ap_dest.iata_code
    )
    """
    wr.catalog.delete_table_if_exists(database=DATABASE_NAME, table=TABLE_NAME)
    wr.catalog.create_database(name=DATABASE_NAME, exist_ok=True)
    wr.athena.read_sql_query(
        query_ctas,
        database      = DATABASE_NAME,
        ctas_approach = False,  # No usar CTAS para evitar problemas de permisos y costos innecesarios
    )
    logger.info("✓ Gold table 'vuelos_analitica' creada con éxito.")


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(description="ETL Silver Layer para datos de vuelos.")
    parser.add_argument("--bucket", required=True, help="Nombre del bucket S3 donde se guardarán los datos transformados.")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    try:

        bucket_name = args.bucket
        build_with_ctas(BUCKET_NAME=bucket_name)
    except Exception as e:
        logger.error(f"Error durante la creación de la tabla Gold: {e}")
        sys.exit(1)

print("✓ Gold/ventas creada")
