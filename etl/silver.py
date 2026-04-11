# etl/silver.py

import argparse
import pandas as pd
import awswrangler as wr
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
DATABASE_NAME = "flights_silver"
TABLE_NAME_DAILY = "flights_daily"
TABLE_NAME_MONTHLY = "flights_monthly"
TABLE_NAME_FLIGHTS_BY_AIRPORT = "flights_by_airport"


# ──────────────────────────────────────────────
# Data Engineering: Silver Layer
# ──────────────────────────────────────────────

def reader(BUCKET_NAME: str, table: str) -> pd.DataFrame:
    """Lee el dataframe desde S3 usando awswrangler. flights_bronze.flights
    Returns:
        pd.DataFrame: Dataframe leído desde S3.
    """
    s3_path = f"s3://{BUCKET_NAME}/flights/bronze/{table}/"
    logger.info("── READING ──────────────────────────────────")
    df = wr.s3.read_parquet(s3_path)
    logger.info(f"Dataframe leído desde {s3_path} con {len(df)} filas.")
    return df

def daily_transform(df: pd.DataFrame) -> pd.DataFrame:
    """Transforma el dataframe para obtener métricas diarias de vuelos.
    Args:
        df (pd.DataFrame): Dataframe de vuelos en bruto.
    Returns:
        pd.DataFrame: Dataframe transformado con métricas diarias.
    """
    logger.info("── DAILY TRANSFORM ──────────────────────────────────")
    # vuelos totales
    df_all = df
    # vuelos no cancelado
    df_non_cancelled = df[df['CANCELLED'] == 0]
    
    # agrupados por año, mes y día para contar vuelos totales, retrasados y cancelados
    df_silver = (
        df_all.groupby(["YEAR", "MONTH", "DAY"])
        .agg(
            total_flights=("FLIGHT_NUMBER", "count"),
            total_delayed=("DEPARTURE_DELAY", lambda x: (x > 0).sum()),
            total_cancelled=("CANCELLED", "sum")
        )
    )
    
    # Agrupar solo no cancelados para promedios de delays
    delays_non_cancelled = (
        df_non_cancelled.groupby(["YEAR", "MONTH", "DAY"])
        .agg(
            avg_departure_delay=("DEPARTURE_DELAY", "mean"),
            avg_arrival_delay=("ARRIVAL_DELAY", "mean")
        )
    )
    
    # Combinar ambas agregaciones con un self join
    df_daily = df_silver.join(delays_non_cancelled, how="left").reset_index()
    
    logger.info("Transformación completada: agrupado por YEAR, MONTH y DAY (delays excluyen cancelados).")
    return df_daily

def monthly_transform(df: pd.DataFrame) -> pd.DataFrame:
    """Transforma el dataframe para obtener métricas mensuales de vuelos.
    Args:
        df (pd.DataFrame): Dataframe de vuelos en bruto.
    Returns:
        pd.DataFrame: Dataframe transformado con métricas mensuales.
    """
    logger.info("── MONTHLY TRANSFORM ──────────────────────────────────")
    df_monthly = (
        df.groupby(["MONTH","AIRLINE"])
        .agg(
            total_flights=("FLIGHT_NUMBER", "count"),
            total_delayed=("DEPARTURE_DELAY", lambda x: (x > 0).sum()),
            total_cancelled=("CANCELLED", "sum"),
            avg_arrival_delay=("ARRIVAL_DELAY", "mean"),
            on_time_pct = ("ARRIVAL_DELAY", lambda x: (x <= 15).mean() * 100) # porcentaje de vuelos con ARRIVAL_DELAY <= 15
        )
        .reset_index()
    )
    
    logger.info("Transformación completada: agrupado por MONTH y AIRLINE.")
    return df_monthly

def transformFlightsByAirport(df: pd.DataFrame) -> pd.DataFrame:
    """ Transforma el dataframe para obtener métricas por aeropuerto de origen.
    Args:
        df (pd.DataFrame): Dataframe de vuelos en bruto.
    Returns:
        pd.DataFrame: Dataframe transformado con métricas por aeropuerto de origen.
    """
    logger.info("── FLIGHTS BY AIRPORT ──────────────────────────────────")
    df_flights_by_airport = (
        df.groupby(["ORIGIN_AIRPORT"])
        .agg(
            total_departures=("FLIGHT_NUMBER", "count"),
            total_delayed=("DEPARTURE_DELAY", lambda x: (x > 0).sum()),
            total_cancelled=("CANCELLED", "sum"),
            avg_departure_delay=("DEPARTURE_DELAY", "mean"),
            #  porcentaje del total de minutos de retraso atribuidos a clima
            weather_delay_pct=("WEATHER_DELAY", lambda x: x.sum() / df["WEATHER_DELAY"].sum() * 100 if df["WEATHER_DELAY"].sum() > 0 else 0)
        )
        .reset_index()
    )
    logger.info("Transformación completada: agrupado por ORIGIN_AIRPORT y MONTH.")
    return df_flights_by_airport

def createCatalogTable(df: pd.DataFrame, TABLE_NAME: str, DATABASE_NAME: str, partition_cols: list[str] = ["MONTH"]):

    logger.info("── CREATING CATALOG TABLE ──────────────────────────────────")
    wr.catalog.create_parquet_table(
        database=DATABASE_NAME,
        table=TABLE_NAME,
        path=f"s3://{BUCKET_NAME}/flights/silver/{TABLE_NAME}/",
        columns_types={col: str(dtype) for col, dtype in df.dtypes.items()},
        partition_cols=partition_cols,
    )
    logger.info(f"Tabla '{TABLE_NAME}' creada en Glue Catalog bajo la base de datos '{DATABASE_NAME}'.")

def writer(df: pd.DataFrame, BUCKET_NAME: str , DATABASE_NAME: str, TABLE_NAME: str, partition_cols: list[str] = ["MONTH"]):
    """ Escribe el dataframe transformado en S3 en formato Parquet"""
    logger.info("── WRITING ──────────────────────────────────")
    output_path = f"s3://{BUCKET_NAME}/flights/silver/{TABLE_NAME}/"
    wr.s3.to_parquet(
        df=df, 
        output_path=output_path, 
        dataset=True,
        database=DATABASE_NAME,
        table=TABLE_NAME,
        index=False,
        partition_cols=partition_cols,
        mode="overwrite"
        )
    logger.info(f"Dataframe transformado guardado en {output_path}.")

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

        df_bronze = reader(BUCKET_NAME=bucket_name, table="flights_bronze")

        df_daily = daily_transform(df_bronze)
        createCatalogTable(df_daily, TABLE_NAME=TABLE_NAME_DAILY, DATABASE_NAME=DATABASE_NAME)
        writer(df_daily, BUCKET_NAME=bucket_name, DATABASE_NAME=DATABASE_NAME, TABLE_NAME=TABLE_NAME_DAILY, partition_cols=["MONTH"])

        df_monthly = monthly_transform(df_bronze)
        createCatalogTable(df_monthly, TABLE_NAME=TABLE_NAME_MONTHLY, DATABASE_NAME=DATABASE_NAME, partition_cols=["MONTH"])
        writer(df_monthly, BUCKET_NAME=bucket_name, DATABASE_NAME=DATABASE_NAME, TABLE_NAME=TABLE_NAME_MONTHLY)

        df_flights_by_airport = transformFlightsByAirport(df_bronze)
        createCatalogTable(df_flights_by_airport, TABLE_NAME=TABLE_NAME_FLIGHTS_BY_AIRPORT, DATABASE_NAME=DATABASE_NAME, partition_cols=["MONTH"])
        writer(df_flights_by_airport, BUCKET_NAME=bucket_name, DATABASE_NAME=DATABASE_NAME, TABLE_NAME=TABLE_NAME_FLIGHTS_BY_AIRPORT)

        logger.info("ETL Silver completado exitosamente.")

    except Exception as e:
        logger.error(f"Error occurred: {e}")
        sys.exit(1)
