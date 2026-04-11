# etl/silver.py

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
DATABASE_NAME = "flights_silver"
TABLE_NAME_DAILY = "flights_daily"
TABLE_NAME_MONTHLY = "flights_monthly"
TABLE_NAME_FLIGHTS_BY_AIRPORT = "flights_by_airport"


# ──────────────────────────────────────────────
# Data Engineering: Silver Layer
# ──────────────────────────────────────────────

REQUIRED_COLUMNS = [
    "year", "month", "day", "flight_number", "airline",
    "origin_airport", "cancelled", "departure_delay",
    "arrival_delay", "weather_delay"
]

def reader(BUCKET_NAME: str, table: str) -> pd.DataFrame:
    """Lee el dataframe desde S3 usando PyArrow, cargando solo las columnas necesarias.
    Returns:
        pd.DataFrame: Dataframe leído desde S3.
    """
    s3_path = f"s3://{BUCKET_NAME}/flights/bronze/{table}/"
    logger.info("── READING ──────────────────────────────────")
    df = pd.read_parquet(s3_path, columns=REQUIRED_COLUMNS)
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
    try:
        not_cancelled = df['cancelled'] == 0

        # agrupados por año, mes y día — delays promediados solo sobre vuelos no cancelados
        df_daily = (
            df.groupby(["year", "month", "day"])
            .agg(
                total_flights=("flight_number", "count"),
                total_delayed=("departure_delay", lambda x: (x > 0).sum()),
                total_cancelled=("cancelled", "sum"),
                avg_departure_delay=("departure_delay", lambda x: x.where(not_cancelled.loc[x.index]).mean()),
                avg_arrival_delay=("arrival_delay", lambda x: x.where(not_cancelled.loc[x.index]).mean()),
            )
            .reset_index()
        )

        logger.info("Transformación completada: agrupado por year, month y day (delays excluyen cancelados).")
        return df_daily
    except Exception as e:
        logger.error(f"Error en daily_transform: {e}")
        raise

def monthly_transform(df: pd.DataFrame) -> pd.DataFrame:
    """Transforma el dataframe para obtener métricas mensuales de vuelos.
    Args:
        df (pd.DataFrame): Dataframe de vuelos en bruto.
    Returns:
        pd.DataFrame: Dataframe transformado con métricas mensuales.
    """
    logger.info("── MONTHLY TRANSFORM ──────────────────────────────────")
    
    try:
        df_monthly = (
            df.groupby(["month", "airline"])
            .agg(
                total_flights=("flight_number", "count"),
                total_delayed=("departure_delay", lambda x: (x > 0).sum()),
                total_cancelled=("cancelled", "sum"),
                avg_arrival_delay=("arrival_delay", "mean"),
                on_time_pct=("arrival_delay", lambda x: (x.dropna() <= 15).mean() * 100) # porcentaje de vuelos con arrival_delay <= 15
            )
            .reset_index()
        )
    

        logger.info("Transformación completada: agrupado por month y airline.")
        return df_monthly
    except Exception as e:
        logger.error(f"Error en monthly_transform: {e}")
        raise

def transform_flights_by_airport(df: pd.DataFrame) -> pd.DataFrame:
    """ Transforma el dataframe para obtener métricas por aeropuerto de origen.
    Args:
        df (pd.DataFrame): Dataframe de vuelos en bruto.
    Returns:
        pd.DataFrame: Dataframe transformado con métricas por aeropuerto de origen.
    """
    logger.info("── FLIGHTS BY AIRPORT ──────────────────────────────────")
    df_flights_by_airport = (
        df.groupby(["origin_airport"])
        .agg(
            total_departures=("flight_number", "count"),
            total_delayed=("departure_delay", lambda x: (x > 0).sum()),
            total_cancelled=("cancelled", "sum"),
            avg_departure_delay=("departure_delay", "mean"),
            #  porcentaje del total de minutos de retraso atribuidos a clima
            weather_delay_pct=("weather_delay", lambda x: x.sum() / df["weather_delay"].sum() * 100 if df["weather_delay"].sum() > 0 else 0)
        )
        .reset_index()
    )
    logger.info("Transformación completada: agrupado por origin_airport.")
    return df_flights_by_airport

def createCatalogTable(df: pd.DataFrame,BUCKET_NAME: str, TABLE_NAME: str, DATABASE_NAME: str, partition_cols: list[str] = ["month"]):

    logger.info("── CREATING CATALOG TABLE ──────────────────────────────────")
    wr.catalog.create_parquet_table(
        database=DATABASE_NAME,
        table=TABLE_NAME,
        path=f"s3://{BUCKET_NAME}/flights/silver/{TABLE_NAME}/",
        columns_types={col: str(dtype) for col, dtype in df.dtypes.items()},
        partition_cols=partition_cols,
    )
    logger.info(f"Tabla '{TABLE_NAME}' creada en Glue Catalog bajo la base de datos '{DATABASE_NAME}'.")

def writer(df: pd.DataFrame, BUCKET_NAME: str , DATABASE_NAME: str, TABLE_NAME: str, partition_cols: list[str] = ["month"]):
    """ Escribe el dataframe transformado en S3 en formato Parquet"""
    logger.info("── WRITING ──────────────────────────────────")
    output_path = f"s3://{BUCKET_NAME}/flights/silver/{TABLE_NAME}/"
    wr.s3.to_parquet(
        df=df,
        path=output_path,
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

        wr.catalog.create_database(name=DATABASE_NAME, exist_ok=True)
        logger.info(f"Base de datos '{DATABASE_NAME}' lista en Glue Catalog.")

        df_bronze = reader(BUCKET_NAME=bucket_name, table="flights")

        df_daily = daily_transform(df_bronze)
        assert not df_daily.empty, "df_daily está vacío"
        assert df_daily[["year", "month", "day"]].notna().all().all(), "df_daily tiene nulos en columnas clave (year, month, day)"
        assert (df_daily["total_flights"] > 0).all(), "df_daily tiene filas con total_flights <= 0"
        assert (df_daily["total_cancelled"] >= 0).all(), "df_daily tiene valores negativos en total_cancelled"
        #createCatalogTable(df_daily, BUCKET_NAME=bucket_name, TABLE_NAME=TABLE_NAME_DAILY, DATABASE_NAME=DATABASE_NAME)
        writer(df_daily, BUCKET_NAME=bucket_name, DATABASE_NAME=DATABASE_NAME, TABLE_NAME=TABLE_NAME_DAILY, partition_cols=["month"])
        del df_daily; gc.collect()

        df_monthly = monthly_transform(df_bronze)
        assert not df_monthly.empty, "df_monthly está vacío"
        assert df_monthly[["month", "airline"]].notna().all().all(), "df_monthly tiene nulos en columnas clave (month, airline)"
        assert (df_monthly["total_flights"] > 0).all(), "df_monthly tiene filas con total_flights <= 0"
        assert df_monthly["on_time_pct"].between(0, 100).all(), "df_monthly tiene on_time_pct fuera de rango [0, 100]"
        #createCatalogTable(df_monthly, BUCKET_NAME=bucket_name, TABLE_NAME=TABLE_NAME_MONTHLY, DATABASE_NAME=DATABASE_NAME, partition_cols=["month"])
        writer(df_monthly, BUCKET_NAME=bucket_name, DATABASE_NAME=DATABASE_NAME, TABLE_NAME=TABLE_NAME_MONTHLY, partition_cols=[])
        del df_monthly; gc.collect()

        df_flights_by_airport = transform_flights_by_airport(df_bronze)
        assert not df_flights_by_airport.empty, "df_flights_by_airport está vacío"
        assert df_flights_by_airport["origin_airport"].notna().all(), "df_flights_by_airport tiene nulos en origin_airport"
        assert (df_flights_by_airport["total_departures"] > 0).all(), "df_flights_by_airport tiene filas con total_departures <= 0"
        assert df_flights_by_airport["weather_delay_pct"].between(0, 100).all(), "df_flights_by_airport tiene weather_delay_pct fuera de rango [0, 100]"
        #createCatalogTable(df_flights_by_airport, BUCKET_NAME=bucket_name, TABLE_NAME=TABLE_NAME_FLIGHTS_BY_AIRPORT, DATABASE_NAME=DATABASE_NAME, partition_cols=["month"])
        writer(df_flights_by_airport, BUCKET_NAME=bucket_name, DATABASE_NAME=DATABASE_NAME, TABLE_NAME=TABLE_NAME_FLIGHTS_BY_AIRPORT, partition_cols=[])
        del df_flights_by_airport; gc.collect()

        logger.info("ETL Silver completado exitosamente.")

    except Exception as e:
        logger.error(f"Error occurred: {e}")
        sys.exit(1)
