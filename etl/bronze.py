"""ETL Bronze Layer - Flights Dataset
    # Descripción:
    Este script realiza el proceso ETL para la capa Bronze del dataset de vuelos.
- Extract: Lee los archivos CSV crudos (airlines.csv, airports.csv, flights.csv) desde un directorio local.
- Validate: Verifica que las tablas pequeñas (airlines, airports) no estén vacías y que las columnas clave no tengan nulos. Para flights, la validación se hace chunk a chunk en load_large_table().
- Load: Crea la base de datos 'flights_bronze' en Glue Catalog y escribe las tablas pequeñas a
    S3 como Parquet. Para la tabla grande (flights), lee en chunks con dtype explícito y los sube a S3 como Parquet, registrándolos en Glue Catalog.
# Uso:
1. Asegúrate de tener AWS CLI configurado y las librerías necesarias instaladas (pandas, awswrangler).
2. Ejecuta el script con los argumentos necesarios:
   python etl/bronze.py --bucket <tu-bucket> --data-dir <ruta-al-directorio-de-csvs>
   python etl/bronze.py --bucket <tu-bucket> --data-dir data/
# Notas:
- El script asume que los archivos CSV están en el formato esperado y que el bucket S3 existe y es accesible.
- La tabla 'flights' se procesa en chunks para manejar su gran tamaño sin consumir demasiada memoria.
    - Se definen tipos de datos explícitos para optimizar el uso de memoria y la compatibilidad con Athena.
"""

import argparse
import logging
import sys
import os

import pandas as pd
import awswrangler as wr

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
DATABASE_NAME = "flights_bronze"
SMALL_TABLES  = ["airlines", "airports"]  
LARGE_TABLES  = ["flights"]                # se procesa en chunks
CHUNK_SIZE    = 500_000                   

# Tipos explícitos para flights
# Se usa "str" en lugar de "category" para columnas de texto por compatibilidad con Athena y parquet
FLIGHTS_DTYPES: dict[str, str] = {
    "YEAR":                "int16",
    "MONTH":               "int8",
    "DAY":                 "int8",
    "DAY_OF_WEEK":         "int8",
    "AIRLINE":             "str",       # clave para JOIN con airlines en Gold
    "FLIGHT_NUMBER":       "int16",
    "TAIL_NUMBER":         "str",
    "ORIGIN_AIRPORT":      "str",       # clave para JOIN con airports en Gold
    "DESTINATION_AIRPORT": "str",       # clave para JOIN con airports en Gold
    "SCHEDULED_DEPARTURE": "int16",
    "DEPARTURE_TIME":      "float32",
    "DEPARTURE_DELAY":     "float32",   # usado en Silver
    "TAXI_OUT":            "float32",
    "WHEELS_OFF":          "float32",
    "SCHEDULED_TIME":      "float32",
    "ELAPSED_TIME":        "float32",
    "AIR_TIME":            "float32",
    "DISTANCE":            "float32",   # usado en Gold
    "WHEELS_ON":           "float32",
    "TAXI_IN":             "float32",
    "SCHEDULED_ARRIVAL":   "int16",
    "ARRIVAL_TIME":        "float32",
    "ARRIVAL_DELAY":       "float32",   # usado en Silver
    "DIVERTED":            "int8",
    "CANCELLED":           "int8",      # usado en Silver
    "CANCELLATION_REASON": "str",       # usado en Gold
    "AIR_SYSTEM_DELAY":    "float32",   # usado en Gold
    "SECURITY_DELAY":      "float32",   # usado en Gold
    "AIRLINE_DELAY":       "float32",   # usado en Gold
    "LATE_AIRCRAFT_DELAY": "float32",   # usado en Gold
    "WEATHER_DELAY":       "float32",   # usado en Silver
}

# Columnas clave que no deben tener nulos
KEY_COLUMNS: dict[str, list[str]] = {
    "airlines": ["IATA_CODE", "AIRLINE"],
    "airports": ["IATA_CODE"],
    "flights":  ["YEAR", "MONTH", "DAY", "AIRLINE",
                 "ORIGIN_AIRPORT", "DESTINATION_AIRPORT"],
}


# ──────────────────────────────────────────────
# Extract  (tablas pequeñas)
# ──────────────────────────────────────────────
def extract(data_dir: str) -> dict[str, pd.DataFrame]:
    """
    Lee airlines y airports completos en memoria.
    Verifica que flights.csv exista se lee en chunks en load_large_table().
    Devuelve un dict {nombre_tabla: DataFrame} con las tablas pequeñas.
    """
    logger.info("── EXTRACT ──────────────────────────────────")
    dataframes: dict[str, pd.DataFrame] = {}

    for table in SMALL_TABLES + LARGE_TABLES:
        file_path = os.path.join(data_dir, f"{table}.csv")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Archivo no encontrado: {file_path}")

        if table in LARGE_TABLES:
            logger.info(f"  → {table}: archivo encontrado "
                        f"(se procesará en chunks de {CHUNK_SIZE:,} filas)")
            continue

        logger.info(f"Leyendo {file_path}...")
        df = pd.read_csv(file_path)
        ram_mb = df.memory_usage(deep=True).sum() / 1e6
        logger.info(f"  → {table}: {len(df):,} filas | "
                    f"{len(df.columns)} columnas | {ram_mb:.1f} MB en RAM")
        dataframes[table] = df

    return dataframes


# ──────────────────────────────────────────────
# Validate  (tablas pequeñas)
# ──────────────────────────────────────────────
def validate(dataframes: dict[str, pd.DataFrame]) -> None:
    """
    Valida requerimientos mínimos de airlines y airports antes de escribir a S3.
    Para flights, la validación ocurre chunk a chunk en load_large_table().
    """
    logger.info("── VALIDATE ─────────────────────────────────")

    for table, df in dataframes.items():
        assert len(df) > 0, f"'{table}' está vacío."

        for col in KEY_COLUMNS.get(table, []):
            if col in df.columns:
                null_count = df[col].isna().sum()
                assert null_count == 0, (
                    f"'{table}'.'{col}' tiene {null_count:,} nulos inesperados."
                )

        logger.info(f"  ✓ {table}: validaciones OK ({len(df):,} filas)")


# ──────────────────────────────────────────────
# Load  (tablas pequeñas)
# ──────────────────────────────────────────────
def load(bucket: str, dataframes: dict[str, pd.DataFrame]) -> None:
    """
    Crea la base de datos flights_bronze en Glue y escribe
    airlines y airports a S3 como Parquet.
    """
    logger.info("── LOAD (tablas pequeñas) ────────────────────")

    logger.info(f"Asegurando base de datos '{DATABASE_NAME}' en Glue Catalog...")
    wr.catalog.create_database(name=DATABASE_NAME, exist_ok=True)

    for table, df in dataframes.items():
        s3_path = f"s3://{bucket}/flights/bronze/{table}/"
        logger.info(f"Escribiendo '{table}' → {s3_path}")

        wr.s3.to_parquet(
            df=df,
            path=s3_path,
            dataset=True,
            database=DATABASE_NAME,
            table=table,
            mode="overwrite",
        )
        logger.info(f"  ✓ {table}: {len(df):,} filas → {s3_path}")


# ──────────────────────────────────────────────
# Load  (tabla grande en chunks)
# ──────────────────────────────────────────────
def load_large_table(bucket: str, table: str, file_path: str) -> None:
    """
    Lee flights.csv en chunks con dtype explícito
    y los sube a S3 como Parquet, los registra en Glue Catalog.
    """
    s3_path  = f"s3://{bucket}/flights/bronze/{table}/"
    key_cols = KEY_COLUMNS.get(table, [])
    total_rows = 0

    # limpieza antes de loop
    logger.info(f"Limpiando datos previos de '{table}' en S3 y Glue Catalog...")
    wr.s3.delete_objects(s3_path)
    wr.catalog.delete_table_if_exists(database=DATABASE_NAME, table=table)

    logger.info(f"── LOAD (chunks) — '{table}' → {s3_path}")

    for i, chunk in enumerate(
        pd.read_csv(file_path, chunksize=CHUNK_SIZE, dtype=FLIGHTS_DTYPES)
    ):
        # Validación de nulos en columnas por chunk
        assert len(chunk) > 0, f"Chunk {i} de '{table}' está vacío."
        for col in key_cols:
            if col in chunk.columns:
                null_count = chunk[col].isna().sum()
                assert null_count == 0, (
                    f"'{table}'.'{col}' tiene {null_count:,} nulos en chunk {i}."
                )

        wr.s3.to_parquet(
            df=chunk,
            path=s3_path,
            dataset=True,
            database=DATABASE_NAME,
            table=table,
            mode="append",
        )

        total_rows += len(chunk)
        logger.info(f"  chunk {i + 1}: {len(chunk):,} filas "
                    f"(acumulado: {total_rows:,})")

    logger.info(f"  ✓ {table}: {total_rows:,} filas totales → {s3_path}")


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="ETL Bronze Layer - Flights Dataset"
    )
    parser.add_argument(
        "--bucket",
        required=True,
        help="Nombre del bucket S3 destino",
    )
    parser.add_argument(
        "--data-dir",
        required=True,
        help="Ruta al directorio con los CSVs crudos",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    try:
        # Leer tablas pequeñas y verificar existencia de todos los archivos
        dataframes = extract(data_dir=args.data_dir)

        # Validar columnas clave de tablas pequeñas
        validate(dataframes=dataframes)

        # Crear BD Glue y subir airlines y airports
        load(bucket=args.bucket, dataframes=dataframes)

        # Subir flights en chunks 
        for table in LARGE_TABLES:
            load_large_table(
                bucket=args.bucket,
                table=table,
                file_path=os.path.join(args.data_dir, f"{table}.csv"),
            )

        logger.info("── Pipeline Bronze completado exitosamente. ──")

    except Exception:
        logger.exception("Error crítico durante el pipeline Bronze.")
        sys.exit(1)
