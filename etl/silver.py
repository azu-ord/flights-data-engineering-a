# etl/silver.py

import pandas as pd
import awswrangler as wr
# Configuración del logger
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

INPUT_PATH = "s3://flights-data-engineering-a/silver/flights/"

# lee un datafram que se guardo en s s3 con wr para agruparlo por año y mes, y luego lo guarda en otro path de s3
def reader():
    df = wr.s3.read_parquet(INPUT_PATH)
    logger.info(f"Dataframe leído desde {INPUT_PATH} con {len(df)} filas.")
    return df

def transform(df: pd.DataFrame) -> pd.DataFrame:
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
    df_gold = df_silver.join(delays_non_cancelled, how="left").reset_index()
    
    logger.info("Transformación completada: agrupado por YEAR, MONTH y DAY (delays excluyen cancelados).")
    return df_gold

# Particiona por MONTH. Usa partition_cols=["MONTH"] y mode="overwrite_partitions". Escribe a s3://<tu-bucket>/flights/silver/flights_daily/.
def writer(df: pd.DataFrame):
    output_path = f"s3://{BUCKET_NAME}/flights/silver/{TABLE_NAME}/"
    wr.s3.to_parquet(
        df=df, 
        output_path=output_path, 
        dataset=True,
        database=TABLE_NAME,
        index=False,
        partition_cols=["MONTH"],
        mode="overwrite"
        )
    logger.info(f"Dataframe transformado guardado en {output_path}.")



if __name__ == "__main__":
    df_bronze = reader()
    df_silver = transform(df_bronze)
    writer(df_silver)
