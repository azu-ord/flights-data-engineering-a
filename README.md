# flights-data-engineering-a
Repositorio para el desarrollo de análisis de información de vuelos
## Estructura del Proyecto

```
etl/
  ├── bronze.py    # Capa Bronze - Extrae e ingesta datos crudos
  ├── silver.py    # Capa Silver - Limpieza y transformación
  └── gold.py      # Capa Gold - Análisis y agregaciones
src/
  └── utils/       # Utilidades compartidas
ingestion.sh       # Script para descargar datos desde S3
etl.sh             # Script para ejecutar el pipeline ETL
config.sh          # Configuración centralizada de parámetros
```

## Configuración

Edita `config.sh` para configurar los parámetros:
```bash
export S3_BUCKET="itam-analytics-dante/flights-hwk"
export DATA_DIR="data"
```

## Ejecución

### 1. Descargar datos

```bash
./ingestion.sh
```

Descarga `flights.zip` desde S3 y lo extrae en el directorio configurado en `config.sh`.

### 2. ETL - Capa Bronze

La capa Bronze extrae e ingesta los datos crudos desde CSV a Parquet en S3.

**Descripción:**
- **Extract**: Lee los archivos CSV crudos (`airlines.csv`, `airports.csv`, `flights.csv`) desde el directorio local
- **Validate**: Verifica que las tablas pequeñas no estén vacías y valida que columnas clave no tengan nulos
- **Load**: Crea la base de datos `flights_bronze` en Glue Catalog y escribe las tablas como Parquet en S3

**Tablas procesadas:**
- `airlines` - Tabla pequeña (cargada completa)
- `airports` - Tabla pequeña (cargada completa)
- `flights` - Tabla grande (procesada en chunks de 500,000 filas)

**Ejecución:**

```bash
# ejemplo  python etl/bronze.py --bucket etl-tarea-8/etl --data-dir data/flights/
python etl/bronze.py --bucket <tu-bucket> --data-dir <ruta-csvs>
```

Ejemplo con configuración en `config.sh`:
```bash
python etl/bronze.py --bucket $S3_BUCKET --data-dir $DATA_DIR
```

**Argumentos:**
- `--bucket`: Nombre del bucket S3 donde almacenar los datos procesados
- `--data-dir`: Directorio local donde están los archivos CSV

**Requisitos:**
- AWS CLI configurado
- Python 3.8+
- Librerías: `pandas`, `awswrangler`