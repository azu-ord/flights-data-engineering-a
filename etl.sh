#!/bin/bash

python etl/bronze.py --bucket <tu-bucket> --data-dir data/
python etl/silver.py --bucket <tu-bucket>
python etl/gold.py   --bucket <tu-bucket>