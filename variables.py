from pathlib import Path

# Puth for list url 
path_spisok = Path(f'-----------logs/spisok.csv')

# Puth for save info about errors
path_erors  = Path(f'--------------/logs/erors.csv')

# Puth for project
home  = '-----------'


# Puth json file keys for GCP 
credentials_location = '----------.json'

# Bucket GCP 
my_bucket = 'gs://--------'

# BQ connector
# Ref:https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark-example

bq_connector = 'gs://spark-lib/bigquery/spark-3.3-bigquery-0.30.0.jar'