
import os
from prefect import flow, task
from variables import bq_connector, my_bucket


@task()
def write_to_bq(name_cluster: str, year, dataset_bq: str = 'bicycles_data_all') -> None:
    command = f"""
                gcloud dataproc jobs submit pyspark ./job_for_dataproc.py \
                    --cluster={name_cluster} \
                    --region=us-central1 \
                    --jars={bq_connector} \
                    --\
                        --bucket={my_bucket} \
                        --input_year={year} \
                        --output_1={dataset_bq}.station_bike \
                        --output_2={dataset_bq}.report_bike 
                """
    os.system(command)



@flow()
def load_to_bq(name_cluster,years, dataset_bq):
    for year in years:
        write_to_bq(name_cluster,year,dataset_bq)


if __name__ == "__main__":
     name_cluster = 'dezoomcluster'
     years = ['2015']
     dataset_bq = 'bicycles_data_all'
     load_to_bq(name_cluster,years, dataset_bq)