from extract import extract
from load_to_gcs import load_to_gcs
from load_to_bq import load_to_bq
from prefect import flow

@flow()
def main_flow(url: str = 'https://cycling.data.tfl.gov.uk/',
     extr: bool = True,
     gcs: bool = True,
     bq: bool  = True,
     load: bool = True,
     all: bool = True,
     start: int = 0,
     end: int = 3,
     years: list = ['2016'],
     name_cluster = 'dezoomcluster',
     dataset_bq = 'bicycles_data_all'):
     if extr ==True:
          extract(url,load, all,start,end)
     else:
          pass
     if gcs ==True:
          load_to_gcs(years)
     else:
          pass
     if bq == True:
          load_to_bq(name_cluster,years,dataset_bq)
     else:
          pass

if __name__ == "__main__":
     extr = True,
     gcs = True,
     bq = True,
     url = 'https://cycling.data.tfl.gov.uk/'
     load = True,
     all = True,
     start = 0,
     end = 3,
     years = ['2015','2016','2017','2018','2019','2020','2021','2022']
     name_cluster = 'dezoomcluster',
     dataset_bq = 'bicycles_data_all'
     main_flow(extr, gcs, bq, url, load, all, start, end, years)

