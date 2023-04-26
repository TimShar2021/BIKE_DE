import time
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from urllib.request import Request, urlopen
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from variables import path_spisok, home, path_erors
import os


bad_link = []
errors = []
list_all_files = []



@task(retries=3,log_prints=True)
def get_list(url: str, load: bool = True) -> list:
    if load == True:
        user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36'
        chrome_options = Options()
        chrome_options.add_argument('user-agent={0}'.format(user_agent))
        chrome_options.add_argument("--headless")
        chrome_options.add_argument('window-size=1080x1920')
        driver = webdriver.Chrome(options = chrome_options )
        driver.maximize_window()
        driver.get(url)
        time.sleep(30)
        driver.find_element(By.TAG_NAME,'body').send_keys(Keys.END)
        time.sleep(30)
        links = driver.find_elements(By.TAG_NAME, "a")
        time.sleep(5)
        download =[]
        for link in links:
            if link.get_attribute("href")[-3:]=='csv':
                    download.append(link.get_attribute("href"))
            pd.DataFrame({'link':download}).to_csv(path_spisok)
        print(download[1])
    else:
        download = pd.read_csv(path_spisok)['link'].to_list()
    return download


@task(retries=3)
def fetch(url: str) -> pd.DataFrame:
    """Read data from web into pandas DataFrame"""
    req = Request(url)
    req.add_header('User-Agent', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:77.0) Gecko/20100101 Firefox/77.0')
    content = urlopen(req)
    df = pd.read_csv(content)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df.dropna(inplace=True)
    df['EndStation Id'] = df['EndStation Id'].astype('int')
    df['Rental Id'] = df['Rental Id'].astype('int')
    df['Duration'] = df['Duration'].astype('int')
    df['Bike Id'] = df['Bike Id'].astype('int')
    df['StartStation Id'] = df['StartStation Id'].astype('int')
    df["Start Date"] = pd.to_datetime(df["Start Date"], format='%d/%m/%Y %H:%M')
    df["End Date"] = pd.to_datetime(df["End Date"],format='%d/%m/%Y %H:%M')
    df['month'] = df["Start Date"].apply(lambda x:x.month)
    df['year'] = df["Start Date"].apply(lambda x:x.year)
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task(log_prints=True)
def write_local(df: pd.DataFrame) -> None:
    """Write DataFrame out locally as csv file"""
    year =  df['year'].min()
    start_date = df["Start Date"].min().strftime('%d.%m')
    end_date = df["Start Date"].max().strftime('%d.%m')
    path = Path(f"{home}/data/{year}")
    try:
        os.mkdir(path)
    except:
        pass
    path_2 = Path(f"{home}/data/{year}/{start_date}-{end_date}.csv.gz")
    print(path_2)
    df.to_csv(path_2, compression='gzip', index=False)



@task()
def save_erors(bad_link, errors) -> None: 
    df = pd.DataFrame({'link':bad_link,
                       'errors': errors}).to_csv(path_erors, index=False)
    

def extract(url: str = 
                   'https://cycling.data.tfl.gov.uk/',
                   load: bool = True, 
                   all: str  = "no", 
                   start: bool = True, 
                   end: int  = 3):
    """The main ETL function"""
    downloads = get_list(url,load)
    if all == True:
        pass  
    else:
        downloads = downloads[start:end+1]
 
    for link in downloads: 
        try:
            df = fetch(link)   
            df_clean = clean(df)
            path = write_local(df_clean)
        except Exception as e:
            bad_link.append(link)
            errors.append(e)
            continue
    
    save_erors(bad_link, errors)
if __name__ == "__main__":
     url = 'https://cycling.data.tfl.gov.uk/'
     load = True
     all = True
     start = 0
     end = 3
     extract(url,load,all,start,end)