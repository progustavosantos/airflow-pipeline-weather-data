import os
import pandas as pd
from os.path import join
from datetime import datetime, timedelta

date_init = datetime.today().strftime('%Y-%m-%d')
date_final = (datetime.today() + timedelta(days=7)).strftime('%Y-%m-%d')
city = 'Boston'
key = 'BGNJPFH697FZ6J229UCTBM3AH'

URL = join("https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/",
          f"{city}/{date_init}/{date_final}?unitGroup=metric&include=days&key={key}&contentType=csv")

get_data_from_api = pd.read_csv(URL)
print(get_data_from_api.head(100))

file_path = f'C:\DevSolutions\DataEng\DataPipelines\WeatherData\week={date_init}'
os.mkdir(file_path)

get_data_from_api.to_csv(file_path + 'raw_data.csv')
get_data_from_api[['datetime','tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperature.csv')
get_data_from_api[['datetime', 'description', 'icon']].to_csv(file_path + 'conditions.csv')