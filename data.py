import pandas as pd
import requests
import os
from drive import Drive
import constants
import math
import time

class Comtrade:
    def __init__(self):
        pass
    
    def get_partners_codes_list(self):
        res = requests.get('https://comtrade.un.org/Data/cache/partnerAreas.json')
        data = res.json().get('results')
        data = pd.DataFrame(data)
        return data['id'][2:]

    def load_partners_code(self)-> pd.DataFrame:
        codes = pd.read_csv('./data/partners_codes.csv')
        return codes
        
    def get_data_api(self, year_bracket, partners, commodities):
        res = requests.get(f'http://comtrade.un.org/api/get?max=50000&type=C&freq=A&px=HS&ps={year_bracket}&r=all&p={partners}&rg=all&cc={commodities}&fmt=csv')
        return res.text
    
    def save_data(self, filepath, data):
        with open(filepath, 'w') as file:
            file.write(data)
        print(f'New file {filepath} created')
    
    def get_data(self):
        start = 0
        end = 19
        partners = constants.partners
        # years = constants.year_brackets
        years = constants.years
        calls = math.ceil(len(partners)/20)
        count = 1
        for call_index, call in enumerate(range(calls)):
            for partnet_index, partner in enumerate(partners[start:end]):
                for year_index, year in enumerate(years):
                    filepath = f'./data/comtrade/comtrade_{call_index}_{partnet_index}_{year_index}.csv'
                    data = self.get_data_api(year, partner, constants.commodities)
                    self.save_data(filepath, data)
                    count += 1
                    if count == 100:
                        time.sleep(4000)
                        count = 1
            start += 20
            end += 20



# drive = Drive()
# print(drive.upload("student-por.csv"))

comtrade = Comtrade()
comtrade.get_data()