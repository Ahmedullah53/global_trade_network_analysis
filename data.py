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

    def merge_data(self, raw_data_path='./data/raw', save_data_path='./data/processed/comtrade.csv'):
        files = os.listdir(raw_data_path)
        data = None
        print('Merginh data...')
        for count, file in enumerate(files):
            if data is None:
                data = pd.read_csv(os.path.join(raw_data_path, file))
            else:
                data = pd.concat([data, pd.read_csv(os.path.join(raw_data_path, file))])
            percentage_complete = math.floor(((count+1)/len(files))*100)
            print(percentage_complete)
            
        data.to_csv(save_data_path)
        print('Merging data complete...')

    def create_nodes(self, df, save_data_path):
        node = df.copy()
        node = node[['Year', 'Reporter', 'Partner', 'Trade Flow']]
        node['weight'] = node.loc[(node['Trade Flow'] != 'Re-Export') & (node['Trade Flow'] != 'Re-Import')].groupby('Reporter')['Reporter'].transform('count')
        node['label'] = node['Reporter']
        node.drop(columns=['Year', 'Reporter', 'Trade Flow', 'Partner'], inplace=True)
        node.dropna(axis=0, inplace=True)
        node.drop_duplicates(inplace=True, ignore_index=True)
        node.to_csv(save_data_path)
        print('Node data created...')

    def create_edges(self, df, save_data_path):
        edge = df.copy()
        edge = edge[['Year', 'Reporter', 'Partner', 'Trade Flow']]
        edge = edge.drop(columns=['Year', 'Trade Flow'])
        edge.drop_duplicates(inplace=True, ignore_index=True)
        edge.to_csv(save_data_path)
        print('Edges data created...')

    def convert_to_graph_data(self, from_file='./data/processed/comtrade.csv', save_data_path='./data/processed'):
        df = pd.read_csv(from_file)
        self.create_nodes(df, os.path.join(save_data_path, 'nodes.csv'))
        self.create_edges(df, os.path.join(save_data_path, 'edges.csv'))



# drive = Drive()
# print(drive.upload("student-por.csv"))
def main():
    comtrade = Comtrade()
    # comtrade.get_data()
    comtrade.merge_data()
    comtrade.convert_to_graph_data()

if __name__ == '__main__':
    main()