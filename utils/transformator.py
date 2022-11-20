import logging
import os
import pandas as pd
from database_connector import DatabaseConnector

class Transformator():
    def __init__(self, entity):
        self.entity = entity
        import json
        conn_config = json.load(open('./config/data_directories.json'))
        os.environ['KAGGLE_CONFIG_DIR'] = "./config" #conn_config['kaggle_config_dir']
        self.data_dir = conn_config['data_dir']
        self.data_dir_landing_raw = conn_config['data_dir_landing_raw']
        self.data_dir_landing_unzip = conn_config['data_dir_landing_unzip']
        self.df = pd.read_csv(f"{self.data_dir_landing_unzip}/{entity}.csv")


    def save_to_db(self):
        connector = DatabaseConnector()
        connector.csv_to_db_raw(self.df, self.entity)
    def transform(self, sql):
        connector = DatabaseConnector()
        connector.query_no_result(sql)
