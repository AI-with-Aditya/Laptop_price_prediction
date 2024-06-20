import os
import sys
import shutil
from src.laptopprediction.exception import CustomException
from src.laptopprediction.logger import logging
import pandas as pd
from src.laptopprediction.utils import read_sql_train
from src.laptopprediction.utils import read_sql_test
from dataclasses import dataclass

@dataclass
class DataIngestionConfig:
    train_data_path:str=os.path.join('artifacts','train.csv')
    test_data_path:str=os.path.join('artifacts','test.csv')
    train_path:str=os.path.join('notebook/data','train.csv')
    test_path:str=os.path.join('notebook/data','test.csv')


class DataIngestion:
    def __init__(self):
        self.ingestion_config=DataIngestionConfig()

    def initiate_data_ingestion(self):
        try:
            ##reading the data from mysql
            df_train=read_sql_train()
            df_test=read_sql_test()
            logging.info("Reading completed mysql database")

            os.makedirs(os.path.dirname(self.ingestion_config.train_data_path),exist_ok=True)
            df_train.to_csv(self.ingestion_config.train_data_path,index=False,header=True)
            df_test.to_csv(self.ingestion_config.test_data_path,index=False,header=True)

            os.makedirs(os.path.dirname(self.ingestion_config.train_path),exist_ok=True)
            df_train.to_csv(self.ingestion_config.train_path,index=False,header=True)
            df_test.to_csv(self.ingestion_config.test_path,index=False,header=True)


            logging.info("Data Ingestion is completed")

            return(
                self.ingestion_config.train_data_path,
                self.ingestion_config.test_data_path,
                self.ingestion_config.train_path,
                self.ingestion_config.test_path
            )


        except Exception as e:
            raise CustomException(e,sys)
        

    try:
        # Define the source files and destination files
        train_file = "notebook/data/train.csv"
        test_file = "notebook/data/test.csv"
        destination_dir = "notebook/data/"
        raw_file = os.path.join(destination_dir, "raw.csv")
        destination_dir_2 = "artifacts/"
        raw_file_2 = os.path.join(destination_dir_2, "raw.csv")

        # Create the destination directories if they don't exist
        os.makedirs(destination_dir, exist_ok=True)
        os.makedirs(destination_dir_2, exist_ok=True)

        # Read the train and test CSV files
        train_df = pd.read_csv(train_file)
        test_df = pd.read_csv(test_file)

        # Concatenate the dataframes
        raw_df = pd.concat([train_df, test_df], ignore_index=True)

        # Save the combined dataframe to raw.csv in both directories
        raw_df.to_csv(raw_file, index=False)
        raw_df.to_csv(raw_file_2, index=False)

        logging.info("train.csv and test.csv have been combined into raw.csv in notebook/data/ and artifact/")

    except Exception as e:
        raise CustomException(e,sys)