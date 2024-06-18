import os
import sys
from src.laptopprediction.exception import CustomException
from src.laptopprediction.logger import logging
import pandas as pd
from dotenv import load_dotenv
import pymysql

load_dotenv()

host=os.getenv("host")
user=os.getenv("user")
password=os.getenv("password")
db_train=os.getenv("db_train")
db_test=os.getenv("db_test")



def read_sql_train():
    logging.info("Reading SQL database which is train")
    try:
        mydb_train=pymysql.connect(
            host=host,
            user=user,
            password=password,
            db=db_train
        )
        
        logging.info("Connection Established: %s",mydb_train)
        df_train=pd.read_sql_query('Select * from laptops_train;',mydb_train)
        return df_train 
        
    except Exception as ex:
        raise CustomException(ex)

def read_sql_test():
    logging.info("Reading SQL database which is train")
    try:
        mydb_test=pymysql.connect(
            host=host,
            user=user,
            password=password,
            db=db_test
        )
        
        logging.info("Connection Established: %s", mydb_test)
        df_test=pd.read_sql_query('Select * from laptops_test;',mydb_test)

        return df_test 
               
    except Exception as ex:
        raise CustomException(ex)

