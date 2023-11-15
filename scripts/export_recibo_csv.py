from sqlalchemy.engine import create_engine
import pandas as pd
import pymysql
from sqlalchemy import URL

# Ruta del archivo CSV
csv_file = 'C:\\Users\\diego\\OneDrive\\Documents\\Python\\data.csv'

connection_string = "mssql+pymssql://DIEGO%5Cdiego@DIEGO%5CSQLEXPRESS/testExportBI?trusted_connection=yes"

# pymssql
engine = create_engine(connection_string)

dataframe = pd.read_csv(csv_file)

dataframe.to_sql('test', con=engine, if_exists='append')
