import pandas as pd
import numpy as np
import pyodbc
import time

# Tiempo inicial de ejecucion 
inicio = time.time()

# Datos de conexión a SQL Server
server = 'DIEGO\\SQLEXPRESS'
database = 'testExportBI'
username = 'DIEGO\\diego'

# Ruta del archivo CSV
csv_file = 'C:\\Users\\diego\\OneDrive\\Documents\\Python\\data.csv'

# Leer el archivo CSV utilizando pandas
dataframe = pd.read_csv(csv_file)

dataframe = dataframe.rename(columns={"CD_RAMO_POL": "CD_RAMO", "CD_SUCURSAL_POL": "CD_SUCURSAL", "NU_POLIZA": "NU_POLIZA"})

# Eliminar las columnas especificadas
columnas_eliminar = ['CD_ST_RECIBO', 'MES', 'POLIZAID_CERTIFID', 'Source.Name', 'TABLA', 'TP_POLIZA', 'SUSCRITO USD']
dataframe = dataframe.drop(columnas_eliminar, axis=1)

# Convertir los valores NaN a None
dataframe = dataframe.fillna(value=np.nan)

# Opcionalmente, especificar columnas para identificar duplicados
dataframe = dataframe.drop_duplicates(subset=['CD_SUCURSAL', 'CD_RAMO', 'NU_POLIZA'])

print(dataframe)