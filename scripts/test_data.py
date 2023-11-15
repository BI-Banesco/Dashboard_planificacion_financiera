import pandas as pd
import numpy as np


csv_file = 'C:\\Users\\diego\\OneDrive\\Documents\\Python\\data.csv'

# Leer el archivo CSV utilizando pandas
dataframe = pd.read_csv(csv_file)

dataframe = dataframe.rename(columns={"CD_RAMO_POL": "CD_RAMO", "CD_SUCURSAL_POL": "CD_SUCURSAL"})

columnas_eliminar = ['CD_ST_RECIBO', 'Mes', 'POLIZAID_CERTIFID', 'Source.Name', 'TABLA', 'TP_POLIZA']
dataframe = dataframe.drop(columnas_eliminar, axis=1)

print(dataframe);