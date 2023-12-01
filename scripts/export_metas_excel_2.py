import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import time

# Leer los DataFrames
dataframe1 = pd.read_excel('C:\\Users\\diego\\OneDrive\\Documents\\Python\\Metas Plan BSV 2023 V03.xlsx', sheet_name='Metas Suscrito', skiprows=range(0, 11))
dataframe2 = pd.read_excel('C:\\Users\\diego\\OneDrive\\Documents\\Python\\Metas Plan BSV 2023 V03.xlsx', sheet_name='Metas Siniestralidad', skiprows=range(0, 11))

# Renombrar las columnas para que coincidan
dataframe1 = dataframe1.rename(columns={'MES1': 'MES', 'SM_MONEDA_RECIBO1': 'SM_MONEDA_RECIBO', 'LINEA_NEGOCIO1': 'LINEA_NEGOCIO'})
dataframe2 = dataframe2.rename(columns={'MES2': 'MES', 'SM_MONEDA_RECIBO2': 'SM_MONEDA_RECIBO', 'LINEA_NEGOCIO2': 'LINEA_NEGOCIO'})

# Definir una función para asignar columnas
def asignar_columnas(dataframe1, dataframe2, column_name):
    merged_data = pd.merge(dataframe1, dataframe2, on=['MES', 'SM_MONEDA_RECIBO', 'LINEA_NEGOCIO'], how='left')
    dataframe1[column_name] = np.where(merged_data[column_name].notnull(), merged_data[column_name], '')

# Asignar las columnas META_INCURRIDO y META DEVENGADO del DataFrame 2 al DataFrame 1
asignar_columnas(dataframe1, dataframe2, 'META_INCURRIDO')
asignar_columnas(dataframe1, dataframe2, 'META_DEVENGADO')

# Eliminar las columnas especificadas
columnas_eliminar = ['Unnamed: 0', 'QUERY', 'ORDENAR METAS POR FECHA DE MAS ANTIGUO A MAS RECIENTE']
dataframe1 = dataframe1.drop(columnas_eliminar, axis=1)

# Tiempo inicial de ejecución
inicio = time.time()

# Datos de conexión a SQL Server
server = 'DIEGO\\SQLEXPRESS'
database = 'testExportBI'
username = 'DIEGO\\diego'

# Nombre de la tabla en la base de datos
table_name = 'TB_METAS_SA'

# Crear la cadena de conexión para autenticación de Windows
conn_str = f'mssql+pyodbc://{server}/{database}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server'

# Crear el motor de SQLAlchemy para la conexión a la base de datos
engine = create_engine(conn_str)

# Utilizar la función to_sql de Pandas para insertar los datos
with engine.begin() as connection:
    dataframe1.to_sql(table_name, con=connection, if_exists='replace', index=False)

# Tiempo final de ejecución
fin = time.time()

# Calcula la duración de la ejecución en segundos
duracion = fin - inicio

# Imprime la duración en segundos
print('Tiempo de ejecución:', duracion, 'segundos')