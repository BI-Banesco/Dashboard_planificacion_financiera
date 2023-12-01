import pandas as pd
from sqlalchemy import create_engine
import time

# Tiempo inicial de ejecución
inicio = time.time()

# Datos de conexión a SQL Server
server = 'DIEGO\\SQLEXPRESS'
database = 'testExportBI'
username = 'DIEGO\\diego'

# Ruta del archivo Excel
excel_file = 'C:\\Users\\diego\\OneDrive\\Documents\\Python\\Metas Plan BSV 2023 V03.xlsx'

# Número máximo de registros a guardar por hoja
max_records = 100

# Nombres de las hojas a leer
hoja1 = 'Metas Suscrito'
hoja2 = 'Metas Siniestralidad'

# Fila inicial para leer los datos
fila_inicial = 12

# Crear la cadena de conexión para autenticación de Windows
conn_str = f'mssql+pyodbc://{server}/{database}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server'

# Crear el motor de SQLAlchemy para la conexión a la base de datos
engine = create_engine(conn_str)

# Leer y guardar los datos de la primera hoja
dataframe1 = pd.read_excel(excel_file, sheet_name=hoja1, skiprows=range(0, fila_inicial - 1), nrows=max_records)

# Eliminar las columnas especificadas
columnas_eliminar = ['ORDENAR METAS POR FECHA DE MAS ANTIGUO A MAS RECIENTE', 'QUERY', 'FECHA']
dataframe1 = dataframe1.drop(columnas_eliminar, axis=1)

# Ajustar la configuración de visualización
pd.set_option('display.max_columns', None)  # Mostrar todas las columnas

print(dataframe1)

#table_name1 = f'TB_PRIMAS_METAS_SA_{hoja1}'
#with engine.begin() as connection:
#    dataframe1.to_sql(table_name1, con=connection, if_exists='replace', index=False)

# Leer y guardar los datos de la primera hoja
dataframe2 = pd.read_excel(excel_file, sheet_name=hoja2, skiprows=range(0, fila_inicial - 1), nrows=max_records)

print(dataframe2)

#table_name2 = f'TB_PRIMAS_METAS_SA_{hoja2}'
#with engine.begin() as connection:
#    dataframe2.to_sql(table_name2, con=connection, if_exists='replace', index=False)

# Tiempo final de ejecución
fin = time.time()

# Calcula la duración de la ejecución en segundos
duracion = fin - inicio

# Imprime la duración en segundos
print('Tiempo de ejecución:', duracion, 'segundos')