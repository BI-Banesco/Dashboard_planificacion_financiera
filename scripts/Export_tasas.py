import pandas as pd
from sqlalchemy import create_engine
import time

# Tiempo inicial de ejecución
inicio = time.time()

# Datos de conexión a SQL Server
server = 'DIEGO\\SQLEXPRESS'
database = 'testExportBI'
username = 'DIEGO\\diego'

# Ruta del archivo CSV
txt_file = 'C:\\Users\\diego\\OneDrive\\Documents\\Python\\Tasas_v2.txt'

# Nombre de la tabla en la base de datos
table_name = 'TB_TASAS_SA'

# Número máximo de registros a guardar

# Leer los registros del archivo CSV utilizando Pandas
dataframe = pd.read_csv(txt_file, delimiter='\t')

print(dataframe)

# Obtener el número de registros en el DataFrame
num_registros = len(dataframe)

# Crear la cadena de conexión para autenticación de Windows
conn_str = f'mssql+pyodbc://{server}/{database}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server'

# Crear el motor de SQLAlchemy para la conexión a la base de datos
engine = create_engine(conn_str)

# Utilizar la función to_sql de Pandas para insertar los datos
with engine.begin() as connection:
    dataframe.to_sql(table_name, con=connection, if_exists='replace', index=False)

# Tiempo final de ejecución
fin = time.time()

# Calcula la duración de la ejecución en segundos
duracion = fin - inicio

# Imprime la duración en segundos
print('Tiempo de ejecución:', duracion, 'segundos')