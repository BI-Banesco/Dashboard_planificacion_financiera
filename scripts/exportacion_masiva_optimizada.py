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
txt_file = 'C:\\Users\\diego\\OneDrive\\Documents\\test_primas_siniestro\\01_Tabla_de_Primas_y_Siniestros_BSV_09_2023.txt'

# Nombre de la tabla en la base de datos
table_name = 'TB_PRIMAS_METAS_SA'

# Número máximo de registros a guardar
max_records = 100

# Leer los primeros 100,000 registros del archivo CSV utilizando Pandas
dataframe = pd.read_csv(txt_file, delimiter='|', nrows=max_records)

# Eliminar las columnas especificadas
#columnas_eliminar = ['CD_LINEA_NEGOCIO']
#dataframe = dataframe.drop(columnas_eliminar, axis=1)

# Verificar y agregar las columnas 'VERSION'
#columnas_version = ['VERSION', 'CARE_CASU_CD_SUCURSAL', 'CARE_CARP_CD_RAMO', 'CARE_CAPO_NU_POLIZA']
#for col in columnas_version:
#    if col not in dataframe.columns:
#        dataframe[col] = None
#        dataframe[col] = 1

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