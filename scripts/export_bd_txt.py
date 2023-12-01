import pandas as pd
from sqlalchemy import create_engine
import time
import numpy as np

# Tiempo inicial de ejecución
inicio = time.time()

# Datos de conexión a SQL Server
server = 'DIEGO\\SQLEXPRESS'
database = 'testExportBI'
username = 'DIEGO\\diego'

# Crear la cadena de conexión para autenticación de Windows
conn_str = f'mssql+pyodbc://{server}/{database}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server'

# Crear el motor de SQLAlchemy para la conexión a la base de datos
engine = create_engine(conn_str)

# Consulta SQL para obtener los datos de V_PRIMAS
query = 'SELECT * FROM V_PRIMAS'

# Leer los datos de la tabla V_PRIMAS en un DataFrame
dataframe = pd.read_sql_query(query, engine)

# Reemplazar None y NaN por cadena vacía
dataframe = dataframe.fillna("")

# Convertir cada columna del DataFrame a una cadena separada por '|'
columnas = dataframe.columns
texto_columnas = '|'.join(columnas)
texto_filas = dataframe.apply(lambda row: '|'.join(row.astype(str)), axis=1)

# Unir las filas en una sola cadena de texto
texto = texto_columnas + '\n' + texto_filas.str.cat(sep='\n')

# Ruta del archivo TXT de salida
txt_file_output = 'C:/Users/diego/OneDrive/Documents/Python/test_export_6.txt'

# Guardar el texto en un archivo de texto
with open(txt_file_output, 'w', encoding='utf-8') as archivo:
    archivo.write(texto)


# Contar el número de filas procesadas
num_filas = len(dataframe)

# Tiempo final de ejecución
fin = time.time()

# Calcula la duración de la ejecución en segundos
duracion = fin - inicio

# Imprime el número de filas procesadas
print('Archivo procesados: test_export_5.txt')

print('Duración de generación del txt:', duracion)

# Imprimir mensaje de éxito
print('Exportación completada')