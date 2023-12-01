
import pandas as pd
from sqlalchemy import create_engine
import time
import numpy as np
import matplotlib.pyplot as plt

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

# Obtener el número de registros en el DataFrame
num_registros = 30

# Crear un DataFrame con la fecha y hora como índice y el número de registros como columna
df_grafico = pd.DataFrame({'Cantidad de Registros': [num_registros]}, index=[pd.Timestamp.now()])

# Generar el gráfico utilizando matplotlib
plt.figure(figsize=(6, 4))  # Ajustar el tamaño de la figura
plt.bar(df_grafico.index, df_grafico['Cantidad de Registros'])
plt.xlabel('Fecha y Hora')
plt.ylabel('Cantidad de Registros')
plt.title('Número de Registros en función de la Fecha y Hora')
plt.xticks(rotation=45)  # Ajustar la rotación de las etiquetas del eje x

# Agregar etiquetas de texto encima de cada barra
for i, value in enumerate(df_grafico['Cantidad de Registros']):
    plt.text(df_grafico.index[i], value, str(value), ha='center', va='bottom')

# Guardar el gráfico en un archivo de imagen
plt.savefig('grafico.png')

# Cerrar la figura para liberar memoria
plt.close()

# Tiempo final de ejecucion
fin = time.time()

# Calcula la duración de la ejecución en segundos
duracion = fin - inicio

# Imprime la duración en segundos
print("Tiempo de ejecución:", duracion, "segundos")