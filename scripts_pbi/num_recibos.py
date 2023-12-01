import pandas as pd
import numpy as np
import pyodbc
import time
import matplotlib.pyplot as plt

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

# Establecer la conexión a SQL Server
conn = pyodbc.connect("DRIVER={SQL Server};SERVER=DIEGO\\SQLEXPRESS;DATABASE=testExportBI;UID=DIEGO\\diego;Trusted_Connection=yes")

# Crear un cursor para ejecutar comandos SQL
cursor = conn.cursor()

# Nombre de la tabla en SQL Server
table_name = 'TB_MARCA_RECIBO'

# Borrar los datos existentes en la tabla
cursor.execute(f"TRUNCATE TABLE {table_name}")

# Insertar los nuevos datos en la tabla
query_insert_data = f"INSERT INTO {table_name} ("

# Recorrer las columnas del DataFrame para obtener los nombres de las columnas
for column in dataframe.columns:
    column_name = column.replace(' ', '_')  # Reemplazar espacios en blanco por guiones bajos
    query_insert_data += f"{column_name},"

query_insert_data = query_insert_data[:-1]  # Eliminar la última coma
query_insert_data += ") VALUES ("

# Generar los placeholders para los valores
query_insert_data += "?," * len(dataframe.columns)
query_insert_data = query_insert_data[:-1]  # Eliminar la última coma
query_insert_data += ")"

# Insertar los datos fila por fila
for row in dataframe.itertuples(index=False):
    # Convertir los valores a tipos de datos apropiados
    row_values = [str(value) if value is not None else value for value in row]
    cursor.execute(query_insert_data, tuple(row_values))

conn.commit()

# Definir la declaración SQL con parámetros de marcador de posición
cursor.execute("EXEC SP_ACTUALIZAR_PRIMAS '2023-9-1', '1'")

conn.commit()

# Definir la declaración SQL con parámetros de marcador de posición
cursor.execute("EXEC SP_ACTUALIZAR_SUSCRITO '2023-9-1', '1'")

conn.commit()

# Cerrar la conexión a SQL Server
conn.close()

# Obtener el número de registros en el DataFrame
num_registros = len(dataframe)

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