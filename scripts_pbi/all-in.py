import pandas as pd
import numpy as np
import pyodbc
import time
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

# Tiempo inicial de ejecucion
inicio = time.time()

# Datos de conexión a SQL Server
server = 'DIEGO\\SQLEXPRESS'
database = 'testExportBI'
username = 'DIEGO\\diego'

# Ruta del archivo CSV
csv_file_recibos = 'C:\\Users\\diego\\OneDrive\\Documents\\Python\\data.csv'

# Leer el archivo CSV utilizando pandas
dataframe = pd.read_csv(csv_file_recibos)

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

# Definir la declaración SQL con parámetros de marcador de posición
query = "EXEC SP_ACTUALIZAR_PRIMAS ?, ?"

# Ejecutar la declaración con los parámetros
cursor.execute(query, ('2023-9-1', '1'))

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

# Nombre de la tabla en la base de datos
table_name1 = 'TB_METAS_SA'

# Crear la cadena de conexión para autenticación de Windows
conn_str = f'mssql+pyodbc://{server}/{database}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server'

# Crear el motor de SQLAlchemy para la conexión a la base de datos
engine = create_engine(conn_str)

# Utilizar la función to_sql de Pandas para insertar los datos
with engine.begin() as connection:
    dataframe1.to_sql(table_name1, con=connection, if_exists='replace', index=False)


# Ruta del archivo CSV
txt_file = 'C:\\Users\\diego\\OneDrive\\Documents\\Python\\Tasas_v2.txt'

# Nombre de la tabla en la base de datos
table_name = 'TB_TASAS_SA'

# Número máximo de registros a guardar

# Leer los registros del archivo CSV utilizando Pandas
dataframe_tasas = pd.read_csv(txt_file, delimiter='\t')

print(dataframe_tasas)

# Crear la cadena de conexión para autenticación de Windows
conn_str = f'mssql+pyodbc://{server}/{database}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server'

# Crear el motor de SQLAlchemy para la conexión a la base de datos
engine = create_engine(conn_str)

# Utilizar la función to_sql de Pandas para insertar los datos
with engine.begin() as connection:
    dataframe.to_sql(table_name, con=connection, if_exists='replace', index=False)


# Nombre de la tabla en SQL Server
table_name2 = 'TB_MARCA_RECIBO'

# Borrar los datos existentes en la tabla
cursor.execute(f"TRUNCATE TABLE {table_name2}")

# Insertar los nuevos datos en la tabla
query_insert_data = f"INSERT INTO {table_name2} ("

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
query = "EXEC SP_ACTUALIZAR_SUSCRITO ?, ?"

# Ejecutar la declaración con los parámetros
cursor.execute(query, ('2023-9-1', '1'))

conn.commit()

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
txt_file_output = 'C:/Users/diego/OneDrive/Documents/Python/test_export_5.txt'

# Guardar el texto en un archivo de texto
with open(txt_file_output, 'w', encoding='utf-8') as archivo:
    archivo.write(texto)
# Imprimir mensaje de éxito
print('Exportación completada')

print("Datos borrados y nuevos datos insertados exitosamente en la tabla existente.")

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