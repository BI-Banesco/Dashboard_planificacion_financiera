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

dataframe = dataframe.rename(columns={"SUSCRITO USD": "SUSCRITO_USD", "Source.Name": "Source_Name"})

# Formato de visualización para las columnas de ID
id_columns = ['CD_RAMO_POL', 'CD_ST_RECIBO', 'CD_SUCURSAL_POL']  # Lista de columnas que contienen los ID

# Aplicar el formato de visualización a las columnas de ID
for column in id_columns:
    dataframe[column] = dataframe[column].apply(lambda x: '{:.0f}'.format(x))

# Convertir los valores NaN a None
dataframe = dataframe.fillna(value=np.nan)

# Establecer la conexión a SQL Server
conn = pyodbc.connect("DRIVER={SQL Server};SERVER=DIEGO\\SQLEXPRESS;DATABASE=testExportBI;UID=DIEGO\\diego;Trusted_Connection=yes")


# Crear un cursor para ejecutar comandos SQL
cursor = conn.cursor()

# Nombre de la tabla en SQL Server
table_name = 'num_recibo_4'

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

# Obtener el número de registros en el DataFrame
num_registros = len(dataframe)

print(num_registros)

# Crear un DataFrame con la fecha y hora como índice y el número de registros como columna
df_grafico = pd.DataFrame({'Cantidad de Registros': [num_registros]}, index=[pd.Timestamp.now()])

# Generar el gráfico utilizando matplotlib
hoy = pd.Timestamp.now()
plt.plot(df_grafico.index, df_grafico['Cantidad de Registros'])
plt.scatter(hoy, df_grafico['Cantidad de Registros'], color='red', label='Hoy')
plt.xlabel('Fecha y Hora')
plt.ylabel('Cantidad de Registros')
plt.title('Número de Registros en función de la Fecha y Hora')\

# Aumentar el tamaño de los ticks del eje y
plt.tick_params(axis='y', labelsize=12)

# Mostrar el gráfico en Power BI utilizando la extensión Matplotlib.pyplot
fig = plt.gcf()
fig.set_size_inches(6, 4)  # Ajusta el tamaño del gráfico si es necesario
plt.show()

# Tiempo final de ejecucion 
fin  = time.time()

# Calcula la duración de la ejecución en segundos
duracion = fin - inicio

# Imprime la duración en segundos
print("Tiempo de ejecución:", duracion, "segundos")