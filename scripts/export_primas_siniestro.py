import pandas as pd
import pyodbc
import time

# Tiempo inicial de ejecución
inicio = time.time()

# Datos de conexión a SQL Server
server = 'DIEGO\\SQLEXPRESS'
database = 'testExportBI'
username = 'DIEGO\\diego'

# Ruta del archivo CSV
txt_file = 'C:\\Users\\diego\\OneDrive\\Documents\\test_primas_siniestro\\01_Tabla_de_Primas_y_Siniestros_BSV_09_2023.txt'

# Leer el archivo CSV utilizando pandas
dataframe = pd.read_csv(txt_file, delimiter='|')

# Eliminar las columnas especificadas
columnas_eliminar = ['CD_LINEA_NEGOCIO']
dataframe = dataframe.drop(columnas_eliminar, axis=1)

# Verificar si la columna 'VERSION' existe en el DataFrame
if 'VERSION' not in dataframe.columns:
    # Agregar la columna 'VERSION' con valores nulos
    dataframe['VERSION'] = None
    # Asignar el valor '1' a la columna 'VERSION'
    dataframe['VERSION'] = 1

# Verificar si la columna 'VERSION' existe en el DataFrame
if 'CARE_CASU_CD_SUCURSAL' not in dataframe.columns:
    # Agregar la columna 'VERSION' con valores nulos
    dataframe['CARE_CASU_CD_SUCURSAL'] = None
    # Asignar el valor '1' a la columna 'VERSION'
    dataframe['CARE_CASU_CD_SUCURSAL'] = 1

# Verificar si la columna 'VERSION' existe en el DataFrame
if 'CARE_CARP_CD_RAMO' not in dataframe.columns:
    # Agregar la columna 'VERSION' con valores nulos
    dataframe['CARE_CARP_CD_RAMO'] = None
    # Asignar el valor '1' a la columna 'VERSION'
    dataframe['CARE_CARP_CD_RAMO'] = 1

# Verificar si la columna 'VERSION' existe en el DataFrame
if 'CARE_CAPO_NU_POLIZA' not in dataframe.columns:
    # Agregar la columna 'VERSION' con valores nulos
    dataframe['CARE_CAPO_NU_POLIZA'] = None
    # Asignar el valor '1' a la columna 'VERSION'
    dataframe['CARE_CAPO_NU_POLIZA'] = 1


# Establecer la conexión a SQL Server
conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};Trusted_Connection=yes')

# Crear un cursor para ejecutar comandos SQL
cursor = conn.cursor()

# Nombre de la tabla en SQL Server
table_name = 'TB_PRIMAS_METAS'

# Borrar los datos existentes en la tabla
cursor.execute(f'TRUNCATE TABLE {table_name}')

# Obtener el esquema de la tabla desde la base de datos
query_schema = f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
cursor.execute(query_schema)
table_schema = cursor.fetchall()

# Convertir los valores en el dataframe al tipo de dato correspondiente en la tabla
for column_info in table_schema:
    column_name = column_info.COLUMN_NAME
    data_type = column_info.DATA_TYPE
    
    # Verificar si la columna existe en el dataframe
    if column_name in dataframe.columns:
        # Convertir 'varchar' a 'object' en Pandas
        if data_type == 'varchar':
            dataframe[column_name] = dataframe[column_name].astype(object)
        # Convertir 'int' a 'int64' en Pandas
        elif data_type == 'int':
            dataframe[column_name] = pd.to_numeric(dataframe[column_name], errors='coerce', downcast='integer')
        # Convertir 'date' a 'datetime64' en Pandas
        elif data_type == 'date':
            dataframe[column_name] = pd.to_datetime(dataframe[column_name], format='%d/%m/%Y', errors='coerce')
        # Convertir 'decimal' a 'float64' en Pandas
        elif data_type == 'decimal':
            dataframe[column_name] = pd.to_numeric(dataframe[column_name], errors='coerce', downcast='float')
    
# Insertar los nuevos datos en la tabla
query_insert_data = f'INSERT INTO {table_name} ('

# Recorrer las columnas del DataFrame para obtener los nombres de las columnas
for column in dataframe.columns:
    column_name = column.replace(' ', '_')  # Reemplazar espacios en blanco por guiones bajos
    query_insert_data += f'{column_name},'

query_insert_data = query_insert_data[:-1]  # Eliminar la última coma
query_insert_data += ') VALUES ('

# Generar los placeholders para los valores
query_insert_data += '?,' * len(dataframe.columns)
query_insert_data = query_insert_data[:-1]  # Eliminar la última coma
query_insert_data += ')'

# Insertar los datos fila por fila
for row in dataframe.itertuples(index=False):
    # Convertir los valores a tipos de datos apropiados
    row_values = [value if pd.notnull(value) else None for value in row]
    cursor.execute(query_insert_data, tuple(row_values))

conn.commit()

print('Datos borrados y nuevos datos insertados exitosamente en la tabla existente.')

# Tiempo final de ejecución
fin = time.time()

# Calcula la duración de la ejecución en segundos
duracion = fin - inicio

# Imprime la duración en segundos
print('Tiempo de ejecución:', duracion, 'segundos')

# Cerrar la conexión a SQL Server
conn.close()