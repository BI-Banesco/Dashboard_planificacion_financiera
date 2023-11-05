import pandas as pd
import pyodbc

# Datos de conexión a SQL Server
server = 'DIEGO\\SQLEXPRESS'
database = 'testExportBI'
username = 'DIEGO\\diego'

# Ruta del archivo CSV
csv_file = 'C:\\Users\\diego\\OneDrive\\Documents\\Python\\data.csv'

# Leer el archivo CSV utilizando pandas
dataframe = pd.read_csv(csv_file)

# Renombrar la columna "ID_TOMADOR.1" a "ID_TOMADOR_1"
dataframe = dataframe.rename(columns={"ID_TOMADOR.1": "ID_TOMADOR_1"})

# Formato de visualización para las columnas de ID
id_columns = ['ID_TOMADOR', 'ID_TOMADOR_1', 'ID_PRODUCTOR', 'ID_REFERIDOR']  # Lista de columnas que contienen los ID

# Aplicar el formato de visualización a las columnas de ID
for column in id_columns:
    dataframe[column] = dataframe[column].apply(lambda x: '{:.0f}'.format(x))

# Establecer la conexión a SQL Server
conn = pyodbc.connect(f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};Trusted_Connection=yes")

# Crear un cursor para ejecutar comandos SQL
cursor = conn.cursor()

# Nombre de la tabla en SQL Server
table_name = 'num_recibos_temp'

# Verificar si la tabla existe
if cursor.tables(table=table_name).fetchone():
    # Borrar los datos existentes en la tabla
    cursor.execute(f"TRUNCATE TABLE {table_name}")
    conn.commit()
else:
    # La tabla no existe, crearla
    # Crear la tabla en SQL Server
    query_create_table = f"CREATE TABLE {table_name} ("

    # Recorrer las columnas del DataFrame para obtener el nombre y tipo de dato
    for column in dataframe.columns:
        column_name = column.replace(' ', '_')  # Reemplazar espacios en blanco por guiones bajos
        column_type = 'VARCHAR(MAX) NULL'  # Permitir NULL en las columnas
        query_create_table += f"{column_name} {column_type},"

    query_create_table = query_create_table[:-1]  # Eliminar la última coma
    query_create_table += ")"

    cursor.execute(query_create_table)
    conn.commit()

# Insertar los datos en la tabla
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
    row_values = [str(value) if pd.isnull(value) else value for value in row]

    cursor.execute(query_insert_data, tuple(row_values))

conn.commit()

# Cerrar la conexión a SQL Server
conn.close()

print("Tabla creada y datos insertados exitosamente.")