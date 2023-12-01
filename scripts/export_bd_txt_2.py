import csv
from sqlalchemy import create_engine
import time

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

# Consulta SQL para obtener los datos de TB_PRIMAS_METAS_SA
query = 'SELECT * FROM V_PRIMAS'

# Ruta del archivo TXT de salida
txt_file_output = 'C:/Users/diego/OneDrive/Documents/Python/test_export.txt'

# Guardar los datos en el archivo de texto utilizando el módulo csv
with open(txt_file_output, 'w', newline='') as archivo:
    writer = csv.writer(archivo, delimiter='|')

    # Crear una conexión a la base de datos
    with engine.connect() as connection:
        # Ejecutar la consulta y guardar los resultados en el archivo
        result = connection.execute(query)
        for row in result:
            writer.writerow(row)

# Tiempo final de ejecución
fin = time.time()

# Calcula la duración de la ejecución en segundos
duracion = fin - inicio

# Imprimir mensaje de éxito
print('Exportación completada')