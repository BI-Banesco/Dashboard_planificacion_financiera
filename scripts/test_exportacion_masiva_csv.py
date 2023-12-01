import csv
import pyodbc
import time

# Tiempo inicial de ejecución
inicio = time.time()

# Datos de conexión a SQL Server
server = 'DIEGO\\SQLEXPRESS'
database = 'testExportBI'
username = 'DIEGO\\diego'

# Ruta del archivo CSV
csv_file = 'C:\\Users\\diego\\OneDrive\\Documents\\Python\\data.csv'

# Leer el archivo CSV y almacenar los datos en una lista de diccionarios
data = []
with open(csv_file, 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        data.append({
            "CD_RAMO": row["CD_RAMO_POL"],
            "CD_SUCURSAL": row["CD_SUCURSAL_POL"],
            "NU_RECIBO": row["NU_RECIBO"],
            **row
        })

# Eliminar las columnas especificadas
columnas_eliminar = ['CD_ST_RECIBO', 'Mes', 'POLIZAID_CERTIFID', 'Source.Name', 'TABLA', 'TP_POLIZA', 'SUSCRITO USD']
data = [{key: row[key] for key in row.keys() if key not in columnas_eliminar} for row in data]

# Convertir los valores a tipo int
for row in data:
    row['CD_RAMO'] = int(row['CD_RAMO'])
    row['CD_SUCURSAL'] = int(row['CD_SUCURSAL'])
    row['NU_RECIBO'] = int(row['NU_RECIBO'])

# Establecer la conexión a SQL Server
conn = pyodbc.connect(f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};Trusted_Connection=yes")

# Borrar los datos existentes y realizar el bulk insert dentro de una transacción
with conn.cursor() as cursor:
    # Iniciar la transacción
    conn.autocommit = False
    
    try:
        # Nombre de la tabla en SQL Server
        table_name = 'TB_MARCA_RECIBO'
        
        # Borrar los datos existentes en la tabla
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        
        # Realizar el bulk insert
        cursor.fast_executemany = True
        insert_query = f"INSERT INTO {table_name} ({', '.join(data[0].keys())}) VALUES ({', '.join(['?'] * len(data[0]))})"
        cursor.executemany(insert_query, [tuple(row.values()) for row in data])
        
        # Confirmar la transacción
        conn.commit()
        
        print("Datos borrados y nuevos datos insertados exitosamente en la tabla existente.")
    except Exception as e:
        # Revertir la transacción en caso de error
        conn.rollback()
        print("Se produjo un error. Se revirtió la transacción.")
        print("Error:", str(e))
    finally:
        # Restaurar la configuración de autocommit y cerrar la conexión
        conn.autocommit = True

# Tiempo final de ejecución
fin  = time.time()

# Calcula la duración de la ejecución en segundos
duracion = fin - inicio

# Imprime la duración en segundos
print("Tiempo de ejecución:", duracion, "segundos")

# Cerrar la conexión a SQL Server
conn.close()