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
csv_file = 'C:\\Users\\diego\\OneDrive\\Documents\\Python\\data.csv'

# Leer el archivo CSV utilizando pandas
dataframe = pd.read_csv(csv_file)

# Renombrar columnas si es necesario
dataframe = dataframe.rename(columns={"CD_RAMO_POL": "CD_RAMO", "CD_SUCURSAL_POL": "CD_SUCURSAL"})

# Eliminar las columnas especificadas
columnas_eliminar = ['CD_ST_RECIBO', 'Mes', 'POLIZAID_CERTIFID', 'Source.Name', 'TABLA', 'TP_POLIZA', 'SUSCRITO USD']
dataframe = dataframe.drop(columnas_eliminar, axis=1)

dataframe = dataframe.dropna(subset=['CD_RAMO', 'CD_SUCURSAL', 'NU_RECIBO'])

# Convertir las columnas a tipo int
dataframe['CD_RAMO'] = dataframe['CD_RAMO'].astype(int)
dataframe['CD_SUCURSAL'] = dataframe['CD_SUCURSAL'].astype(int)
dataframe['NU_RECIBO'] = dataframe['NU_RECIBO'].astype(int)

# Convertir los valores NaN a None
dataframe = dataframe.where(pd.notnull(dataframe), None)

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
        insert_query = f"INSERT INTO {table_name} ({', '.join(dataframe.columns)}) VALUES ({', '.join(['?'] * len(dataframe.columns))})"
        cursor.executemany(insert_query, dataframe.values.tolist())
        
        # Confirmar la transacción
        conn.commit()
        
        print("Datos borrados y nuevos datos insertados exitosamente en la tabla existente.")
    except Exception as e:
        # Revertir la transacción en caso de error
        conn.rollback()
        print("Se produjo un error. Se revirtió la transacción.")
        print("Error:", str(e))
    finally:
        conn.autocommit = True

        # Restaurar la configuración de autocommit y cerrar la conexión
        cursor.execute("EXEC EjemploPrint2")
        conn.commit()

# Tiempo final de ejecución
fin  = time.time()

# Calcula la duración de la ejecución en segundos
duracion = fin - inicio

# Imprime la duración en segundos
print("Tiempo de ejecución:", duracion, "segundos")

# Cerrar la conexión a SQL Server
conn.close()