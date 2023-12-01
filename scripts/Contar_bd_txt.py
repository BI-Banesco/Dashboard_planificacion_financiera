import pandas as pd
from sqlalchemy import create_engine
import time

# Tiempo inicial de ejecución
inicio = time.time()

# Ruta del archivo CSV
txt_file = 'C:\\Users\\diego\\OneDrive\\Documents\\test_primas_siniestro\\01_Tabla_de_Primas_y_Siniestros_BSV_09_2023.txt'

# Leer los primeros 100,000 registros del archivo CSV utilizando Pandas
dataframe = pd.read_csv(txt_file, delimiter='|')

# Contar el número de filas procesadas
num_filas = len(dataframe) - 60

# Tiempo final de ejecución
fin = time.time()

# Calcula la duración de la ejecución en segundos
duracion = fin - inicio

# Imprime el número de filas procesadas
print('Archivos procesados: 01_Tabla_de_Primas_y_Siniestros_BSV_09_2023.txt')

# Imprime el número de filas procesadas
print('Número de filas procesadas:', num_filas)

# Imprime la duración en segundos
print('Tiempo de ejecución:', duracion, 'segundos')