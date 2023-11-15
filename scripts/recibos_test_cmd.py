import pandas as pd
import numpy as np

# Ruta del archivo CSV
csv_file = 'C:\\Users\\diego\\OneDrive\\Documents\\Python\\data.csv'

# Leer el archivo CSV utilizando pandas
dataframe = pd.read_csv(csv_file)

# Imprimir la cantidad real de registros
num_registros = dataframe.shape[0]
print("Cantidad real de registros:", num_registros)