import pandas as pd

# Ruta del archivo TXT
txt_file = 'C:\\Users\\diego\\OneDrive\\Documents\\test_primas_siniestro\\01_Tabla_de_Primas_y_Siniestros_BSV_09_2023.txt'

# Leer el archivo de texto utilizando Pandas
df = pd.read_csv(txt_file, delimiter='|')

columnas_eliminar = ['CD_LINEA_NEGOCIO']
df = df.drop(columnas_eliminar, axis=1)

# Ajustar la configuración de visualización
pd.set_option('display.max_columns', None)  # Mostrar todas las columnas

# Imprimir el DataFrame
print(df)