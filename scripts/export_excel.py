import pandas as pd

# Ruta del archivo TXT
txt_file = 'C:\\Users\\diego\\OneDrive\\Documents\\test_primas_siniestro\\01_Tabla_de_Primas_y_Siniestros_BSV_09_2023.txt'

# Leer el archivo de texto utilizando Pandas
df = pd.read_csv(txt_file, delimiter='|')

columnas_eliminar = ['CD_LINEA_NEGOCIO']
df = df.drop(columnas_eliminar, axis=1)

# Especifica la ruta del archivo de salida Excel
excel_file = 'ruta_del_archivo.xlsx'

# Guarda el DataFrame en un archivo Excel
df.to_excel(excel_file, index=False)

print(f"El DataFrame se ha guardado en el archivo Excel: {excel_file}")