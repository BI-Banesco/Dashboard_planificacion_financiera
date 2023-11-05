import json

def obtener_estructura_tablas(path_archivo_pbit):
    with open(path_archivo_pbit, 'r', encoding='latin-1') as archivo:
        contenido_json = archivo.read()
    
    datos = json.loads(contenido_json)
    
    # Obtener la estructura de las tablas y columnas
    model = datos['model']
    tablas = model['tables']
    
    for tabla in tablas:
        nombre_tabla = tabla['name']
        columnas = tabla['columns']
        
        print(f"Tabla: {nombre_tabla}")
        print("Columnas:")
        for columna in columnas:
            nombre_columna = columna['name']
            tipo_dato = columna['dataType']
            print(f"\t{nombre_columna}: {tipo_dato}")
        
        print("\n")

# Ruta del archivo PBIT
ruta_archivo_pbit = 'C:\\Users\\diego\\OneDrive\\Documents\\Python\\metadata_dashboard.pbit'

# Llamar a la función para obtener la estructura de las tablas
obtener_estructura_tablas(ruta_archivo_pbit)



