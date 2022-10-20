
# importar librerias internas del sistema
import os
import pandas as pd
import numpy  as np
from pathlib import Path
from google.cloud import storage


def get_data(filename):
    """Metodo que importa un archivo .csv y lo guarda en un dataframe

    Args:
        filename (string): Nombre del archivo a cargar

    Returns:
        dataframe: El dataframe donde se cargo el archivo
    """
    datos=pd.read_csv(f"gs://orsanchez_llamadas_123/data/raw/{filename}",sep=";",encoding="latin-1")
    print("------------------")
    print(f"Filename: {filename}")
    print(datos.shape[0],datos.shape[1])
    print(datos.columns)
    print("------------------")
    return datos


def normalizeNamesColumns(df):
    """Metodo que unifica los nombres de las columnas de los dataframe

    Args:
        datos (dataframe): El dataframe en donde se cargo cada uno de los archivos

    Returns:
        df: El dataframe donde se cargo el archivo con las columnas unificadas
    """
    if df.shape[1] == 10:
        df.columns = ['NUMERO_INCIDENTE', 'FECHA_INICIO_DESPLAZAMIENTO_MOVIL', 'CODIGO_LOCALIDAD', 'LOCALIDAD',	'EDAD',	'UNIDAD', 'GENERO',	'RED','TIPO_INCIDENTE',	'PRIORIDAD']
    else: 
        df.columns = ['NUMERO_INCIDENTE', 'FECHA_INICIO_DESPLAZAMIENTO_MOVIL', 'CODIGO_LOCALIDAD', 'LOCALIDAD',	'EDAD',	'UNIDAD', 'GENERO',	'RED','TIPO_INCIDENTE',	'PRIORIDAD', 'RECEPCIÓN']
    return df



   
def drop_duplicate(datos):
    """Metodo que borra los duplicados del dataframe

    Args:
        datos (dataframe): Ingresa el data frame

    Returns:
        dataframe: El dataframe sin los datos duplicados
    """
    df=datos.drop_duplicates()
    df.reset_index(inplace=True)#Sobreescribe en el espacio de memoria
    df.drop(columns='index',inplace=True)
    #df.drop(columns='index',inplace=True,drop=True)
    print('forma sin duplicados',df.shape)
    return df
    #df.info()


def change_nulls(df):
    """Metodo que cambia los datos nulos cuando las columnas son de tipo de datos object

    Args:
        df (dataframe): Ingresa el dataframe a analizar o quitar duplicados

    Returns:
        dataframe: dataframe sin nulos
    """
    for col in df.columns:
        print('\n') 
        print('-------  dtype ------')
        print(df[col].dtype)
        print('-------  ----- ------') 
        if df[col].dtype == 'object':
            df[col].fillna('SIN_DATO',inplace=True)
            #print("object")
            #print(type(df[col])) 
        else:
             print("otro")
    return df 

def change_type(df):
    """Metodo que cambia el tipo de dato cuando es una fecha

    Args:
        df (dataframe): data frame al cual se le va a trasnformar el tipo de dato de fecha

    Returns:
        dataframe: dataframe con el tipo de dato fecha
    """
    for col in df.columns:
        if 'FECHA'.lower() in col.lower():
            df[col] = pd.to_datetime(df[col],errors='coerce')
    return df

    

def showAmountNAPerColumn(columns):

  for i in columns.columns:
    print(f'Porcentaje de datos NAN en "{i}" es: {(columns[i].isnull().sum()/columns.shape[0])*100}%')


      
def generate_file(df,file_name):
    """Metodo para guardar el archivo

    Args:
        df (dataframe): dataframe que voy a guardar en el archivo
        file_name (string): nombre del archivo que se cargo inicialmente
    """
    out_name='reporte_limpieza_' + file_name
    df.to_csv(f'gs://orsanchez_llamadas_123/data/processed/{out_name}',encoding="latin-1",index = False)


def main ():
    storage_client = storage.Client()
    bucket_name = 'orsanchez_llamadas_123'
    blobs = storage_client.list_blobs(bucket_name)

    filenames = []
    for blob in blobs:
        if 'raw' in blob.name:
            filenames.append(blob.name[9:])
    
    dataframes = []
    for filename in filenames[1:]:
        datos = get_data(filename)
        showAmountNAPerColumn(datos)
        datos = normalizeNamesColumns(datos)
        datos = drop_duplicate(datos)
        datos = change_nulls(datos)
        datos = change_type(datos)
        dataframes.append(datos)
    print('dataframes', dataframes)
    fullData = pd.concat(dataframes)
    print(f'Tamaño del fullData:{len(fullData)}')
    fullData = fullData.reset_index(drop=True)
    fullData= change_nulls(fullData)
    #fullData['UNIDAD'] = np.where(fullData['UNIDAD'] == 'A¤os', 'AÑOS', fullData['UNIDAD'])
    generate_file(fullData, "datos_consolidados_v3.csv")
   

if __name__=='__main__':
    main()
