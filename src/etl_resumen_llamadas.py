
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
        df.columns = ['NUMERO_INCIDENTE', 'FECHA_INICIO_DESPLAZAMIENTO_MOVIL', 'CODIGO_LOCALIDAD', 'LOCALIDAD',	'EDAD',	'UNIDAD', 'GENERO',	'RED','TIPO_INCIDENTE',	'PRIORIDAD', 'RECEPCION']
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
    return df



def change_nulls(df):
    """Metodo que cambia los datos nulos cuando las columnas son de tipo de datos object

    Args:
        df (dataframe): Ingresa el dataframe a analizar o quitar duplicados

    Returns:
        dataframe: dataframe sin nulos
    """
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col].fillna('SIN_DATO',inplace=True)
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
    df.to_csv(f'gs://orsanchez_llamadas_123/data/processed/{out_name}', index = False)
    
    table_name='Llamadas_123_Oscar_Sanchez.Llamadas_123'
    df.to_gbq(table_name,if_exists='replace')

def cleanData(fullData):
    fullData['UNIDAD'] = np.where(fullData['UNIDAD'] == 'A??os', 'A??OS', fullData['UNIDAD'])
    fullData['UNIDAD'] = np.where(fullData['UNIDAD'] == 'A????os', 'A??OS', fullData['UNIDAD'])
    fullData['PRIORIDAD'] = np.where(fullData['PRIORIDAD'] == 'CRITCA', 'CRITICA', fullData['PRIORIDAD'])
    fullData['TIPO_INCIDENTE'] = np.where(fullData['TIPO_INCIDENTE'] == 'Accidente de Aviaci??n', 'Accidente de Aviaci??n', fullData['TIPO_INCIDENTE'])
    fullData['TIPO_INCIDENTE'] = np.where(fullData['TIPO_INCIDENTE'] == 'Acompa??amiento Evento', 'Acompa??amiento Evento', fullData['TIPO_INCIDENTE'])
    fullData['TIPO_INCIDENTE'] = np.where(fullData['TIPO_INCIDENTE'] == 'Acompa????amiento Evento', 'Acompa??amiento Evento', fullData['TIPO_INCIDENTE'])
    fullData['TIPO_INCIDENTE'] = np.where(fullData['TIPO_INCIDENTE'] == 'Ca??da de Altura', 'Ca??da de Altura', fullData['TIPO_INCIDENTE'])
    fullData['TIPO_INCIDENTE'] = np.where(fullData['TIPO_INCIDENTE'] == 'Ca????da de Altura', 'Ca??da de Altura', fullData['TIPO_INCIDENTE'])
    fullData['TIPO_INCIDENTE'] = np.where(fullData['TIPO_INCIDENTE'] == 'Convulsi??n', 'Convulsi??n', fullData['TIPO_INCIDENTE'])
    fullData['TIPO_INCIDENTE'] = np.where(fullData['TIPO_INCIDENTE'] == 'Convulsi????n', 'Convulsi??n', fullData['TIPO_INCIDENTE'])
    fullData['TIPO_INCIDENTE'] = np.where(fullData['TIPO_INCIDENTE'] == 'Dolor Tor??cico', 'Dolor Tor??cico', fullData['TIPO_INCIDENTE'])
    fullData['TIPO_INCIDENTE'] = np.where(fullData['TIPO_INCIDENTE'] == 'Dolor Tor????cico', 'Dolor Tor??cico', fullData['TIPO_INCIDENTE'])
    fullData['TIPO_INCIDENTE'] = np.where(fullData['TIPO_INCIDENTE'] == 'Electrocuci??n / rescate', 'Electrocuci??n / rescate', fullData['TIPO_INCIDENTE'])
    fullData['TIPO_INCIDENTE'] = np.where(fullData['TIPO_INCIDENTE'] == 'Electrocuci????n / rescate', 'Electrocuci??n / rescate', fullData['TIPO_INCIDENTE'])
    fullData['TIPO_INCIDENTE'] = np.where(fullData['TIPO_INCIDENTE'] == 'Intoxicaci??n', 'Intoxicaci??n', fullData['TIPO_INCIDENTE'])
    fullData['TIPO_INCIDENTE'] = np.where(fullData['TIPO_INCIDENTE'] == 'Intoxicaci????n', 'Intoxicaci??n', fullData['TIPO_INCIDENTE'])
    fullData['TIPO_INCIDENTE'] = np.where(fullData['TIPO_INCIDENTE'] == 'Patolog??a Ginecobst???trica ', 'Patolog??a Ginecobst??trica', fullData['TIPO_INCIDENTE'])
    fullData['TIPO_INCIDENTE'] = np.where(fullData['TIPO_INCIDENTE'] == 'Patolog????a Ginecobst???????trica ', 'Patolog??a Ginecobst??trica', fullData['TIPO_INCIDENTE'])
    fullData['TIPO_INCIDENTE'] = np.where(fullData['TIPO_INCIDENTE'] == 'S??ntomas Gastrointestinales', 'S??ntomas Gastrointestinales', fullData['TIPO_INCIDENTE'])

    
    fullData['LOCALIDAD'] = np.where(fullData['LOCALIDAD'] == 'Antonio Nari??o', 'Antonio Nari??o', 
np.where(fullData['LOCALIDAD'] == 'Antonio Nari????o', 'Antonio Nari??o', 
np.where(fullData['LOCALIDAD'] == 'Ciudad Bol??var', 'Ciudad Bol??var', 
np.where(fullData['LOCALIDAD'] == 'Ciudad Bol????var', 'Ciudad Bol??var', 
np.where(fullData['LOCALIDAD'] == 'Engativ??', 'Engativ??', 
np.where(fullData['LOCALIDAD'] == 'Engativ????', 'Engativ??', 
np.where(fullData['LOCALIDAD'] == 'Fontib??n', 'Fontib??n', 
np.where(fullData['LOCALIDAD'] == 'Fontib????n', 'Fontib??n',
np.where(fullData['LOCALIDAD'] == 'Los M??rtires', 'Los M??rtires', 
np.where(fullData['LOCALIDAD'] == 'Los M????rtires', 'Los M??rtires', 
np.where(fullData['LOCALIDAD'] == 'San Crist??bal', 'San Crist??bal', 
np.where(fullData['LOCALIDAD'] == 'San Crist????bal', 'San Crist??bal', 
np.where(fullData['LOCALIDAD'] == 'Usaqu???n', 'Usaqu??n', 
np.where(fullData['LOCALIDAD'] == 'Usaqu???????n', 'Usaqu??n', 
np.where(fullData['LOCALIDAD'] == 'Usaqu??n', 'Usaqu??n', fullData['LOCALIDAD'])))))))))))))))


    fullData['UNIDAD'] = fullData['UNIDAD'].map({'A??OS':'A??OS', 
                                     'SIN_DATO': 'SIN_DATO', 
                                     'A??os':'A??OS', 
                                     'Sin_dato': 'SIN_DATO',
                                     'Meses':'MESES', 
                                     'Dias': 'DIAS', 
                                     'Horas': 'HORAS', 
                                     'horas': 'HORAS'
                                     })
    fullData['GENERO'] = fullData['GENERO'].map({'SIN_DATO':'SIN_DATO', 
                                     'Sin_dato': 'SIN_DATO', 
                                     'Masculino':'MASCULINO', 
                                     'Femenino': 'FEMENINO',
                                     'MASCULINO':'MASCULINO', 
                                     'FEMENINO': 'FEMENINO'
                                     })
    fullData['EDAD'] = np.where(fullData['EDAD'] == 'SIN_DATO', '0', fullData['EDAD'])
    fullData['EDAD'] = pd.to_numeric(fullData['EDAD'] , errors='coerce')
    #fullData['EDAD'] = np.where(fullData['EDAD'] == 0, round(fullData['EDAD'].mean(),2), fullData['EDAD'])
    return fullData
        

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
        #showAmountNAPerColumn(datos)
        datos = normalizeNamesColumns(datos)
        datos = drop_duplicate(datos)
        datos = change_nulls(datos)
        datos = change_type(datos)
        dataframes.append(datos)
   # print('dataframes', dataframes)
    fullData = pd.concat(dataframes)
    #print(f'Tama??o del fullData:{len(fullData)}')
    fullData = fullData.reset_index(drop=True)
    fullData = change_nulls(fullData)
    fullData = cleanData(fullData)
    generate_file(fullData, "datos_consolidados_v5.csv")
   

if __name__=='__main__':
    main()
