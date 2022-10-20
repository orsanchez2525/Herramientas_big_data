
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
    print('forma sin duplicados',df.shape)
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

def cleanData(fullData):
    fullData['UNIDAD'] = np.where(fullData['UNIDAD'] == 'A¤os', 'AÑOS', fullData['UNIDAD'])
    fullData['UNIDAD'] = np.where(fullData['UNIDAD'] == 'AÂ¤os', 'AÑOS', fullData['UNIDAD'])
    fullData['PRIORIDAD'] = np.where(fullData['PRIORIDAD'] == 'CRITCA', 'CRITICA', fullData['PRIORIDAD'])
    fullData['TIPO_INCIDENTE'] = np.where(fullData['TIPO_INCIDENTE'] == 'Accidente de Aviaci¢n', 'Accidente de Aviación', fullData['TIPO_INCIDENTE'])
    fullData['TIPO_INCIDENTE'] = np.where(fullData['TIPO_INCIDENTE'] == 'Acompa¤amiento Evento', 'Acompañamiento Evento', fullData['TIPO_INCIDENTE'])
    fullData['TIPO_INCIDENTE'] = np.where(fullData['TIPO_INCIDENTE'] == 'AcompaÂ¤amiento Evento', 'Acompañamiento Evento', fullData['TIPO_INCIDENTE'])
    fullData['TIPO_INCIDENTE'] = np.where(fullData['TIPO_INCIDENTE'] == 'Ca¡da de Altura', 'Caída de Altura', fullData['TIPO_INCIDENTE'])
    fullData['TIPO_INCIDENTE'] = np.where(fullData['TIPO_INCIDENTE'] == 'CaÂ¡da de Altura', 'Caída de Altura', fullData['TIPO_INCIDENTE'])
    fullData['TIPO_INCIDENTE'] = np.where(fullData['TIPO_INCIDENTE'] == 'Convulsi¢n', 'Convulsión', fullData['TIPO_INCIDENTE'])
    fullData['TIPO_INCIDENTE'] = np.where(fullData['TIPO_INCIDENTE'] == 'ConvulsiÂ¢n', 'Convulsión', fullData['TIPO_INCIDENTE'])
    fullData['TIPO_INCIDENTE'] = np.where(fullData['TIPO_INCIDENTE'] == 'Dolor Tor cico', 'Dolor Torácico', fullData['TIPO_INCIDENTE'])
    fullData['TIPO_INCIDENTE'] = np.where(fullData['TIPO_INCIDENTE'] == 'Dolor TorÂ cico', 'Dolor Torácico', fullData['TIPO_INCIDENTE'])
    fullData['TIPO_INCIDENTE'] = np.where(fullData['TIPO_INCIDENTE'] == 'Electrocuci¢n / rescate', 'Electrocución / rescate', fullData['TIPO_INCIDENTE'])
    fullData['TIPO_INCIDENTE'] = np.where(fullData['TIPO_INCIDENTE'] == 'ElectrocuciÂ¢n / rescate', 'Electrocución / rescate', fullData['TIPO_INCIDENTE'])
    fullData['TIPO_INCIDENTE'] = np.where(fullData['TIPO_INCIDENTE'] == 'Intoxicaci¢n', 'Intoxicación', fullData['TIPO_INCIDENTE'])
    fullData['TIPO_INCIDENTE'] = np.where(fullData['TIPO_INCIDENTE'] == 'IntoxicaciÂ¢n', 'Intoxicación', fullData['TIPO_INCIDENTE'])
    fullData['TIPO_INCIDENTE'] = np.where(fullData['TIPO_INCIDENTE'] == 'Patolog¡a Ginecobst‚trica ', 'Patología Ginecobstétrica', fullData['TIPO_INCIDENTE'])
    fullData['TIPO_INCIDENTE'] = np.where(fullData['TIPO_INCIDENTE'] == 'PatologÂ¡a Ginecobstâ€štrica ', 'Patología Ginecobstétrica', fullData['TIPO_INCIDENTE'])
    fullData['TIPO_INCIDENTE'] = np.where(fullData['TIPO_INCIDENTE'] == 'S¡ntomas Gastrointestinales', 'Síntomas Gastrointestinales', fullData['TIPO_INCIDENTE'])

    
    fullData['LOCALIDAD'] = np.where(fullData['LOCALIDAD'] == 'Antonio Nari¤o', 'Antonio Nariño', 
np.where(fullData['LOCALIDAD'] == 'Antonio NariÂ¤o', 'Antonio Nariño', 
np.where(fullData['LOCALIDAD'] == 'Ciudad Bol¡var', 'Ciudad Bolívar', 
np.where(fullData['LOCALIDAD'] == 'Ciudad BolÂ¡var', 'Ciudad Bolívar', 
np.where(fullData['LOCALIDAD'] == 'Engativ ', 'Engativá', 
np.where(fullData['LOCALIDAD'] == 'EngativÂ ', 'Engativá', 
np.where(fullData['LOCALIDAD'] == 'Fontib¢n', 'Fontibón', 
np.where(fullData['LOCALIDAD'] == 'FontibÂ¢n', 'Fontibón',
np.where(fullData['LOCALIDAD'] == 'Los M rtires', 'Los Mártires', 
np.where(fullData['LOCALIDAD'] == 'Los MÂ rtires', 'Los Mártires', 
np.where(fullData['LOCALIDAD'] == 'San Crist¢bal', 'San Cristóbal', 
np.where(fullData['LOCALIDAD'] == 'San CristÂ¢bal', 'San Cristóbal', 
np.where(fullData['LOCALIDAD'] == 'Usaqu‚n', 'Usaquén', 
np.where(fullData['LOCALIDAD'] == 'Usaquâ€šn', 'Usaquén', 
np.where(fullData['LOCALIDAD'] == 'Usaquân', 'Usaquén', fullData['LOCALIDAD'])))))))))))))))


    fullData['UNIDAD'] = fullData['UNIDAD'].map({'AÑOS':'AÑOS', 
                                     'SIN_DATO': 'SIN_DATO', 
                                     'Años':'AÑOS', 
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
    
def showAllDataFromDataset(df, entries):
  for i in df[entries].columns:
    print(f' 🚀 | {i} - types: {df[i].nunique()}')
    print((df[i].value_counts()))
    print('-------------------------------')


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
    print('dataframes', dataframes)
    fullData = pd.concat(dataframes)
    print(f'Tamaño del fullData:{len(fullData)}')
    fullData = fullData.reset_index(drop=True)
    fullData = change_nulls(fullData)
    print('😁 Antes ----------------')
    showAllDataFromDataset(fullData, ['LOCALIDAD', 'GENERO', 'UNIDAD','EDAD'])
    fullData = cleanData(fullData)
    print('😁 Ahora ----------------')
    showAllDataFromDataset(fullData, ['LOCALIDAD', 'GENERO', 'UNIDAD','EDAD'])
    generate_file(fullData, "datos_consolidados_v5.csv")
   

if __name__=='__main__':
    main()
