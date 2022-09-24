
# importar librerias internas del sistema
import os
import pandas as pd
from pathlib import Path
  

def get_data(filename):
    """Metodo que importa un archivo .csv y lo guarda en un dataframe

    Args:
        filename (string): Nombre del archivo a cargar

    Returns:
        dataframe: El dataframe donde se cargo el archivo
    """
    data_dir="raw"
    root_dir=Path('.').resolve().parent
    file_path=os.path.join(root_dir,"data",data_dir,filename)
    datos=pd.read_csv(file_path,sep=";",encoding="latin-1")
    print('get_date')
    print(datos.shape[0],datos.shape[1])
    print(filename, type(filename))
    return datos
   


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
    #data_dir="raw"
    root_dir=Path('.').resolve().parent 
    out_name='reporte_limpieza_' + file_name
    out_path=os.path.join(root_dir,"data","processed",out_name)
    df.to_csv(out_path)

def main ():
    filename="llamadas123_julio_2022.csv"
    datos=get_data(filename)
    datos_sin_duplicados=drop_duplicate(datos)
    datos_sin_nulos=change_nulls(datos_sin_duplicados)
    #print(datos_sin_nulos)
    #print (datos_sin_nulos['UNIDAD'].value_counts(dropna=False))
    showAmountNAPerColumn(datos)
    change_type(datos_sin_nulos)
    print(datos_sin_nulos.dtypes)
    generate_file(datos_sin_nulos,filename)


if __name__=='__main__':
    main()
