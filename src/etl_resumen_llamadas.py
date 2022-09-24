
# importar librerias internas del sistema
import os
import pandas as pd
from pathlib import Path
  

def get_data(filename):
    """Importa un archivo .csv y lo guarda en un dataframe

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
    df=datos.drop_duplicates()
    df.reset_index(inplace=True)#Sobreescribe en el espacio de memoria
    df.drop(columns='index',inplace=True)
    #df.drop(columns='index',inplace=True,drop=True)
    print('forma sin duplicados',df.shape)
    return df
    #df.info()


def change_nulls(df):
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
    for col in df.columns:
        if 'FECHA'.lower() in col.lower():
            df[col] = pd.to_datetime(df[col],errors='coerce')
    return df

    

def showAmountNAPerColumn(columns):
  for i in columns.columns:
    print(f'Porcentaje de datos NAN en "{i}" es: {(columns[i].isnull().sum()/columns.shape[0])*100}%')


      
def generate_file(df,file_name):
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
