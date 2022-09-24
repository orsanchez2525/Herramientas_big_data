import numpy as np
import argparse

print('----------------------------------------')

print('Porgrama para calcular valores')

print('----------------------------------------')
#funcion realiza la suma
def calcular_suma(lista_numeros):
    print('Estoy en la funcion suma')
    """calcula la suma de una lista de numeros


    Args:
        lista_numeros (lista): lista de numeros con valores enteros

    Returns:
        int: suma
    """

    resultado  =np.sum(lista_numeros)
    print('suma:',resultado)
    return resultado



#Valores_centrales
def calcular_valores_centrales(lista_numeros,verbose=0):

    """calcula el valor maximo y minimo de una lista

    Args:
        lista_numeros (lista): lista de numeros, valores enteros

    Returns:
        tuple: (media,desv_std)
        """
    if verbose==1:
       print('----------------------------------------')
       print('Estoy en la funcion de valores centrales')
    else:
       pass

    media    =np.mean(lista_numeros)
    desv_std =np.std(lista_numeros)
    print('        ')
    print('Estoy en la funcion valores centrales, media-->',media,'Desv_estandar-->',desv_std)
    print('----------------------------------------')
    return   media,desv_std

def calcular_valores_extremos(lista_numeros):
    """calcula el vamor minimo y maximo de una lista de numeros

    Args:
        lista_numeros (lista): lista de numeros con valores enteros

    Returns:
        tuple:(minimo, maximo)"""
    maximo  =np.max(lista_numeros)
    minimo  =np.min(lista_numeros)
    return  maximo,minimo



def  calcular_valores(lista_numeros):
    suma           =calcular_suma(lista_numeros)
    media,desv_std =calcular_valores_centrales(lista_numeros)
    maximo,minimo  =calcular_valores_extremos(lista_numeros)
    return suma,media,desv_std,maximo,minimo


def main():
    parser = argparse.ArgumentParser()#argumentos del script
    parser.add_argument("--verbose", type=int,default=1,help="para decidor si imprime o no")
    args=parser.parse_args()
    verbose=args.verbose

    lista_numeros = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    suma,media,desv_std,maximo,minimo=calcular_valores(lista_numeros)
    

if __name__=='__main__':
    main()