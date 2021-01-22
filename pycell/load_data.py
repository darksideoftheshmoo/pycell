#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 15 12:03:09 2020

@author: jose
"""
#Script complemento para cellID.
#Requiere librerias python standard pandas, os, re.
#Bebe pasarse como parámetro la ruta generada por cellID. 
#El programa recorrerá las subcarpetas.
#Toma de las tabalas de salida cellID (out_all, out_bf_fl_mapping) por position.
#Creará una subcarpeta pydata/df (opcional).
#Salida: único DataFrame con los valores de cada tabla. Agregará las series:
  #df['ucid'] identificador de célula por posición. int(). Unic Cell ID = ucid
  #df['pos'] identificador de posición de adquisición. int()
  #Para los valores de fluorescencia mapeados en out_bf_fl_mapping(df_mapp)
   #se crearran tantas series como flags en df_mapp multiplicado por la cantidad
   #de variables morfológicas df['f_tot_x1fp','f_tot_x2fp',..., 'f_tot_xnfp']
    
import os
import pandas as pd
import re


#Procesamiento de tablas
def get_dataframe(file):
    '''
    Devuelve un Dataframe.
    file = ruta al texto plano (formato tabla).
    Elimina las delimitaciones por espacio de headers.
    '''
    df = pd.read_table(file)
    #Elimino los espacios en los nombres de las columnas ' x.pos '. 
    df.columns = df.columns.str.strip()
    #Cambio (. por _) las separaciones x.pos por x_pos
    df.columns = df.columns.str.replace('.', '_')
    return df

def get_ucid(df, pos):
    '''
    Crea una columna en el dataframe (df) con número de tracking
    df[ucid].loc[0] = 100000000000 para cellID = 0 Position = 1
    ucid = int(numberPosition + cellID)
    df = df creado por cellID contiene la serie df['cellID']
    '''
    df['ucid'] = [pos * 100000000000 + cellID for cellID in df['cellID']]
    return df
 
def get_chanel(df_mapping, flag):
    '''
    Devuelve str(chanel) correspondiente al int(flag)
    pre: recibe un DataFrame de mapeo, df_mapping, con series ['flag']=int()
         y ['fluor']=str(path_file) (ver cellID doc). 
    pos: str('chanel') para un int(flag)
    '''
    #La escritura del cellID tiene la siguiente expresión regular
     #De 2 a tres caractes xFP luego  _Position
    chanel = re.compile(r'\w{2,3}_Position')
    #cellID codifica en la columna 'fluor'(ruta_archivo contiene str('chanel'))
     #Filtro el DataFrame  para la coincidencia falg == fluor
    path = df_mapping[df_mapping['flag'] == flag]['fluor'].values[0]
    return chanel.findall(path)[0].split('_')[0].lower()

def get_col_chan(df, df_map):
    '''
    Modifica la entrada df proviniente del pipeline pyCell. 
    Separa las series (columnas) morfológicas por canal de fluorecsencia
    pre: df = Tabla cellID contiendo df['ucid']
         df_map = Tabla mapping cellID (out_bf_fl_mapping)
    pos: Crea serias morfologicas por canal df['f_tot_yfp',...,'f_nuc_bfp',...] 
         Elimina los valores redundandes de cellID y la serie 'flag'.
    '''
    #Mensaje
    print('Agragando columnas chanles ...')
    
    #Variables de fluorescencia
    fluor  = [f_var for f_var in df.columns if f_var.startswith('f_')]
    #Creo un df con columnas variable_fluor por ucid y t_frame
    #idx = ['ucid', 't_frame'] if 't_frame' in df else idx = ['ucid']
    df_flag = df.pivot(index = ['ucid', 't_frame'] ,columns = 'flag', values= fluor)
    
    #Renombro columnas 
    #Obtengo todos los flag:chanel en mapping
    chanels = {flag:get_chanel(df_map, flag) for flag in df_map['flag'].unique()}
    #Col_name
    df_flag.columns = [n[0] + '_' + chanels[n[1]] for n in df_flag.columns]
    
    #Lista de variables morfologicas
    morf = [name for name in df.columns if not name.startswith('f_')]
    
    #Creo un df con las variables morfologicas
     #Elimino las redundancias creadas por cellID, registo un solo flag. 
    df_morf = df[df.flag == 0 ][morf]
    df_morf.set_index(['ucid', 't_frame'], inplace=True)
    #Junto los df_flag y df_morf
    df = pd.merge(df_morf, df_flag, on=['ucid', 't_frame'], how='outer')
    del df['flag']
    #Por congruencia con RCell
    #Indices numéricos. ucid, t_frama pasan a columnas
    df = df.reset_index()
    #Ordeno columnas compatible con marco de datos RCell
    col = ['pos', 't_frame', 'ucid', 'cellID']
    df = pd.concat([df[col],df.drop(col,axis=1)], axis=1)
    return df

# def get_serie_chanels(df, df_map): funcion eliminada, crea elemento por linea

def make_df(path_file):
    '''
    Crea un dataframe con numero de tracking ucid y position
    pre: path_file = nombre del archivo de salida cellID out_all
    pos: Devuelve un df del archivo pasado conteniendo df['ucid']
    '''
    #Position está codificada en el nombre del path al archivo.
    pos = int(re.findall("\d+", path_file)[0])
    print('leyendo position: ', pos)
    #Leo la tabla de texto plano.
    df = get_dataframe(path_file)
    #Asigno ucid
    df = get_ucid(df, pos)
    df['pos'] = [pos for _ in range(len(df))]
    return df

#%% #Navego direcctorios para obtener tablas

def get_outall_files(path):
    '''
    Devuelve una lista generadora con path de acceso a tablas 'out' de cellID.
    pre: path = carpeta que contiene las salidas cellID.
    pos: cambia el working directory.
    '''
    #Rutas a los archivos out_all, out_bf_fl_mapping de cellID
    for r, d, f in os.walk ( "." ):
        d.sort()
        for name in f:
            if 'out_all' in name:
                p = os.path. join (r, name)
                print(p)
                yield p

def get_mapp_files(path):
    '''
    Devuelve una lista generadora con path de acceso a tablas 'out' de cellID.
    pre: path = carpeta que contiene las salidas cellID.
    pos: cambia el working directory.
    '''
    #Rutas a los archivos out_all, out_bf_fl_mapping de cellID
    for r, d, f in os. walk ( "." ):
        d.sort()
        for name in f:
            if 'mapping' in name:
                yield os.path.join(r, name)
                
#%%
#Junto el pipeline compact_df
def read_cellidtable(path): #cambio load_df
    '''
    Devuelve único dataframe para las tablas out cellID
    path = ruta de acceso a las salida cellID
    '''
    #Me posiciono en el directorio a buscar.
    os.chdir(path)
    #creo un DataFrame vacío.
    df = pd.DataFrame()
    #Itero sobre la lista de archivos out.
    for f in get_outall_files(path):
        #Proceso las tablas de a una
        df_i = make_df(f)
        #Creo el DataFrame para mapear canales
        df_i =  get_col_chan(df_i, get_dataframe(get_mapp_files(path).__next__()))
        df = pd.concat([df, df_i], ignore_index=True)
    return df

#%% #Opcional, crear directrio pydata y de guardar tabla

def save_df (df):
    '''Crea una carpeda /pydata
    guarda el parámetro df DataFrame'''
    #Mensaje de entrada
    print('\nguardando archivo...')
    dir_name = 'pydata'
    #Me fijo si no existe el directorio
    if not os.path.exists(dir_name):
        os.mkdir(dir_name)# si no existe lo creo
    os.chdir(dir_name)
    #gruado csv
    df.to_csv('df.csv', index= False)
    return print('se guardó el archivo df')

#%% #Programa principal y consola

def main(argv):
    try:
        if len(argv) != 2:
            raise SystemExit (f'\nUso adecuado: {sys.argv[0]}'
                                ' ' 'path salida de cellID')
        df = read_cellidtable(argv[1])
        
        guardar = input('¿Decea guardar DataFrame? S/N ')
        if 's' in guardar.lower():
            save_df(df)
        
    except SystemExit as e:
        print(e)
        path = input('\ningrege path de acceso a salida cellID\n')
        
        df = read_cellidtable(path)
        
        guardar = input('¿Decea crear la carpeta pydata y guardar DataFrame? S/N ')
        if 's' in guardar.lower():
            save_df(df)

if __name__ == '__main__':
    import sys
    main(sys.argv)
