# -*- coding: utf-8 -*-

#Modulo de importación de tablas PyCell
import pycell.load_data as ld
from pycell.load_data import read_cellidtable

import matplotlib.image as mpimg
import matplotlib.pyplot as plt


#%%
# Pruebas de import modulo load_data
path_c2 ='/home/jose/Documentos/Mio/Trabajo/CONICET/IFIBYNE/Grupos_de_Investigacion/ACL/Andy/Micro/2019-10-21_Swi6_k_YPP5932_time_course'
df2 = read_cellidtable(path_c2)

#Experimento
# 12 t_frames, 16 posiciones (365792 filas 60 columnas)
path_c2 ='/home/jose/Documentos/Mio/Trabajo/CONICET/IFIBYNE/Grupos_de_Investigacion/ACL/Andy/Micro/2019-10-21_Swi6_k_YPP5932_time_course'
df2 = read_cellidtable(path_c2)
#save_df(load_df(path_c2))

#Carpeta de prueba 
# 12 t_frames, 3 posiciones (72756 filas y 60 columnas)
path_c= '/home/jose/Documentos/Mio/Trabajo/CONICET/IFIBYNE/Grupos_de_Investigacion/ACL/proyecto_python/muestras_cellid'
df = ld.read_cellidtable(path_c)


df_tabla = ld.get_dataframe(path_c + '/Position01/out_all')
#df_tabla = ld.get_ucid(df_tabla, 1)

#df_mapping = get_dataframe(path_c + '/Position01/out_bf_fl_mapping')

#ch = get_chanel(df_mapping, 1)

#d = make_df(path_c + '/Position01/out_all')

#%%
# img
# Cargar en memoria las imagenes obtenidas por cellID

x_pos = 9
y_pos = 424

img = mpimg.imread(path_c + '/RFP_Position03_time06.tif.out.tif')

imgplot = plt.imshow(img[(y_pos - 12):(y_pos + 10), (x_pos - 5):(x_pos + 20)])

