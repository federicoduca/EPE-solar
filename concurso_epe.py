
import pandas as pd
import numpy as np

from datetime import datetime
import pytz
import re

import os
import yaml

from concurso import Concurso

if __name__ == '__main__':
    
    print()
    print('#### EVALUADOR COMPETENCIA WSPR ####')

    print()
    print('Cargando configuracion del torneo')

    # Nos aseguramos de estar en el directorio del script
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    # Cargamos el archivo YAML con la configuracion
    PATH_YAML = 'concurso_epe.yaml'
    with open(PATH_YAML, 'r') as stream:
            config = yaml.load(stream, yaml.SafeLoader)

    # asignamos sobre variables que quedaran como constantes
    PATH_PARTICIPANTES  = config['files']['path_participantes']
    PATH_LOGS           = config['files']['path_logs']
    PATH_REGISTROS      = config['files']['path_registros']
    CHUNK_SIZE          = config['files']['chunk_size']

    FI = config['concurso']['fi']
    FF = config['concurso']['ff']
    ENTIDADES = config['concurso']['entidades']

    print(f' > Duracion:')
    print(f'   - Desde {FI}')
    print(f'   - Hasta {FF}')
    print(f' > Entidades: ')
    for entidad in ENTIDADES:
        print(f'   - {entidad}')
    
    # creamos el concurso
    concurso = Concurso(FI,FF,ENTIDADES,{},[])

    print()
    print("Filtrando registros")

    # abrimos el log de contactos wspr en partes
    wspr_names = ['spot_id','time','reporter','reporter_grid','snr','freq','call_sign','call_sign_grid','power','drift','distance','azimuth','band','version','code']
    dfs_log = pd.read_csv(PATH_LOGS, chunksize=CHUNK_SIZE, names=wspr_names)
    df_log_wspr = pd.DataFrame(columns=wspr_names)

    # creamos el dataframe con el log filtrado del concurso
    if not os.path.isfile(PATH_REGISTROS):

        print('Cargando logs en partes')
        for chunk, df_log in enumerate(dfs_log):
            
            print()
            print(f'Procesando chunk numero: {chunk}')      
            
            if chunk >= 40:
                if chunk >= 46:
                    print("  > no' vimo' en disney")
                    break 
                
                if 'LU3BHO/M' in df_log['reporter']:
                    print("Acá estoy! 'LU3BHO/M'")
                if 'LU3BHO' in df_log['reporter']:
                    print("Acá estoy! 'LU3BHO'")
                
                ## creamos campos de chequeo de fecha 
                time_check = df_log['time'].apply(concurso.chequeo_fecha)

                # filtramos los que sean validos
                df_log = df_log[time_check]

                print(f' > Registros en fecha: {df_log.shape[0]}')

                ## si lo cumple
                if not df_log.empty:

                    # chequeamos por entidad
                    entidad_rep_check = df_log['reporter'].apply(concurso.chequeo_region)
                    entidad_sd_check = df_log['call_sign'].apply(concurso.chequeo_region)
                                        
                    # filtramos solo los comunicados de la región
                    # reporter escucha, y call_sign transmite
                    registro_r_r = entidad_rep_check & entidad_sd_check

                    # filtramos registros que cumpla con alguno de los criterios region
                    df_log = df_log[registro_r_r]
                    print(f' > Registros validos por region: {df_log.shape[0]}')

                    ## si quedo algo 
                    if not df_log.empty:
                        df_log_wspr = df_log_wspr.append(df_log, ignore_index=True)

    else:
        df_log_wspr = pd.read_excel(PATH_REGISTROS)

        
    ## cambiamos el formato de la fecha a uno legible por humanos
    df_log_wspr['fecha'] = pd.to_datetime(df_log_wspr['time'], unit='s') 

    ## guardamos los contactos validos
    df_log_wspr.to_excel(PATH_REGISTROS,index=None)

    print()
    print('Gracias vuelva prontos :)')
    print()
