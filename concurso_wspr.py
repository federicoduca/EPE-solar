
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
    print('### Iniciando evaluador de competencia WSPR ####')

    print()
    print('Cargando configuracion del torneo:')

    # Nos aseguramos de estar en el directorio del script
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    # Cargamos el archivo YAML con la configuracion
    PATH_YAML = 'concurso_wspr.yaml'
    with open(PATH_YAML, 'r') as stream:
            config = yaml.load(stream, yaml.SafeLoader)

    # asignamos sobre variables que quedaran como constantes
    PATH_PARTICIPANTES  = config['files']['path_participantes']
    PATH_LOGS           = config['files']['path_logs']
    PATH_RESULTADOS     = config['files']['path_resultados']
    PATH_REGISTROS      = config['files']['path_registros']
    CHUNK_SIZE          = config['files']['chunk_size']

    FI = config['concurso']['fi']
    FF = config['concurso']['ff']
    ENTIDADES = config['concurso']['entidades']
    BANDAS = config['concurso']['bandas']

    print()
    print(f'Concurso:')
    print(f' > Abierto desde {FI} hasta {FF}')
    print(f' > Entidades: ')
    for entidad in ENTIDADES:
        print(f'   - {entidad}')
    print(f' > Bandas / Puntaje:')
    for b, p in BANDAS.items():
        print(f'   - {b}: {p}')
    
    # obtenemos el listado de participantes de WSPR
    df_ham = pd.read_csv(PATH_PARTICIPANTES)
    participantes = df_ham[df_ham['modo']=='WSPR']['call_sign'].tolist()
    del df_ham

    # creamos el concurso
    concurso = Concurso(FI,FF,ENTIDADES,BANDAS,participantes)

    print()
    print('Registros validos del concurso... ',end='')

    # creamos el dataframe con el log filtrado del concurso
    if not os.path.isfile(PATH_REGISTROS):

        print('no encontrados')
        
        # abrimos el log de contactos wspr en partes
        wspr_names = ['spot_id','time','reporter','reporter_grid','snr','freq','call_sign','call_sign_grid','power','drift','distance','azimuth','band','version','code']
        wspr_usecols = ['time','reporter','call_sign','band']
        dfs_log = pd.read_csv(PATH_LOGS, chunksize=CHUNK_SIZE, names=wspr_names, usecols=wspr_usecols)
        
        # aca guardaremos los logs validos
        df_log_conquest = pd.DataFrame(columns=wspr_usecols)

        print('Cargando logs en partes')
        for chunk, df_log in enumerate(dfs_log):

            print()
            print(f'Procesando chunk numero: {chunk}')
            
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
                
                # chequeamos por participantes
                partici_rep_check = df_log['reporter'].apply(concurso.chequeo_participante)
                partici_sd_check = df_log['call_sign'].apply(concurso.chequeo_participante)
                
                # filtramos solo los comunicados validos
                ## p_p -> reporter: participante y call_sign: participante
                ## p_r -> reporter: participante y call_sign: alguien de la region
                ## r_p -> reporter: alguien de la region y call_sign: participante
                # reporter escucha, y call_sign transmite
                registro_p_p = partici_rep_check & partici_sd_check
                registro_p_r = partici_rep_check & entidad_sd_check
                registro_r_p = entidad_rep_check & partici_sd_check

                # filtramos registros que cumpla con alguno de los criterios participante/region
                df_log = df_log[registro_p_p | registro_p_r | registro_r_p]
                print(f' > Registros válidos por participante / región: {df_log.shape[0]}')

                ## si quedo algo 
                if not df_log.empty:

                    # filtramos por banda
                    df_log['band'] = df_log['band'].apply(concurso.convertir_band_a_banda_m)
                    banda_check = df_log['band'].apply(concurso.chequeo_banda)

                    df_log = df_log[banda_check]
                    print(f' > Nuevos registros validos: {df_log.shape[0]}')

                    ## si quedo algo lo guardamos
                    if not df_log.empty:
                        df_log_conquest = df_log_conquest.append(df_log, ignore_index=True)
           
        ## cambiamos el formato de la fecha a uno legible por humanos
        df_log_conquest['time'] = pd.to_datetime(df_log_conquest['time'], unit='s') 

        ## guardamos los contactos validos
        df_log_conquest.to_csv(PATH_REGISTROS,index=None)

    else: 
        print('encontrados!')
        df_log_conquest = pd.read_csv(PATH_REGISTROS)

    print('Procesando resultados')

    ## contactos por banda en recepción
    df_log_conquest['N_rx'] = df_log_conquest['time'].astype(str) + '-' +df_log_conquest['call_sign']

    df_rx = df_log_conquest.groupby(['reporter','band']).nunique()[['N_rx','time']]
    df_rx.reset_index(inplace=True)
    df_rx.rename(columns={'reporter':'sd','time':'H_rx'},inplace=True)
    df_rx.set_index(['sd','band'],inplace=True)

    ## contactos por banda en transmisión
    df_log_conquest['N_tx'] = df_log_conquest['time'].astype(str) + '-' +df_log_conquest['reporter']

    df_tx = df_log_conquest.groupby(['call_sign','band']).nunique()[['N_tx','time']]
    df_tx.reset_index(inplace=True)
    df_tx.rename(columns={'call_sign':'sd','time':'H_tx'},inplace=True)
    df_tx.set_index(['sd','band'],inplace=True)

    ## unimos resultados
    df_r = pd.concat([df_tx,df_rx],join='outer',axis=1).fillna(0)
    df_r.reset_index(inplace=True)

    ## filtramos finalmente a los que fueron concursantes
    df_r[df_r['sd'].isin(concurso.obtener_participantes())]
    
    ## guardamos resultados sin procesar con puntaje 
    df_r.to_excel(PATH_RESULTADOS,index=None)

    print('Gracias vuelva prontos')
    print()
