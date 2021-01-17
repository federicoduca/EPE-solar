
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
    print('### EVALUADOR COMPETENCIA FT8 ####')

    print()
    print('Cargando configuracion del torneo:')

    # Nos aseguramos de estar en el directorio del script
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    # Cargamos el archivo YAML con la configuracion
    PATH_YAML = 'concurso_ft8.yaml'
    with open(PATH_YAML, 'r') as stream:
            config = yaml.load(stream, yaml.SafeLoader)

    # asignamos sobre variables constantes
    PATH_PARTICIPANTES  = config['files']['path_participantes']
    PATH_LOG            = config['files']['path_log']
    PATH_RESULTADOS     = config['files']['path_resultados']
    PATH_REGISTROS      = config['files']['path_registros']
    
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
    # ham_usecols = ['call_sign','modo'] 
    df_ham = pd.read_csv(PATH_PARTICIPANTES)
    df_ham = df_ham[df_ham['modo']=='FT8']
    participantes = df_ham['call_sign'].tolist()
    del df_ham

    # creamos el concurso
    concurso = Concurso(FI,FF,ENTIDADES,BANDAS,participantes)

    # buscamos los distintos logs:    
    print()
    print("Buscando log... ")

    log_columns = ['CALL','GRIDSQUARE','MODE','RST_SENT','RST_RCVD','QSO_DATE','TIME_ON	TIME_OFF','BAND','FREQ','STATION_CALLSIGN','MY_GRIDSQUARE','TX_PWR','COMMENT','NAME','OPERATOR','SUBMODE','EQSL_QSL_SENT']
    log_usecols = ['BAND','STATION_CALLSIGN','CALL','QSO_DATE','TIME_ON','TIME_OFF']

    df_log = pd.read_excel(PATH_LOG,usecols=log_usecols)

    # creamos el df con todos los contactos validos
    ft8_usecols = ['time_on','time_off','reporter','call_sign','band']
    df_log_conquest = pd.DataFrame(columns=ft8_usecols)

    #######################################
    ############# FORMATEAMOS #############
    #######################################
    print()
    print("FORMATEANDO LOG ")
    print(f' > Comunicados encontrados: {df_log.shape[0]}')

    ############# UNIFICAMOS FECHA
    df_log['time_on_str']  = df_log['QSO_DATE'].astype(str) + df_log['TIME_ON'].astype(str)
    df_log['time_off_str'] = df_log['QSO_DATE'].astype(str) + df_log['TIME_OFF'].astype(str)

    # CREAMOS CAMPO TIMESTAMP
    df_log['time_on'] = pd.to_datetime(df_log['time_on_str'], format='%Y%m%d%H%M%S')
    df_log['time_off'] = pd.to_datetime(df_log['time_off_str'], format='%Y%m%d%H%M%S')

    del df_log['QSO_DATE']
    del df_log['TIME_ON']
    del df_log['TIME_OFF']
    del df_log['time_on_str']
    del df_log['time_off_str']

    df_log['timestamp_on'] = df_log['time_on'].astype(np.int64) // 10 ** 9
    df_log['timestamp_off'] = df_log['time_off'].astype(np.int64) // 10 ** 9

    ## creamos campos de chequeo de fecha 
    time_on_check = df_log['timestamp_on'].apply(concurso.chequeo_fecha)
    time_off_check = df_log['timestamp_off'].apply(concurso.chequeo_fecha)

    # filtramos los que sean validos
    df_log = df_log[time_off_check & time_on_check]
    
    print(f' > Registros en fecha: {df_log.shape[0]}')

    ######### modificamos nombre de entidades
    df_log['reporter'] = df_log['STATION_CALLSIGN'].astype(str)
    del df_log['STATION_CALLSIGN']
    df_log['call_sign'] = df_log['CALL'].astype(str)
    del df_log['CALL']

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

    print(f' > Registros válidos por participante / region: {df_log.shape[0]}')

    ######## filtramos por banda
    df_log['band'] = df_log['BAND'].astype(str)
    del df_log['BAND']
    
    banda_check = df_log['band'].apply(concurso.chequeo_banda)
    df_log = df_log[banda_check]

    print(f' > Nuevos registros validos: {df_log.shape[0]}')

    ## guardamos los contactos validos
    df_log.to_excel(PATH_REGISTROS,index=None)

    #######################################
    ############# RESULTADOS ##############
    #######################################
    
    print()
    print("PROCESANDO RESULTADOS")

    df_log['N'] = df_log['time_on'].astype(str) + '-' + df_log['call_sign']

    df_r = df_log.groupby(['reporter','band']).nunique()[['N','time_on']]
    df_r.reset_index(inplace=True)
    df_r.rename(columns={'reporter':'sd','time_on':'H'},inplace=True)

    df_r['H'] = df_r['H'] * 100 * (2*60) / (concurso.duracion_concurso()) 
    
    df_r['B'] = df_r['band'].apply(concurso.calcular_puntaje_bandas)

    ## filtramos finalmente a los que fueron concursantes
    df_r = df_r[df_r['sd'].isin(concurso.obtener_participantes())]
    
    ## guardamos resultados sin procesar con puntaje 
    df_r.to_excel(PATH_RESULTADOS,index=None)

    print()
    print('Gracias vuelva prontos')
    print()
