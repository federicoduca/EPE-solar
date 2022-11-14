from helpers import Concurso
import pandas as pd
import numpy as np
import os


def formatear_contactos_validos(df_log, concurso):

    print()
    print("FORMATEANDO LOG ")
    print(f" > Comunicados encontrados: {df_log.shape[0]}")

    # UNIFICAMOS FECHA
    df_log["time_on_str"] = df_log["QSO_DATE"].astype(str) + df_log["TIME_ON"].astype(
        str
    )
    df_log["time_off_str"] = df_log["QSO_DATE"].astype(str) + df_log["TIME_OFF"].astype(
        str
    )

    # CREAMOS CAMPO TIMESTAMP
    df_log["time_on"] = pd.to_datetime(df_log["time_on_str"], format="%Y%m%d%H%M%S")
    df_log["time_off"] = pd.to_datetime(df_log["time_off_str"], format="%Y%m%d%H%M%S")

    del df_log["QSO_DATE"]
    del df_log["TIME_ON"]
    del df_log["TIME_OFF"]
    del df_log["time_on_str"]
    del df_log["time_off_str"]

    df_log["timestamp_on"] = df_log["time_on"].view(np.int64) // 10 ** 9
    df_log["timestamp_off"] = df_log["time_off"].view(np.int64) // 10 ** 9

    # creamos campos de chequeo de fecha
    time_on_check = df_log["timestamp_on"].apply(concurso.chequeo_fecha)
    time_off_check = df_log["timestamp_off"].apply(concurso.chequeo_fecha)

    # filtramos los que sean validos
    df_log = df_log[time_off_check & time_on_check]
    print(f" > Registros en fecha: {df_log.shape[0]}")

    # chequeamos por entidad
    entidad_rep_check = df_log["reporter"].apply(concurso.chequeo_region)
    entidad_sd_check = df_log["call_sign"].apply(concurso.chequeo_region)

    # chequeamos por participantes
    partici_rep_check = df_log["reporter"].apply(concurso.chequeo_participante)
    partici_sd_check = df_log["call_sign"].apply(concurso.chequeo_participante)

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

    print(f" > Registros válidos por participante / region: {df_log.shape[0]}")

    # filtramos por banda
    banda_check = df_log["band"].apply(concurso.chequeo_banda)
    df_log = df_log[banda_check]

    print(f" > Nuevos registros validos: {df_log.shape[0]}")

    return df_log


def cargar_logs(concurso):
    # buscamos los distintos logs:
    print("Buscando logs... ")

    log_usecols = [
        "BAND",
        "STATION_CALLSIGN",
        "CALL",
        "QSO_DATE",
        "TIME_ON",
        "TIME_OFF",
    ]
    convert_cols = {
        "BAND": "band",
        "CALL": "call_sign",
        "STATION_CALLSIGN": "reporter",
    }
    converters = {k: str for k, v in convert_cols.items()}

    df_log = pd.read_excel(
        concurso.PATH_LOGS, usecols=log_usecols, converters=converters
    )

    df_log.rename(columns=convert_cols, inplace=True)

    return df_log


def obtener_resultados(df_log, concurso):
    print()
    print("PROCESANDO RESULTADOS")

    df_log["N"] = df_log["time_on"].astype(str) + "-" + df_log["call_sign"]

    df_r = df_log.groupby(["reporter", "band"]).nunique()[["N", "time_on"]]
    df_r.reset_index(inplace=True)
    df_r.rename(columns={"reporter": "sd", "time_on": "H"}, inplace=True)

    df_r["H"] = df_r["H"] * 100 * (2 * 60) / (concurso.duracion_concurso())

    df_r["B"] = df_r["band"].apply(concurso.calcular_puntaje_bandas)

    ## filtramos finalmente a los que fueron concursantes
    df_r = df_r[df_r["sd"].isin(concurso.participantes)]

    return df_r


if __name__ == "__main__":

    modalidad = "ft8"

    # Instanciamos el concurso
    concurso = Concurso(modalidad)
    print(concurso)

    # cargamos todos los logs
    df_log = cargar_logs(concurso)

    ## filtramos contactos validos y formateamos
    df_log_val = formatear_contactos_validos(df_log, concurso)
    df_log_val.to_excel(concurso.PATH_REGISTROS, index=None)

    # guardamos resultados sin procesar con puntaje
    df_r = obtener_resultados(df_log_val, concurso)
    df_r.to_excel(concurso.PATH_RESULTADOS, index=None)

    print()
    print("Gracias vuelva prontos")
    print()
