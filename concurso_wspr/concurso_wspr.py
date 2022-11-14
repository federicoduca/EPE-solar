from helpers import Concurso
import pandas as pd
import os


def cargar_logs_validos(concurso):

    print()
    print("Registros validos del concurso... ", end="")

    # creamos el dataframe con el log filtrado del concurso
    if os.path.isfile(concurso.PATH_REGISTROS):
        print("encontrados!")
        df_log_conquest = pd.read_csv(concurso.PATH_REGISTROS)
        return df_log_conquest

    print("no encontrados")
    # abrimos el log de contactos wspr en partes
    wspr_names = [
        "spot_id",
        "time",
        "reporter",
        "reporter_grid",
        "snr",
        "freq",
        "call_sign",
        "call_sign_grid",
        "power",
        "drift",
        "distance",
        "azimuth",
        "band",
        "version",
        "code",
    ]
    wspr_usecols = ["time", "reporter", "call_sign", "band"]
    dfs_log = pd.read_csv(
        concurso.PATH_LOGS,
        chunksize=concurso.CHUNK_SIZE,
        names=wspr_names,
        usecols=wspr_usecols,
    )

    # aca guardaremos los logs validos
    df_log_conquest = pd.DataFrame(columns=wspr_usecols)

    print("Cargando logs en partes")
    for chunk, df_log in enumerate(dfs_log):

        print()
        print(f"Procesando chunk numero: {chunk}")

        # creamos campos de chequeo de fecha
        time_check = df_log["time"].apply(concurso.chequeo_fecha)
        df_log = df_log[time_check]
        print(f" > Registros en fecha: {df_log.shape[0]}")

        # si filtramos todos los registros, pasamos a otro chunk
        if df_log.empty:
            continue

        # chequeamos por entidad
        entidad_rep_check = df_log["reporter"].apply(concurso.chequeo_region)
        entidad_sd_check = df_log["call_sign"].apply(concurso.chequeo_region)

        # chequeamos por participantes
        partici_rep_check = df_log["reporter"].apply(concurso.chequeo_participante)
        partici_sd_check = df_log["call_sign"].apply(concurso.chequeo_participante)

        # filtramos solo los comunicados validos
        # p_p -> reporter: participante y call_sign: participante
        # p_r -> reporter: participante y call_sign: alguien de la region
        # r_p -> reporter: alguien de la region y call_sign: participante
        # reporter escucha, y call_sign transmite
        registro_p_p = partici_rep_check & partici_sd_check
        registro_p_r = partici_rep_check & entidad_sd_check
        registro_r_p = entidad_rep_check & partici_sd_check

        # filtramos registros que cumpla con alguno de los criterios participante/region
        df_log = df_log[registro_p_p | registro_p_r | registro_r_p]
        print(f" > Registros validos por participante / region: {df_log.shape[0]}")

        # si filtramos todos los registros, pasamos a otro chunk
        if df_log.empty:
            continue

        # filtramos por banda
        df_log["band"] = df_log["band"].apply(concurso.convertir_band_a_banda_m)
        banda_check = df_log["band"].apply(concurso.chequeo_banda)

        df_log = df_log[banda_check]
        print(f" > Nuevos registros validos: {df_log.shape[0]}")

        # si filtramos todos los registros, pasamos a otro chunk
        if df_log.empty:
            continue

        df_log_conquest = df_log_conquest.append(df_log, ignore_index=True)

    # cambiamos el formato de la fecha a uno legible por humanos
    df_log_conquest["time"] = pd.to_datetime(df_log_conquest["time"], unit="s")

    # guardamos los contactos validos
    df_log_conquest.to_csv(concurso.PATH_REGISTROS, index=None)
    print()

    return df_log_conquest


if __name__ == "__main__":

    modalidad = "wspr"

    # Instanciamos un concurso
    concurso = Concurso(modalidad)
    print(concurso)

    # Cargamos resultados
    df_log_conquest = cargar_logs_validos(concurso)
    print(df_log_conquest.head())
    print("Procesando resultados")
    # contactos por banda en recepción
    df_log_conquest["N_rx"] = (
        df_log_conquest["time"].astype(str) + "-" + df_log_conquest["call_sign"]
    )

    df_rx = df_log_conquest.groupby(["reporter", "band"]).nunique()[["N_rx", "time"]]
    df_rx.reset_index(inplace=True)
    df_rx.rename(columns={"reporter": "sd", "time": "H_rx"}, inplace=True)
    df_rx.set_index(["sd", "band"], inplace=True)

    # contactos por banda en transmisión
    df_log_conquest["N_tx"] = (
        df_log_conquest["time"].astype(str) + "-" + df_log_conquest["reporter"]
    )

    df_tx = df_log_conquest.groupby(["call_sign", "band"]).nunique()[["N_tx", "time"]]
    df_tx.reset_index(inplace=True)
    df_tx.rename(columns={"call_sign": "sd", "time": "H_tx"}, inplace=True)
    df_tx.set_index(["sd", "band"], inplace=True)

    # unimos resultados
    df_r = pd.concat([df_tx, df_rx], join="outer", axis=1).fillna(0)
    df_r.reset_index(inplace=True)
    del df_tx
    del df_rx

    # corregimos el valor de H
    df_r["H_tx"] = df_r["H_tx"] * 100 * (2 * 60) / (concurso.duracion_concurso())
    df_r["H_rx"] = df_r["H_rx"] * 100 * (2 * 60) / (concurso.duracion_concurso())

    # filtramos finalmente a los que figuran en la lista de participantes
    df_r = df_r[df_r["sd"].isin(concurso.participantes)]

    # creamos el campo de banda
    df_r["B"] = df_r["band"].apply(concurso.calcular_puntaje_bandas)

    # calculamos el puntaje final
    df_r["puntaje"] = (
        df_r["H_tx"] * df_r["N_tx"] + df_r["H_rx"] * df_r["N_rx"]
    ) * df_r["B"]

    # y creamos el ranking por banda:
    df_r["ranking"] = df_r.groupby("band")["puntaje"].rank("dense", ascending=False)

    # obtenemos los inscriptos que no participaron
    no_participantes = [
        p for p in concurso.participantes if p not in df_r["sd"].unique()
    ]
    print(f"Inscriptos que no participaron: {len(no_participantes)}")
    print(concurso.participantes)

    # creamos un diccionario con las columnas y los campos en 0
    dict_no_p = dict.fromkeys(df_r.columns.tolist(), 0)
    dict_no_p["band"] = "0m"

    # los agregamos al listado con puntaje 0 en todo
    for no_p in no_participantes:
        dict_no_p["sd"] = no_p
        df_r = df_r.append(dict_no_p, ignore_index=True)

    # ordenamos los resultados por puntaje
    df_r.sort_values(by=["band", "puntaje"], inplace=True, ascending=False)

    # guardamos resultados sin procesar con puntaje
    with pd.ExcelWriter(concurso.PATH_RESULTADOS) as writer:
        for band in df_r.band.unique():
            df_r[df_r["band"] == band].to_excel(writer, band, index=None)

    print("Gracias vuelva prontos :)")
    print()
