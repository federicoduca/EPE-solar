import pandas as pd
import numpy as np
import os
import glob

class QSO:
    """
    con esta clase realizamos una normalización de formato de los qsos
    los campos son construidos a partir de la cantidad de campos encontrados en el msg
    """
    def __init__(self, linea, sd_a, grid_a):
        # dividimos la linea en campos
        campos = linea.split()

        self.sd_a = sd_a
        self.grid_a = grid_a

        self.q_campos = len(campos)

        if self.q_campos == 7:
            # log CE3JSXK linea 2
            self.type = "tx"
            self.fecha = int(campos[0]) 
            self.hora = int(campos[1])
            self.freq = float(campos[2])
            self.sd_b = campos[4]
            self.grid_b = campos[5]

        elif self.q_campos == 8:
            # log LW2EDM linea 890
            self.type = "tx"
            self.fecha = int(campos[0].split("_")[0])
            self.hora = int(campos[0].split("_")[1][:-2])
            self.freq = float(campos[1])
            self.sd_b = ""
            self.grid_b = ""

        elif self.q_campos == 9:
            # log LU1ZG linea 5
            self.type = "tx"
            self.fecha = int(campos[0].split("_")[0])
            self.hora = int(campos[0].split("_")[1][:-2])
            self.freq = float(campos[1])
            self.sd_b = campos[7]
            self.grid_b = ""

        elif self.q_campos == 10:
            # log LU5FF linea 1
            self.type = "tx"
            self.fecha = int(campos[0].split("_")[0])
            self.hora = int(campos[0].split("_")[1][:-2])
            self.freq = float(campos[1])
            self.sd_b = campos[7]
            self.grid_b = campos[8]

        elif self.q_campos == 11:
            # log CE3JSXK linea 1
            self.type = "t2"
            self.fecha = int(campos[0]) 
            self.hora = int(campos[1])
            self.freq = float(campos[5]) 
            self.sd_b = campos[6] 
            self.grid_b = ""
        
        elif self.q_campos == 12:
            # log CE3JSXK linea 3
            self.type = "t1"          
            self.fecha = int(campos[0]) 
            self.hora = int(campos[1])
            self.freq = float(campos[5])   
            self.sd_b = campos[6] 
            self.grid_b = campos[7]

        elif self.q_campos == 14:
            # log LU3DJ linea 1444
            self.type = "t2"
            self.fecha = int(campos[0]) 
            self.hora = int(campos[1])
            self.freq = float(campos[5]) 
            self.sd_b = campos[6] 
            self.grid_b = ""

        elif self.q_campos == 15:
            if campos[6][0] == "<":
                # log LU1IBL linea 3
                self.type = "t3"
                self.fecha = int(campos[0]) 
                self.hora = int(campos[1])
                self.freq = float(campos[5])
                self.sd_b = campos[6][1:-1]
                self.grid_b = campos[7]
            else:
                # log LU1IBL linea 1
                self.type = "t1"
                self.fecha = int(campos[0]) 
                self.hora = int(campos[1])
                self.freq = float(campos[5])
                self.sd_b = campos[6] 
                self.grid_b = campos[7]

        elif self.q_campos == 16:
            # log LW2EDM linea 2009
            self.type = "t2"
            self.fecha = int(campos[0]) 
            self.hora = int(campos[1])
            self.freq = float(campos[4])
            self.sd_b = campos[5] 
            self.grid_b = ""

        elif self.q_campos == 17:
            if campos[5][0] == "<":
                # log LU5FF linea 382
                self.type = "t3"
                self.fecha = int(campos[0]) 
                self.hora = int(campos[1])
                self.freq = float(campos[4])
                self.sd_b = campos[5][1:-1]
                self.grid_b = campos[6]
            else:
                # log LU2BN linea 1
                self.type = "t1"
                self.fecha = int(campos[0]) 
                self.hora = int(campos[1])
                self.freq = float(campos[4])
                self.sd_b = campos[5]
                self.grid_b = campos[6]
        else:
            print(self.q_campos)
            raise ("no se tiene contemplado este tipo de qso")

        self.banda = 10
        
    def __str__(self):
        return f"{self.fecha}_{str(self.hora)} - {self.type} - {self.q_campos} - {self.sd_b} - {self.grid_b}"


class Concurso():
    def __init__(self, fi, hi, ff, hf, t1, t2, t3):
        self.fecha_inicio = int(fi)
        self.hora_inicio = int(hi)
        self.fecha_fin = int(ff)
        self.hora_fin = int(hf)
        self.wspr_tipo1_valido = True if t1 == "s" else False
        self.wspr_tipo2_valido = True if t2 == "s" else False
        self.wspr_tipo3_valido = True if t3 == "s" else False


def obtener_senal_distintiva(nombre_archivo_log):
    # primero del archivo sacamos la señal distintiva
    nombre_log = nombre_archivo_log.split(".")[0]
    senial_distintiva = [s for s in nombre_log.split("_") if s not in ["ALL","WSPR"]]

    # si tiene mas de un elemento, entonces el nombre del archivo está mal
    if len(senial_distintiva) != 1:
        raise (f"El archivo posee nombre erroneo")

    # convertimos la lista a valor y retornamos
    return senial_distintiva[0]


def chequeo_validez_qso(com, conc):

    ## Chequeo de fecha
    if com.fecha < conc.fecha_inicio or (com.fecha == conc.fecha_inicio and com.hora < conc.hora_inicio):
        return "Concurso no iniciado"
    elif com.fecha > conc.fecha_fin or (com.fecha == conc.fecha_fin and com.hora > conc.hora_fin):
        return "Concurso finalizado"

    ## Chequeo de tipo de mensaje
    if com.type == "tx":
        return "Tipo de qso no valido - " + com.type
    elif com.type == "t1":
        if not conc.wspr_tipo1_valido:
            return "Tipo de qso no valido - " + com.type
    elif com.type == "t2":
        if not conc.wspr_tipo2_valido:
            return "Tipo de qso no valido - " + com.type
    elif com.type == "t3":
        if not conc.wspr_tipo3_valido:
            return "Tipo de qso no valido - " + com.type

    if com.contacto == "...":
        return "Contacto no valido"

    return "VALIDO"

def calculo_puntaje_qso(qso):
    
    return 0 


if __name__ == "__main__":
    
    print()
    print("A continuacion, ingrese los parametros del concurso:")
    print()
    print()
    # fi = input("Ingrese fecha de inicio del concurso (YYMMDD): ")
    # hi = input("Ingrese horario (UTC) de inicio del concurso (HHMM): ")
    # ff = input("Ingrese fecha de fin del concurso (YYMMDD): ")
    # hf = input("Ingrese horario (UTC) de fin del concurso (HHMM): ")
    # t1 = input("¿Es valido el mensaje Type 1 (ej: K1ABC FN42 37) (s/n)?: ")
    # t2 = input("¿Es valido el mensaje Type 2 - sin grid (ej: PJ4/K1ABC 37) (s/n)?: ")
    # t3 = input("¿Es valido el mensaje Type 3 - con grid de 6 caracteres (ej: <PJ4/K1ABC> FK52UD 37) (s/n)?: ")
    
    fi = 201213
    hi = 1500
    ff = 201215
    hf = 1500
    t1 = "s"
    t2 = "n"
    t3 = "s"
    concurso = Concurso(fi,hi,ff,hf,t1,t2,t3)
    
    print()
    print("Iniciando evaluador de competencia WSPR")
    print()

    # definimos el directorio para ejecutart
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    # estos van a ser los datos relevantes a guardar de los qso:
    columns_logs = ["sd_a","grid_a","sd_b","grid_b","banda"]
    
    # creamos/abrimos el archivo con los logs unificados, filtrados y formateados
    os.makedirs("resultados/",exist_ok=True)
    f_res = open("resultados/ALL_WSPR.txt","w+")

    # guardamos el encabezado de los campos
    f_res.write("sd_a,grid_a,sd_b,grid_b,banda\n") 
    
    print()
    print("Buscando grids de estaciones...")
    print()

    # detalle de QT
    df_grids = pd.read_csv("grids.csv")
    print(f"grids_head:{list(df_grids.columns)}")

    print()
    print("Buscando logs...")

    # busco los nombres de los archivos de format TXT que esten dentro de esta carpeta
    nombre_archivos_logs = [f.split("\\")[-1] for f in glob.glob("logs/*.txt")]

    print(f"Se encontraron {len(nombre_archivos_logs)} logs en el directorio")
    print()

    # analizamos log por log:
    for nombre_archivo_log in nombre_archivos_logs:

        print(f"Procesando el log: {nombre_archivo_log}")
        
        # informacion base
        sd_a = obtener_senal_distintiva(nombre_archivo_log)
        
        # grid del que realiza la escucha
        grid_a = df_grids[df_grids.senial == sd_a].grid

        # abrimos el log
        f_log = open("logs/"+nombre_archivo_log,"r")

        # guardamos comentarios del log
        os.makedirs("comentarios/",exist_ok=True)
        f_com = open("comentarios/" + nombre_archivo_log,"w+")

        # recorremos linea por linea el archivo del log para analizar cada qso
        for linea in f_log:
            if len(linea.split())!=0:
                # dividimos en campos
                qso = QSO(linea, sd_a, grid_a) 

                # chequamos validez
                chequeo_validez = chequeo_validez_qso(qso, concurso)

                # guardamos el comentarios sobre validez
                f_com.write(chequeo_validez)

                # si es valido lo guardamos en log masivo
                if chequeo_validez == "VALIDO":
                    f_res.write(f"{qso.sd_a},{qso.grid_a},{qso.sd_b},{qso.grid_b},{qso.banda}")

            # agregamos salto de linea en los comentarios
            f_com.write("\n")

        # cerramos los archivos de comentarios y log
        f_log.close()
        f_com.close()
        
    # cerrar archivos de resultados
    f_res.close()