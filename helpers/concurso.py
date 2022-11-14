from datetime import datetime
import pandas as pd
import pytz
import re
import yaml

DICT_PREFIJO_ENTIDADES = {
    "Argentina": ["LP", "LQ", "LR", "LS", "LT", "LU", "LV", "LW", "AY", "AZ", "L"],
    "Brasil": ["PP", "PQ", "PR", "PS", "PT", "PU", "PV", "PW", "PX", "PY"],
    "Uruguay": ["CV", "CW", "CX"],
    "Chile": ["XQ", "XR", "CA", "CB", "CC", "CD", "CE"],
    "Bolivia": ["CP"],
    "Paraguay": ["ZP"],
    "Peru": ["OA", "OB", "OC"],
    "Antartida": [
        "DP",
        "RI",
        "LP",
        "LQ",
        "LR",
        "LS",
        "LT",
        "LU",
        "LV",
        "LW",
        "AY",
        "AZ",
        "L",
    ],
}


class Concurso:
    """
    clase donde se definen los parametros necesarios para
    determinar las reglas básicas del concurso
    """

    def __init__(self, modalidad):

        self.modalidad = modalidad
        config = self._cargar_configuracion_concurso()

        # constantes de archivos auxiliares
        self.PATH_PARTICIPANTES = config["files"]["path_participantes"]
        self.PATH_LOGS = config["files"]["path_logs"]
        self.PATH_RESULTADOS = config["files"]["path_resultados"]
        self.PATH_REGISTROS = config["files"]["path_registros"]
        self.CHUNK_SIZE = config["files"].get("chunk_size")

        # constantes del concurso
        FI = config["concurso"]["fi"]
        FF = config["concurso"]["ff"]
        ENTIDADES = config["concurso"]["entidades"]
        BANDAS = config["concurso"]["bandas"]

        # parametros de fecha
        self.fecha_inicio = datetime.strptime(FI, "%d-%m-%Y %H:%M")
        self.fecha_fin = datetime.strptime(FF, "%d-%m-%Y %H:%M")

        timezone = pytz.timezone("UTC")
        self.fecha_inicio = timezone.localize(self.fecha_inicio)
        self.fecha_fin = timezone.localize(self.fecha_fin)

        self.timestamp_inicio = datetime.timestamp(self.fecha_inicio)
        self.timestamp_fin = datetime.timestamp(self.fecha_fin)

        # listado de participantes
        self.participantes = self._obtener_lista_participantes()

        # listado de bandas
        self.bandas = list(BANDAS.keys())
        self.puntaje_bandas = BANDAS

        # listado de paises participantes
        self.entidades = ENTIDADES

        # listado de prefijos validos
        self.prefijos = []

        for entidad, prefijos in DICT_PREFIJO_ENTIDADES.items():
            if entidad in self.entidades:
                self.prefijos.extend(prefijos)
            else:
                raise ValueError("Entidad no valida")

    def _cargar_configuracion_concurso(self):

        # Cargamos el archivo YAML con la configuracion
        with open(f"configuracion_{self.modalidad}.yaml", "r") as stream:
            config = yaml.load(stream, yaml.SafeLoader)

        return config

    def _obtener_lista_participantes(self):
        df_ham = pd.read_csv(self.PATH_PARTICIPANTES, delimiter=";")
        filterMODO = df_ham["modo"] == self.modalidad.upper()
        participantes = df_ham[filterMODO]["call_sign"].tolist()

        return participantes

    def duracion_concurso(self):
        """
        devuelve la duracion del evento en segundos
        """
        return self.timestamp_fin - self.timestamp_inicio

    def chequeo_fecha(self, timestamp):
        """
        chequea que la fecha pasada como unix-timestamp 
        se encuentra dentro del plazo del concurso
        """
        return self.timestamp_inicio <= timestamp <= self.timestamp_fin

    def chequeo_participante(self, call_sign):
        """
        chequea que la señal distintiva figure dentro del listado de los participantes
        """
        return call_sign in self.participantes

    def chequeo_region(self, sd):
        """
        chequea que la señal distintiva figure dentro del listado de los participantes
        """
        prefijo = re.split(r"\d", sd)[0]
        return prefijo in self.prefijos

    def chequeo_banda(self, banda_m):
        """
        chequea que sea una banda permitida para el puntaje
        """
        return banda_m in self.bandas

    def convertir_band_a_banda_m(self, band):
        """ 
        convierte formato banda de Megas a banda en metros
        """
        ## si la banda es > 1000 es error, se divide por 1000
        if band > 1000:
            band = band // 1000

        if band == 14:
            res = "20m"
        elif band == 7:
            res = "40m"
        elif band == 3:
            res = "80m"
        elif band == 1:
            res = "160m"
        else:
            res = ""
        return res

    def calcular_puntaje_bandas(self, banda):
        """
        convierte banda a puntaje
        """
        return self.puntaje_bandas[banda]

    def __repr__(self) -> str:
        repr = "> Duracion:" + "\n"
        repr += f"   - Desde {self.fecha_inicio}" + "\n"
        repr += f"   - Hasta {self.fecha_fin}" + "\n"
        repr += f"> Entidades:" + "\n"
        for entidad in self.entidades:
            repr += f"   - {entidad}" + "\n"
        repr += f" > Bandas / Puntaje:" + "\n"
        for b, p in self.puntaje_bandas.items():
            repr += f"   - {b}: {p}" + "\n"

        return repr
