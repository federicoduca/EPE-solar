
import pandas as pd
import numpy as np

from datetime import datetime
import pytz
import re

import os
import yaml

class Concurso():
    '''
    clase donde se definen los parametros necesarios para
    determinar las reglas básicas del concurso
    '''

    def __init__(self, fi, ff, e, b, p):
        
        ##### parametros de fecha
        self.fecha_inicio = datetime.strptime(fi,'%d-%m-%Y %H:%M')
        self.fecha_fin = datetime.strptime(ff,'%d-%m-%Y %H:%M')

        timezone = pytz.timezone("UTC")
        self.fecha_inicio = timezone.localize(self.fecha_inicio)
        self.fecha_fin = timezone.localize(self.fecha_fin)

        self.timestamp_inicio = datetime.timestamp(self.fecha_inicio)
        self.timestamp_fin = datetime.timestamp(self.fecha_fin)

        ##### listado de participantes
        self.participantes = p

        ##### listado de paises participantes
        self.entidades = e
        
        ##### listado de prefijos validos
        self.prefijos = []
        prefijo_entidad = {}             
        prefijo_entidad['Argentina'] = ['LP','LQ','LR','LS','LT','LU','LV','LW','AY','AZ','L']
        prefijo_entidad['Brasil'] = ['PP','PQ','PR','PS','PT','PU','PV','PW','PX','PY']
        prefijo_entidad['Uruguay'] = ['CV','CW','CX']
        prefijo_entidad['Chile'] = ['XQ','XR','CA','CB','CC','CD','CE']
        prefijo_entidad['Bolivia'] = ['CP']
        prefijo_entidad['Paraguay'] = ['ZP']
        prefijo_entidad['Peru'] = ['OA','OB','OC']
        prefijo_entidad['Antartida'] = ['DP','RI'] + prefijo_entidad['Argentina']

        for entidad, prefijos in prefijo_entidad.items():
            if entidad in self.entidades:
                self.prefijos.extend(prefijos)
            else:
                raise ValueError('Entidad no valida')
        
        ##### listado de bandas
        self.bandas = list(b.keys())
        self.puntaje_bandas = b

    def chequeo_fecha(self, timestamp):
        '''
        chequea que la fecha pasada como unix-timestamp 
        se encuentra dentro del plazo del concurso
        '''
        return (self.timestamp_inicio <= timestamp <= self.timestamp_fin)
           
    def chequeo_participante(self,call_sign):
        '''
        chequea que la señal distintiva figure dentro del listado de los participantes
        '''
        return call_sign in self.participantes

    def chequeo_region(self,sd):
        '''
        chequea que la señal distintiva figure dentro del listado de los participantes
        '''
        prefijo = re.split(r'\d',sd)[0]
        return prefijo in self.prefijos
            
    def chequeo_banda(self,banda_m):
        '''
        chequea que sea una banda permitida para el puntaje
        '''
        return banda_m in self.bandas   

    def convertir_band_a_banda_m(self,band):
        ''' 
        convierte formato banda de Megas a banda en metros
        '''
        ### si la banda es > 1000 es error, se divide por 1000
        if band > 1000:
            band = band//1000

        if band == 14:
            res = '20m'
        elif band == 7 :
            res = '40m'
        elif band == 3:
            res = '80m'
        elif band == 1:
            res = '160m'
        else:
            res = ''
        return res
    
    def calcular_puntaje_bandas(self, banda):
        '''
        convierte banda a puntaje
        '''
        return self.puntaje_bandas[banda]

    def obtener_participantes(self):
        return self.participantes
