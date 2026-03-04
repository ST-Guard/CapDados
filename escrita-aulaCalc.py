import psutil
import pandas as pd
import datetime
import time
import os
import platform

while True:
    momento = datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S") # Data e horário da máquina formatado

    usuario = platform.node()

    freq =  psutil.cpu_freq()
    cpuUsoFreq = freq.current # Uso da CPU em MHz
    cpuPorcUso = psutil.cpu_percent(interval=1) # Porcentagem de uso da cpu
    
    ram = psutil.virtual_memory()
    ramUso = ram.used # Uso da RAM em bytes
    ramPorcUso = ram.percent # Uso da RAM em porcetagem

    disco = psutil.disk_usage('C:/')
    discoUso = disco.used # Uso da Disco em bytes
    discoUsoPorc = disco.percent # Uso da Disco em porcetagem

    registroAtual = {
        'Usuário': [usuario],
        'horarioData': [momento],
        'cpuUsoFreq': [cpuUsoFreq],
        'cpuPorcUso': [cpuPorcUso],
        'ramUso': [ramUso / (1024 ** 3)],
        'ramPorcUso': [ramPorcUso],
        'discoUso': [discoUso/ (1024 ** 3)],
        'discoPorcUso': [discoUsoPorc],
    }

    df = pd.DataFrame(registroAtual)
    # mode=a para adicionar nova linha e header=not para não adicionar o header novamente
    df.to_csv("dadosMonitorados.csv", mode='a', header=not os.path.exists("dadosMonitorados.csv"), index=False, sep=";")

    time.sleep(10)
