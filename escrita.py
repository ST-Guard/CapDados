import psutil
import pandas as pd
import datetime
import time
import os

while True:
    momento = datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S") # Data e horário da máquina formatado

    freq =  psutil.cpu_freq()
    cpuUsoFreq = freq.current # Uso da CPU em MHz
    cpuMaxFreq = freq.max # Frequência máxima da CPU em MHz
    cpuNuclesLogi = psutil.cpu_count(logical=True) # Conta a quantidade de núcles lógicos
    cpuPorcUso = psutil.cpu_percent(interval=1) # Porcentagem de uso da cpu
    
    ram = psutil.virtual_memory()
    ramTotal = ram.total # Total da RAM em bytes
    ramUso = ram.used # Uso da RAM em bytes
    ramPorcUso = ram.percent # Uso da RAM em porcetagem

    disco = psutil.disk_usage('C:/')
    dicosTotal = disco.total # Total da Disco em bytes
    discoUso = disco.used # Uso da Disco em bytes
    discoUsoPorc = disco.percent # Uso da Disco em porcetagem

    registroAtual = {
        'horarioData': [momento],
        'cpuUsoFreq': [cpuUsoFreq],
        'cpuMaxFreq': [cpuMaxFreq],
        'cpuPorcUso': [cpuPorcUso],
        'ramTotal': [ramTotal],
        'ramUso': [ramUso],
        'ramPorcUso': [ramPorcUso],
        'dicosTotal': [dicosTotal],
        'discoUso': [discoUso],
        'discoPorcUso': [discoUsoPorc],
    }

    df = pd.DataFrame(registroAtual)
    # mode=a para adicionar nova linha e header=not para não adicionar o header novamente
    df.to_csv("dadosMonitorados.csv", mode='a', header=not os.path.exists("dadosMonitorados.csv"), index=False)

    time.sleep(10)
