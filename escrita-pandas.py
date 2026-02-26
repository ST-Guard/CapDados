import psutil
import pandas as pd
import datetime
import time
import os

while True:
    agora = datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S") # Data e horário da máquina formatado

    cpuPorcentagem = psutil.cpu_percent(interval=1) # Porcentagem de uso da cpu

    ram = psutil.virtual_memory()
    ramUso = ram.used

    disco = psutil.disk_usage('C:/')
    discoUso = disco.used

    rede = psutil.net_if_stats()
    veloWifi = rede['Wi-Fi'].speed


    registroAtual = {
        'horarioData': [agora],
        'cpuPorcentagemUso': [cpuPorcentagem],
        'ramBytesUso': [ramUso],
        'discoBytesUso': [discoUso],
        'velocidadeMbWifi': [veloWifi]
    }

    df = pd.DataFrame(registroAtual)
    # mode=a para adicionar nova linha e header=not para não adicionar o header novamente
    df.to_csv("dadosMonitorados.csv", mode='a', header=not os.path.exists("dadosMonitorados.csv"), index=False)

    time.sleep(2)
