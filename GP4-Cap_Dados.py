import psutil
import csv
from datetime import datetime
import time
import boto3
import os



arquivo_csv = "dados-brutos.csv"

print("""\033[33m
  /$$$$$$                                      /$$     /$$$$$$$              /$$              
 /$$__  $$                                    | $$    | $$__  $$            | $$              
| $$  \__/ /$$$$$$/$$$$   /$$$$$$   /$$$$$$  /$$$$$$  | $$  \ $$  /$$$$$$  /$$$$$$    /$$$$$$ 
|  $$$$$$ | $$_  $$_  $$ |____  $$ /$$__  $$|_  $$_/  | $$  | $$ |____  $$|_  $$_/   |____  $$
 \____  $$| $$ \ $$ \ $$  /$$$$$$$| $$  \__/  | $$    | $$  | $$  /$$$$$$$  | $$      /$$$$$$$
 /$$  \ $$| $$ | $$ | $$ /$$__  $$| $$        | $$ /$$| $$  | $$ /$$__  $$  | $$ /$$ /$$__  $$
|  $$$$$$/| $$ | $$ | $$|  $$$$$$$| $$        |  $$$$/| $$$$$$$/|  $$$$$$$  |  $$$$/|  $$$$$$$
 \______/ |__/ |__/ |__/ \_______/|__/         \___/  |_______/  \_______/   \___/   \_______/
                                                                                              
                                                                                              
                                                                                              

\033[m""")


def upload_file(file_name, bucket, object_name=None):
    session = boto3.client(
        's3',
        aws_access_key_id='',
        aws_secret_access_key='',
        aws_session_token=''
    )
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    try:
        response = session.upload_file(file_name, bucket, object_name)
    except:
        return False
    return True

with open(arquivo_csv, 'a', newline='') as csvfile:
    while(True): 
        colunas = ['USER','CPU','RAM_TOTAL','RAM_USADA','RAM_PERCENT','DISCO_TOTAL','DISCO_USADO','DISCO_PERCENT','RPM_FAN', 'TEMP_PC', 'QTD_PR','DATA_HORA']
        CSV_DIC_WRITER = csv.DictWriter(csvfile, fieldnames=colunas, delimiter=';')
        
        if csvfile.tell() == 0:
            CSV_DIC_WRITER.writeheader()

        cpu_usage = psutil.cpu_percent(interval=1)
        ram = psutil.virtual_memory() 
        disk = psutil.disk_usage("/")
        tempo_agora = datetime.now()
        rpm_fan = psutil.sensors_fans()['acpi_fan'][0].current 
        temperatura_computador = psutil.sensors_temperatures()['nvme'][0].current 
        contagem_processos = len(psutil.pids())
        print(f"Escrevendo dados: \n CPU: {cpu_usage}\n RAM_TOTAL: {ram.total} : RAM_USADA: {ram.used} : RAM_PORCENTAGEM: {ram.percent}\n DISCO_TOTAL: {disk.total} : DISCO_USADA: {disk.used} : DISCO_PORCENTAGEM: {disk.percent}\n TEMPERATURA: {temperatura_computador} RPM_FANS: {rpm_fan} QUANTIDADE PROCESSOS {contagem_processos}  \n DATA_HORA: {tempo_agora}")
        print()
        dados_dict =  {
            'USER': 'Matheus V.',
            'CPU': cpu_usage, 
            'RAM_TOTAL': ram.total, 
            'RAM_USADA': ram.used, 
            'RAM_PERCENT': ram.percent, 
            'DISCO_TOTAL': disk.total,
            'DISCO_USADO': disk.used, 
            'DISCO_PERCENT': disk.percent,
            'RPM_FAN': rpm_fan,
            'TEMP_PC': temperatura_computador,
            'QTD_PR': contagem_processos, 
            'DATA_HORA': tempo_agora}

        CSV_DIC_WRITER.writerow(dados_dict)
        csvfile.flush()

    
        upload_file('dados-brutos.csv','buckte-teste-sptech-gabrielmr', 'raw/dados-brutos.csv')
        time.sleep(5)



