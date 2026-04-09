import psutil
import csv
from datetime import datetime
import time
import speedtest # baixar como pip install speedtest-cli
import boto3 
import os


arquivo_csv = "dados-brutos_maquina.csv"

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
        object_name = os.path.basename('/home/valle/Área de trabalho/Caculo computacional/CapDados/dados-brutos_maquina.csv')

    try:
        response = session.upload_file(file_name, bucket, object_name)
    except:
        return False
    return True




with open(arquivo_csv, 'a', newline='') as csvfile:
    while(True): 
        colunas = ['EMPRESA','REGIAO', 'DATACENTER', 'ZONA', 'SERVIDOR','CPU','RAM_TOTAL','RAM_USADA','RAM_PERCENT','DISCO_TOTAL','DISCO_USADO','DISCO_PERCENT', 'LATENCIA', 'PACOTES_ENVIADOS', 'PACOTES_RECEBIDOS', 
                   'QTD_PR','PROCESSO1_CPU', 'PORCENTAGEM_PROCESSO1_CPU','PROCESSO2_CPU', 'PORCENTAGEM_PROCESSO2_CPU', 'PROCESSO3_CPU', 'PORCENTAGEM_PROCESSO3_CPU'
                   ,'PROCESSO1_RAM', 'PORCENTAGEM_PROCESSO1_RAM','PROCESSO2_RAM', 'PORCENTAGEM_PROCESSO2_RAM', 'PROCESSO3_RAM', 'PORCENTAGEM_PROCESSO3_RAM'  
                   ,'DATA_HORA']
        CSV_DIC_WRITER = csv.DictWriter(csvfile, fieldnames=colunas, delimiter=';')
        
        if csvfile.tell() == 0:
            CSV_DIC_WRITER.writeheader()


        
        #pegando os componentes normais

        cpu_usage = psutil.cpu_percent(interval=1)
        ram = psutil.virtual_memory() 
        disk = psutil.disk_usage("/")



        # pegando hora e contagem de processos
        tempo_agora = datetime.now()
        contagem_processos = len(psutil.pids())


        #pegando rede
        instancia_speed_test = speedtest.Speedtest(secure=True)
        instancia_speed_test.get_best_server()

        ping = instancia_speed_test.results.ping

        
        dados_internet = psutil.net_io_counters()
        pacotes_enviados = dados_internet.packets_sent
        pacotes_recebidos = dados_internet.packets_recv
        
        
        
        # pegando processos com maior consumo

        lista_tres_ultimos = []

        for p in psutil.process_iter(['name', 'cpu_percent', 'memory_info', 'ppid']):
            
            
            info = p.info
            if (info['name'] is None or info['memory_info'] is None):
                a= a
            else:
                

                dados_enviar =  {'nome': info['name'], 'cpu': info['cpu_percent'],'memoria': info['memory_info'], 'pid': info['ppid']}
                lista_tres_ultimos.append(dados_enviar)


     


        top_maior_processo_cpu= lista_tres_ultimos[0]
        top_segundo_processo_cpu = lista_tres_ultimos[1]
        top_terceiro_processo_cpu = lista_tres_ultimos[2]


        top_maior_processo_ram= lista_tres_ultimos[0]
        top_segundo_processo_ram = lista_tres_ultimos[1]
        top_terceiro_processo_ram = lista_tres_ultimos[2]

        for produto_agora in lista_tres_ultimos:
            




            if (top_maior_processo_cpu['cpu'] < produto_agora['cpu']):
                top_terceiro_processo_cpu = top_segundo_processo_cpu
                top_segundo_processo_cpu = top_maior_processo_cpu
                top_maior_processo_cpu= produto_agora
            
            elif (top_segundo_processo_cpu['cpu'] < produto_agora['cpu']):
                top_terceiro_processo_cpu = top_segundo_processo_cpu
                top_segundo_processo_cpu = produto_agora

        
            elif (top_terceiro_processo_cpu['cpu'] < produto_agora['cpu']):
                top_terceiro_processo_cpu = produto_agora


            if (top_maior_processo_ram['memoria'].rss < produto_agora['memoria'].rss):
                top_terceiro_processo_ram = top_segundo_processo_ram
                top_segundo_processo_ram = top_maior_processo_ram
                top_maior_processo_ram= produto_agora
            
            elif (top_segundo_processo_ram['memoria'].rss < produto_agora['memoria'].rss):
                top_terceiro_processo_ram = top_segundo_processo_ram
                top_segundo_processo_ram = produto_agora

        
            elif (top_terceiro_processo_ram['memoria'].rss < produto_agora['memoria'].rss ):
                top_terceiro_processo_ram = produto_agora



       
        print()
        dados_dict =  {
            'EMPRESA': 'GOGE', 
            'REGIAO': 'A1',
            'DATACENTER': 'DATA_CENTER_01',
            'ZONA': 'A1',
            'SERVIDOR': 'AB043', 
            'CPU': cpu_usage, 
            'RAM_TOTAL': ram.total, 
            'RAM_USADA': ram.used, 
            'RAM_PERCENT': ram.percent, 
            'DISCO_TOTAL': disk.total,
            'DISCO_USADO': disk.used, 
            'DISCO_PERCENT': disk.percent,
            'LATENCIA': ping, 
            'PACOTES_ENVIADOS': pacotes_enviados, 
            'PACOTES_RECEBIDOS': pacotes_recebidos,  
            'QTD_PR': contagem_processos,
            'PROCESSO1_CPU': top_maior_processo_cpu['nome'], 
            'PORCENTAGEM_PROCESSO1_CPU': top_maior_processo_cpu['cpu'],
            'PROCESSO2_CPU': top_segundo_processo_cpu['nome'], 
            'PORCENTAGEM_PROCESSO2_CPU': top_segundo_processo_cpu['cpu'], 
            'PROCESSO3_CPU': top_terceiro_processo_cpu['nome'], 
            'PORCENTAGEM_PROCESSO3_CPU': top_terceiro_processo_cpu['cpu'],
            'PROCESSO1_RAM': top_maior_processo_ram['nome'], 
            'PORCENTAGEM_PROCESSO1_RAM': top_maior_processo_ram['memoria'].rss,
            'PROCESSO2_RAM': top_segundo_processo_ram['nome'], 
            'PORCENTAGEM_PROCESSO2_RAM': top_segundo_processo_ram['memoria'].rss, 
            'PROCESSO3_RAM': top_terceiro_processo_ram['nome'], 
            'PORCENTAGEM_PROCESSO3_RAM': top_terceiro_processo_ram['memoria'].rss,  
            'DATA_HORA': tempo_agora}
        

        print(f"""
        ----------------CAPTURANDO   DADOS ----------------
        {tempo_agora}
        CPU: {cpu_usage}
        RAM: {ram.percent}
        DISCO: {disk.percent}

        LATENCIA {ping}
        PACOTES ENVIADOS: {pacotes_enviados}
        PACOTES RECEBIDOS: {pacotes_recebidos}


        QUANTIDADE DE PROCESSOS: {contagem_processos}
        PROCESSO COM MAIOR CONSUMO DE CPU: {top_maior_processo_cpu['nome']}  {top_maior_processo_cpu['cpu']}%
        PROCESSO COM MAIOR CONSUMO DE RAM: {top_maior_processo_ram['nome']}  {top_maior_processo_cpu['memoria'].rss}%

        ----------------------------------------------------


""")

        CSV_DIC_WRITER.writerow(dados_dict)
        csvfile.flush()
        upload_file('dados-brutos_maquina.csv','s3-smartdata-2026-06-04-teste', 'raw/dados-brutos_maquina.csv')



        time.sleep(5)



    


