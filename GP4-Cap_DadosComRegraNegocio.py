import psutil
import csv
from datetime import datetime
import time
import speedtest # baixar como pip install speedtest-cli
import boto3 
import os
import mysql.connector
from dotenv import load_dotenv
import random
#pip install python-dotenv
#pip install mysql-connector-python


arquivo_csv = "dados-brutos_maquina.csv"
bucket_name = 's3-smart-data-teste'

#STE12345          
#SERVIDOR-DC01-WEB-05

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




load_dotenv(".env.dev")


chave_acesso = os.getenv('AWS_ACCESS_KEY_ID')
chave_secreta = os.getenv('AWS_SECRET_ACCESS_KEY')
token_sessao = os.getenv('AWS_SESSION_TOKEN')

# Banco de Dados
banco_host = os.getenv('DB_HOST')
banco_user = os.getenv('DB_USER')
banco_senha = os.getenv('DB_PASSWORD')
banco_nome = os.getenv('DB_NAME')
banco_porta = int(os.getenv('DB_PORT', 3306))






# def upload_file(file_name, bucket, object_name=None):
#     session = boto3.client(
#         's3',
#           aws_access_key_id=chave_acesso,
#         aws_secret_access_key=chave_secreta,
#         aws_session_token=token_sessao 
#     )
#     # If S3 object_name was not specified, use file_name
#     if object_name is None:
#         object_name = file_name

#     try:
#         response = session.upload_file(file_name, bucket, object_name)
#         print("enviado para o S3 com sucesso")
#     except ValueError as e:
#         print(f"Erro ao enviar para o S3: {e}")
#     return True




conexao = mysql.connector.connect(
        host=banco_host,
        user=banco_user,
        password=banco_senha,
        database=banco_nome,
        port=banco_porta
)


token = input("Digite o token da empresa: ")


#------------------------------------------------------------------------------------------------------------------
#FUNÇÕES DA SIMULAÇÃO

#Função utilizada para que os valores simulados não ultrapassem 100% no caso do disco e cpu, ou ultrapassem o que eles tem disponível para utilizar como a Ram 
def limitar(valor, minimo, maximo):
    return max(minimo, min(valor, maximo))

#A steam tem um comportamento especifico em cada dia da semana e para isso ser simulado com veracidade, iremos colcoar um fator de cada dia 
def fator_por_dia(dia_semana):  
    #[0] monday ...
    if dia_semana in [3, 4]:
        return 2.0
    elif dia_semana in [0, 1]:
        return 1.5
    elif dia_semana in [5, 6]:
        return 1.1
    elif dia_semana == 2:
        return 0.7
    #caso ele não capte nada, o default vai ser 1.0
    return 1.0


#Na steam há horários onde tem mais ou menos jogadores, para simular isso, irei fazer um peso para cada faixa horária
def fator_por_horario(hora):
    if 18 <= hora <= 23:
        return 1.8
    elif 12 <= hora < 18:
        return 1.2
    elif 6 <= hora < 12:
        return 0.8
    else:
        return 0.5


#Na steam, toda terça feira entre 18 - 21 horas, há manutenção dos seus servidores para aliviar a carga e isso afeta os servidores
def esta_em_manutencao(agora):
    return agora.weekday() == 1 and 18 <= agora.hour < 21

#------------------------------------------------------------------------------------------------------------------


cursor = conexao.cursor()
query_empresa = f"SELECT * FROM empresa WHERE tokenEmpresa ='{token}';" 
cursor.execute(query_empresa)
empresa = cursor.fetchall()


if len(empresa) == 0:
    print("TOKEN INVALIDO!")
else:
    servidor = input("Digite o nome do servidor: ")
    query_servidor = f"SELECT * FROM servidor AS s JOIN zona ON s.fkZona = idZona JOIN regiao ON fkRegiaoDatacenter = fkDataCenter JOIN empresa ON fkRegiaoEmpresa = idEmpresa JOIN datacenter ON fkRegiaoDatacenter = idDatacenter WHERE idEMpresa = {empresa[0][0]} AND  s.nome = '{servidor}';"
    cursor.execute(query_servidor)
    servidor = cursor.fetchall()
    if len(servidor) == 0:
        print("SERVIDOR NÃO É VALIDO")
    else:
        print("INICIANDO A CAPTAÇÃO DOS DADOS")
        arquivo_csv = f"dados-brutos-{servidor[0][1]}.csv"

        
 

        with open(arquivo_csv, 'a', newline='') as csvfile:
            while(True): 
                colunas = ['EMPRESA','REGIAO', 'DATACENTER', 'ZONA', 'SERVIDOR','CPU','RAM_TOTAL','RAM_USADA','RAM_PERCENT','DISCO_TOTAL','DISCO_USADO','DISCO_PERCENT', 'LATENCIA', 'UPLOAD_BYTES', 'DOWNLOAD_BYTES', 'PERDA_PACOTES_PERCENTUAL',
                           'QTD_PR','PROCESSO1_CPU', 'PORCENTAGEM_PROCESSO1_CPU','PROCESSO2_CPU', 'PORCENTAGEM_PROCESSO2_CPU', 'PROCESSO3_CPU', 'PORCENTAGEM_PROCESSO3_CPU'
                           ,'PROCESSO1_RAM', 'PORCENTAGEM_PROCESSO1_RAM','PROCESSO2_RAM', 'PORCENTAGEM_PROCESSO2_RAM', 'PROCESSO3_RAM', 'PORCENTAGEM_PROCESSO3_RAM'  
                           , 'DATA_HORA', 'DIA_SEMANA']
                CSV_DIC_WRITER = csv.DictWriter(csvfile, fieldnames=colunas, delimiter=';')

                if csvfile.tell() == 0:
                    CSV_DIC_WRITER.writeheader()

                
                cpu_real = psutil.cpu_percent(interval=1)
                ram = psutil.virtual_memory()
                disk = psutil.disk_usage("/")
                tempo_agora = datetime.now()

                dia_semana = tempo_agora.strftime('%A')
                dia = tempo_agora.weekday()
                hora = tempo_agora.hour

                contagem_processos_real = len(psutil.pids())
                bootime = psutil.boot_time()
                
                try:
                    latencia_speed_test = speedtest.Speedtest(secure=True)
                    latencia_speed_test.get_best_server()
                    ping_real = latencia_speed_test.results.ping
                except Exception as e:
                    print(f"Erro no speedtest: {e}")
                    ping_real = 0.0

                dados_internet = psutil.net_io_counters()
                #----------------------------------------------------------
                #FATORES DE CADA DIA
                fator_dia = fator_por_dia(dia)
                fator_hora = fator_por_horario(hora)
                manutencao = esta_em_manutencao(tempo_agora)

                fator_latencia_dia = {
                    0: 1.2,  
                    1: 1.3,  
                    2: 0.8,  
                    3: 1.4,  
                    4: 1.5,  
                    5: 1.1,  
                    6: 1.0   
                }

                #----------------------------------------------------------
                #Aplicando a regra de negócio:

                #CPU
                cpu_negocio = cpu_real * fator_dia * fator_hora

                if manutencao:
                    cpu_negocio *= 0.45

                cpu_negocio += random.uniform(-3, 3)
                cpu_negocio = limitar(cpu_negocio, 5, 98)

                #RAM
                ram_percent_negocio = ram.percent
                ram_percent_negocio *= 1 + ((fator_hora - 1) * 0.15)
                ram_percent_negocio += random.uniform(-2, 2)
                ram_percent_negocio = limitar(ram_percent_negocio, 30, 92)
                ram_usada_negocio = int(ram.total * (ram_percent_negocio / 100))

                #DISCO
                disco_percent_negocio = disk.percent
                disco_percent_negocio *= 1 + ((fator_dia - 1) * 0.10)

                if dia == 2:
                    disco_percent_negocio *= 0.85

                if manutencao:
                    disco_percent_negocio *= random.uniform(0.75, 0.90)

                disco_percent_negocio += random.uniform(-1.5, 1.5)
                disco_percent_negocio = limitar(disco_percent_negocio, 20, 95)
                disco_usado_negocio = int(disk.total * (disco_percent_negocio / 100))

                #LATÊNCIA
                latencia_base = ping_real

                if latencia_base <= 0:
                    latencia_base = random.uniform(8, 25)

                if manutencao:
                    latencia_negocio = latencia_base * random.uniform(6, 12)
                else:
                    latencia_negocio = latencia_base * fator_hora * fator_latencia_dia[dia]
                    latencia_negocio += random.uniform(-2, 4)

                latencia_negocio = limitar(latencia_negocio, 5, 500)

                #PACOTES
                
                bytes_enviados_atual = dados_internet.bytes_sent
                bytes_recebidos_atual = dados_internet.bytes_recv

                # se a variável bytes_enviados_anterior ainda não existe (para nao dar erro)
                if 'bytes_enviados_anterior' not in globals():
                    bytes_enviados_anterior = bytes_enviados_atual
                    bytes_recebidos_anterior = bytes_recebidos_atual

                upload_bytes = (bytes_enviados_atual - bytes_enviados_anterior)
                download_bytes = (bytes_recebidos_atual - bytes_recebidos_anterior)

                bytes_enviados_anterior = bytes_enviados_atual
                bytes_recebidos_anterior = bytes_recebidos_atual

                upload_bytes *= (1 + ((fator_hora - 1) * 0.20))
                download_bytes *= (1 + ((fator_hora - 1) * 0.35))

                # Na quinta e na sexta começam as promoções de fim de semana, por há um aumento
                if dia in [3, 4]:
                    download_bytes *= random.uniform(1.3, 2.0)
                
                if manutencao:
                    upload_bytes *= random.uniform(0.30, 0.60)
                    download_bytes *= random.uniform(0.20, 0.50)
                    perda_pacotes_percentual = random.uniform(5, 18)
                else:
                    perda_pacotes_percentual = (random.uniform(0.1, 2.5)* fator_hora)
                    perda_pacotes_percentual += random.uniform(-0.2, 0.3)

                #ruído natural do dia a dia
                upload_bytes += random.randint(-5000, 5000)
                download_bytes += random.randint(-10000, 10000)

                # para não ficar negativo
                upload_bytes = max(0, int(upload_bytes))
                download_bytes = max(0, int(download_bytes))

                perda_pacotes_percentual = limitar(perda_pacotes_percentual,0,100)

                #PROCESSOS
                qtd_processos_negocio = int(contagem_processos_real * (1 + ((fator_hora - 1) * 0.05)))
                qtd_processos_negocio += random.randint(-3, 3)
                qtd_processos_negocio = max(1, qtd_processos_negocio)

                lista_processos = []

                #Fazendo um for para que o uso da cpu não venho como 0, pq a primeira rodada dele vem 0
                for p in psutil.process_iter():
                    try:
                        p.cpu_percent(None)
                    except:
                        pass

                time.sleep(0.5)
                
                #TOP3 CPU E RAM
                for p in psutil.process_iter(['name', 'cpu_percent', 'memory_info', 'ppid']):
                    try:
                        info = p.info

                        if info['name'] and info['memory_info'] is not None:
                            cpu_processo = info['cpu_percent']
                            ram_processo = info['memory_info'].rss

                            cpu_processo *= 1 + ((fator_hora - 1) * 0.20)
                            ram_processo *= 1 + ((fator_dia - 1) * 0.08)

                            if manutencao:
                                cpu_processo *= random.uniform(0.35, 0.70)
                                ram_processo *= random.uniform(0.80, 0.95)

                            cpu_processo += random.uniform(-1, 1)
                            cpu_processo = limitar(cpu_processo, 0, 100)

                            lista_processos.append({
                                'nome': info['name'],
                                'cpu': cpu_processo,
                                'memoria': int(ram_processo),
                                'pid': info['ppid']
                            })

                    except:
                        pass

                #Como funciona o sorted: ordene a lista x, kewy-lambda x = pelo o que vc quer ordenar, reverse= TRUE[:3] : do maior para o menor, pegar o top3
                top_cpu = sorted(lista_processos, key=lambda x: x['cpu'], reverse=True)[:3]
                top_ram = sorted(lista_processos, key=lambda x: x['memoria'], reverse=True)[:3]

                #Se não tiver 3 processos...
                while len(top_cpu) < 3:
                    top_cpu.append({'nome': 'N/A', 'cpu': 0, 'memoria': 0})

                while len(top_ram) < 3:
                    top_ram.append({'nome': 'N/A', 'cpu': 0, 'memoria': 0})


                print()
                dados_dict =  {
                    'EMPRESA': f'{servidor[0][16]}',
                    'REGIAO': f'{servidor[0][12]}',
                    'DATACENTER': f'{servidor[0][21]}',
                    'ZONA': f'{servidor[0][6]}',
                    'SERVIDOR': f'{servidor[0][1]}',
                    'CPU': cpu_negocio,
                    'RAM_TOTAL': ram.total,
                    'RAM_USADA': ram_usada_negocio,
                    'RAM_PERCENT': ram_percent_negocio,
                    'DISCO_TOTAL': disk.total,
                    'DISCO_USADO': disco_usado_negocio,
                    'DISCO_PERCENT': disco_percent_negocio,
                    'LATENCIA': latencia_negocio,
                    'UPLOAD_BYTES': upload_bytes,
                    'DOWNLOAD_BYTES': download_bytes,
                    'PERDA_PACOTES_PERCENTUAL': perda_pacotes_percentual,
                    'QTD_PR': qtd_processos_negocio,
                    'PROCESSO1_CPU': top_cpu[0]['nome'],
                    'PORCENTAGEM_PROCESSO1_CPU': top_cpu[0]['cpu'],
                    'PROCESSO2_CPU': top_cpu[1]['nome'],
                    'PORCENTAGEM_PROCESSO2_CPU': top_cpu[1]['cpu'],
                    'PROCESSO3_CPU': top_cpu[2]['nome'],
                    'PORCENTAGEM_PROCESSO3_CPU': top_cpu[2]['cpu'],
                    'PROCESSO1_RAM': top_ram[0]['nome'],
                    'PORCENTAGEM_PROCESSO1_RAM': top_ram[0]['memoria'],
                    'PROCESSO2_RAM': top_ram[1]['nome'],
                    'PORCENTAGEM_PROCESSO2_RAM': top_ram[1]['memoria'],
                    'PROCESSO3_RAM': top_ram[2]['nome'],
                    'PORCENTAGEM_PROCESSO3_RAM': top_ram[2]['memoria'],
                    'DATA_HORA': tempo_agora,
                    'DIA_SEMANA': dia_semana
                    }


                print(f"""
                    ----------------CAPTURANDO DADOS ----------------

                    DATA/HORA: {tempo_agora}

                    EMPRESA: {servidor[0][16]}
                    REGIAO: {servidor[0][12]}
                    DATACENTER: {servidor[0][21]}
                    ZONA: {servidor[0][6]}
                    SERVIDOR: {servidor[0][1]}

                    ---------------- COMPONENTES ----------------

                    CPU: {cpu_negocio:.2f}%

                    RAM TOTAL: {ram.total}
                    RAM USADA: {ram_usada_negocio}
                    RAM USO: {ram_percent_negocio:.2f}%

                    DISCO TOTAL: {disk.total}
                    DISCO USADO: {disco_usado_negocio}
                    DISCO USO: {disco_percent_negocio:.2f}%

                    LATENCIA: {latencia_negocio:.2f}ms

                    ---------------- REDE ----------------

                    UPLOAD BYTES: {upload_bytes}
                    DOWNLOAD BYTES: {download_bytes}
                    PERDA DE PACOTES: {perda_pacotes_percentual:.2f}%

                    ---------------- PROCESSOS ----------------

                    QTD PROCESSOS: {qtd_processos_negocio}

                    BOOT TIME: {bootime}

                    TOP 3 CPU:

                    1° {top_cpu[0]['nome']} -> {top_cpu[0]['cpu']:.2f}%
                    2° {top_cpu[1]['nome']} -> {top_cpu[1]['cpu']:.2f}%
                    3° {top_cpu[2]['nome']} -> {top_cpu[2]['cpu']:.2f}%

                    TOP 3 RAM:

                    1° {top_ram[0]['nome']} -> {top_ram[0]['memoria']}
                    2° {top_ram[1]['nome']} -> {top_ram[1]['memoria']}
                    3° {top_ram[2]['nome']} -> {top_ram[2]['memoria']}

                    ----------------------------------------------------
                    """)
                        
                CSV_DIC_WRITER.writerow(dados_dict)
                csvfile.flush()
                nome_arquivo_s3 = f"raw/{dados_dict['EMPRESA']}_{dados_dict['DATACENTER']}_{dados_dict['ZONA']}_{dados_dict['SERVIDOR']}_dadosBrutos.csv"
                #upload_file(arquivo_csv, bucket_name, nome_arquivo_s3)



                time.sleep(5)


