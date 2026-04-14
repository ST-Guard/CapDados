import pandas as pd
from datetime import datetime, timedelta
import time 
import csv
import boto3
import os
import mysql.connector
#pip install mysql-connector-python
#pip install pandas



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




##### Acesso ao banco de dados

conexao = mysql.connector.connect(
    host="127.0.0.1",  
    database="smartData",
    user="root",
    password="",
    port="3306"  
)
cursor = conexao.cursor()
query = "SELECT * FROM empresa"
cursor.execute(query)
empresas = cursor.fetchall()

for linha in empresas:
    print(linha)



##### Acesso ao bucket

s3_cliente = boto3.client(
    's3',
    aws_access_key_id='',
    aws_secret_access_key='',
    aws_session_token=''
)

bucket_name = 'smartdatabucket1'
try:
    s3_cliente.download_file(bucket_name, 'raw/dados-brutos_maquina.csv', 'dados-brutos_maquina.csv')
    print(f"Arquivo dados-brutos.csv baixado com sucesso.")
except:
    print("Não foi encontrado o arquivo")


def upload_file(file_name, bucket, object_name=None):
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.p(file_name)
    try:
        response = s3_cliente.upload_file(file_name, bucket, object_name)
    except:
        return False
    return True


caminho_csv = 'dados-brutos_maquina.csv'
dados_brutos = pd.read_csv(caminho_csv, sep=';')



######################### Tratando os dados para o treated #########################

dados_brutos['DATA_HORA'] = pd.to_datetime(dados_brutos['DATA_HORA'], format="%m/%d/%y %H:%M:%S %A")
limite_tempo = pd.Timestamp.now() - timedelta(minutes=5)
df_filtrado = dados_brutos[dados_brutos['DATA_HORA'] >= limite_tempo].copy()


if df_filtrado.empty:
    print("Nenhum dado encontrado nos ultimos 5 minutos")
else:
    print(f"Encontradas {len(df_filtrado)} linhas para processar.")

    df_filtrado['RAM_TOTAL_GB'] = df_filtrado['RAM_TOTAL'] / (1024 ** 3)
    df_filtrado['RAM_USADA_GB'] = df_filtrado['RAM_USADA'] / (1024 ** 3)
    df_filtrado['DISCO_TOTAL_GB'] = df_filtrado['DISCO_TOTAL'] / (1024 ** 3)
    df_filtrado['DISCO_USADO_GB'] = df_filtrado['DISCO_USADO'] / (1024 ** 3)
    
    df_filtrado['LATENCIA'] = df_filtrado['LATENCIA'].round(2)
    
    df_filtrado['PROCESSO1_RAM_GB'] = df_filtrado['PORCENTAGEM_PROCESSO1_RAM'] / (1024 ** 3)
    df_filtrado['PROCESSO2_RAM_GB'] = df_filtrado['PORCENTAGEM_PROCESSO2_RAM'] / (1024 ** 3)
    df_filtrado['PROCESSO3_RAM_GB'] = df_filtrado['PORCENTAGEM_PROCESSO3_RAM'] / (1024 ** 3)

    df_filtrado['PROCESSO1_RAM_PERC'] = (df_filtrado['PROCESSO1_RAM_GB'] * 100) / df_filtrado['RAM_TOTAL_GB']
    df_filtrado['PROCESSO2_RAM_PERC'] = (df_filtrado['PROCESSO2_RAM_GB'] * 100) / df_filtrado['RAM_TOTAL_GB']
    df_filtrado['PROCESSO3_RAM_PERC'] = (df_filtrado['PROCESSO3_RAM_GB'] * 100) / df_filtrado['RAM_TOTAL_GB']

    #TEMPOOOOO
    df_filtrado['BOOTTIME_DT'] = pd.to_datetime(df_filtrado['BOOTTIME'], unit='s')
    df_filtrado['UPTIME'] = df_filtrado['DATA_HORA'] - df_filtrado['BOOTTIME_DT']
    df_filtrado['HORA_TRATAMENTO'] = pd.Timestamp.now()

    
    colunas_finais = {
        'EMPRESA': 'EMPRESA', 'REGIAO': 'REGIAO', 'DATACENTER': 'DATACENTER', 'ZONA': 'ZONA', 'SERVIDOR': 'SERVIDOR',
        'CPU': 'CPU_PER', 'QTD_NUCLEOS': 'QTD_NUCLEOS', 'RAM_TOTAL_GB': 'RAM_TOTAL', 'RAM_USADA_GB': 'RAM_USADO', 
        'RAM_PERCENT': 'RAM_PER', 'DISCO_TOTAL_GB': 'DISCO_TOTAL', 'DISCO_USADO_GB': 'DISCO_USADO', 'DISCO_PERCENT': 'DISCO_PER',
        'LATENCIA': 'LATENCIA', 'PACOTES_ENVIADOS': 'PACOTES_ENV', 'PACOTES_RECEBIDOS': 'PACOTES_RCB', 'PACOTES_PERDIDOS': 'PACOTES_PER',
        'QTD_PR': 'QTR_PR', 'USO_USER': 'USO_USER', 'USO_SISTEM': 'USO_SISTEM',
        'PROCESSO1_CPU': 'PROCESSO01_CPU_N', 'PORCENTAGEM_PROCESSO1_CPU': 'PROCESSO1_CPU_P',
        'PROCESSO2_CPU': 'PROCESSO2_CPU_N', 'PORCENTAGEM_PROCESSO2_CPU': 'PROCESSO2_CPU_P',
        'PROCESSO3_CPU': 'PROCESSO3_CPU_N', 'PORCENTAGEM_PROCESSO3_CPU': 'PROCESSO3_CPU_P',
        'PROCESSO1_RAM': 'PROCESSO01_RAM_N', 'PROCESSO1_RAM_GB': 'PROCESSO1_RAM_T', 'PROCESSO1_RAM_PERC': 'PROCESSO1_RAM_P',
        'PROCESSO2_RAM': 'PROCESSO2_RAM_N', 'PROCESSO2_RAM_GB': 'PROCESSO2_RAM_T', 'PROCESSO2_RAM_PERC': 'PROCESSO2_RAM_P',
        'PROCESSO3_RAM': 'PROCESSO3_RAM_N', 'PROCESSO3_RAM_GB': 'PROCESSO3_RAM_T', 'PROCESSO3_RAM_PERC': 'PROCESSO3_RAM_P',
        'BOOTTIME_DT': 'BOOTTIME', 'DATA_HORA': 'DATE', 'UPTIME': 'UPTIME', 'HORA_TRATAMENTO': 'HORA_TRATAMENTO'
    }


    df_silver = df_filtrado.rename(columns=colunas_finais)[list(colunas_finais.values())]

  
    df_silver.to_csv('dados-tratados.csv', sep=';', index=False, mode='a', header=not pd.io.common.file_exists('dados-tratados.csv'))

    
    hora_envio = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S') 
    upload_file('dados-tratados.csv', bucket_name, f'treated/dados_tratados-{hora_envio}.csv')
    print("Dados tratados e enviados com sucesso!")




######################### Tratando os dados para o client #########################



#PARA CADA EMPRESA SERÀ GERADO TRÊS CSVS: GESTOR, ANALISTA, ESPECIFICA 
#OLHAR O TXT!
#VALLE: GESTOR E DOIS ULTIMOS GRAFICOS DA ESPECIFICA
#GABRIEL: ANALISTA E KPIS E PRIMEIRO GRAFICO DA ESPECIFICA

#!!!!Gabriel  troquei o jeito dos comentarios
#EU VIIII, mas mudei um tico tbm

# EXEMPLO: dados-client-empresaX-gestor-12-10-26-22:33:00
# EXEMPLO: dados-client-empresaX-analista-12-10-26-22:33:00


dados_client_gestor = {}
dados_client_analista = {}
dados_client_ESPECIFICA = {}




########### JSON ANALISA


########KPIS
#Total de servidores com servidores inativos
#R: MYSQL
#P99 da RAM (%) (99% do tempo está x valor de RAM)
#R: Ultimos 5 minutos
#P99 do CPU (%) (99% do tempo está x valor de CPU)
#R: Ultimos 5 minutos
#Uso do Disco (%) com a quantidade e o total
#R: Ultimos 5 minutos
#Quantidade de servidores com baixa latência 
#R: Ultimos 5 minutos

########GRAFICOS
#Todos os servidores mais criticos (Ranking (>85% uso))
#R: Ultimos 5 minutos
# Servidores_críticos(ORDENADO){
# SERVIDOR1: {USO_CPU: 90%, USO_RAM: 90%},
# SERVIDOR1: {USO_CPU: 80%, USO_RAM: 80%},
# ...
# }

#Quantidade de servidores estressados VS quantidade de servidores sobrecarregados
#Estressados (Stress): Servidores com uso alto de recursos (ex: CPU > 80%), mas que ainda processam requisições sem erro.
#Sobrecarregados (Overloaded): Servidores que atingiram o limite e estão apresentando latência alta ou erros de timeout (5xx).

#Gráfico Top 3 processos que mais estão utilizando RAM
#R: Ultimos 5 minutos

#Gráfico Top 3 processos que mais estão utilizando RAM
#R: Ultimos 5 minutos

#Querys

query = "SELECT * FROM zona"
cursor.execute(query)
zonas = cursor.fetchall()



#KPIs
total_servidores = 0
servidores_inativos = 0
p99Ram_Perc = 0
p99CPU_Perc = 0
UsoDisco_Perc = 0
UsoDisco_TB = 0
TotalDisco_TB = 0
Qtd_serv_baixaLatencia = 0

#GRAFICOS
servidoresCriticos = 0
qtdTotalServidores = 0

top3_ProcessosUsoRam = 0
somaTop3_ProcUsoRamGB = 0
porcentagemSomaTop_ProcUsoRam3GB = 0

top3_ProcessosUsoCPU = 0
porcentagemSomaTop_ProcUsoCPU3GB = 0

UsoMedioRecursos = 0


for zona in zonas:
    nomeZona = zona[2]
    idZona = zona[0]
    agora = datetime.now()
    limite_tempo = agora - timedelta(minutes=5)
    df_ultimos5M = df_filtrado[df_filtrado['DATA_HORA'] >= limite_tempo]
    if df_ultimos5M.empty:
        break
    df_ultimos5MZonaX = df_ultimos5M[df_ultimos5M['ZONA'] == nomeZona]
    
    # =============
    # KPI
    # =============

    #KPI1 total_servidores
    query = f"SELECT count(idServidores) FROM servidor where fkZona = {idZona}"
    cursor.execute(query)
    queryAtual = cursor.fetchall()
    total_servidores = queryAtual[0]
    
    #KPI2 servidores_inativos
    query = f"SELECT count(idServidores) FROM servidor where fkZona = {idZona} AND estado = 'Inativo'"
    cursor.execute(query)
    queryAtual = cursor.fetchall()
    servidores_inativos = queryAtual[0]

    #KPI3 p99Ram_Perc
    p99Ram_Perc = df_ultimos5MZonaX['RAM_PERCENT'].quantile(0.99)

    #KPI4 p99CPU_Perc
    p99CPU_Perc = df_ultimos5MZonaX['CPU'].quantile(0.99)

    #KPI5 UsoDisco_Perc
    UsoDisco_Perc = df_ultimos5MZonaX['DISCO_PERCENT']

    #KPI6 UsoDisco_TB 
    UsoDisco_TB = (df_ultimos5MZonaX['DISCO_USADO'] / (1024 ** 4)).max()

    #KPI7 TotalDisco_TB
    TotalDisco_TB = (df_ultimos5MZonaX['DISCO_TOTAL'] / (1024 ** 4)).max()

    #KPI8 Qtd_serv_baixaLatencia
    Qtd_serv_baixaLatencia = (df_ultimos5MZonaX['LATENCIA'] < 50).sum()


    

#TESTE
#ENVIAR O JSON ANALISA PARA O CLIENT

#upload_file(f'dados-clint-{linhaT['EMPRESA']}-analista.json', 's3-smart-data-teste', f'treated/dados-client-{linhaT['EMPRESA']}-analista-{timedelta.now()}')
    
    
    
    
    

            
            #MONTAR O JSON GESTOR
            #MONTAR O JSON ESPECIFICA



            #ENVIAR O JSON GESTOR PARA O CLIENT
            #ENVIAR O JSON ESPECIFICA PARA O CLIENT


    







#Fechar a conexão com o mysql
conexao.close()
###################################