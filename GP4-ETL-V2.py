import pandas as pd
from datetime import timedelta
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




# MYSQL///////////////////

conexao = mysql.connector.connect(
    host="127.0.0.1",  
    database="smartdata",
    user="",
    password="",
    port="3306"  
)
cursor = conexao.cursor()
query = "SELECT * FROM empresa"
cursor.execute(query)
empresas = cursor.fetchall()

for linha in empresas:
    print(linha)

conexao.close()




# BUCKET////////////////////////

s3_cliente = boto3.client(
    's3',
    aws_access_key_id='',
    aws_secret_access_key='',
    aws_session_token=''
)

s3_cliente.download_file('smartdatabucket', 'raw/dados-brutos.csv', 'dados-brutos.csv')
print(f"Arquivo dados-brutos.csv baixado com sucesso.")


def upload_file(file_name, bucket, object_name=None):
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.p(file_name)
    try:
        response = s3_cliente.upload_file(file_name, bucket, object_name)
    except:
        return False
    return True


caminho_csv = 'dados-brutos.csv'
dados_brutos = pd.read_csv(caminho_csv, sep=';')

################################################################################TRATANDO OS DADOS PARA O TRATED NO BUCKETE
limite_tempo = pd.Timestamp.now() - timedelta(minutes=5)
qtd_linha_atual = len(dados_brutos) - 1
cont_linhas_soma = 0
while qtd_linha_atual >= 0:
    if(dados_brutos['DATA_HORA'][qtd_linha_atual] >= limite_tempo ):
        
        dados_brutos['DATA_HORA'] = pd.to_datetime(dados_brutos['DATA_HORA'])
        dados_tratados = dados_brutos.drop_duplicates()
        dados_tratados['EMPRESA'] = dados_brutos['EMPRESA']
        dados_tratados['REGIAO'] = dados_brutos['REGIAO']
        dados_tratados['DATACENTER'] = dados_brutos['DATACENTER']
        dados_tratados['ZONA'] = dados_brutos['ZONA']
        dados_tratados['SERVIDOR'] = dados_brutos['SERVIDOR']

        # BYTE PARA GIGABYTE
        dados_tratados['RAM_TOTAL'] = dados_brutos['RAM_TOTAL'] / (1024 ** 3)
        dados_tratados['RAM_USADA'] = dados_brutos['RAM_TOTAL'] / (1024 ** 3)
        dados_tratados['DISCO_TOTAL'] = dados_brutos['DISCO_TOTAL'] / (1024 ** 3)
        dados_tratados['DISCO_USADO'] = dados_brutos['DISCO_USADO'] / (1024 ** 3)


        dados_tratados['RAM_PERCENT'] = dados_brutos['RAM_PERCENT']
        dados_tratados['CPU'] = dados_brutos['CPU']
        dados_tratados['DISCO_PERCENT'] = dados_brutos['DISCO_PERCENT'] 
        dados_tratados['LATENCIA'] = dados_brutos['LATENCIA'] 
        dados_tratados['PACOTES_ENVIADOS'] = dados_brutos['PACOTES_ENVIADOS'] 
        dados_tratados['PACOTES_RECEBIDOS'] = dados_brutos['PACOTES_RECEBIDOS'] 
        dados_tratados['QTD_PR'] = dados_brutos['QTD_PR'] 
        dados_tratados['PROCESSO1_CPU'] = dados_brutos['PROCESSO1_CPU'] 
        dados_tratados['PORCENTAGEM_PROCESSO1_CPU'] = dados_brutos['PORCENTAGEM_PROCESSO1_CPU'] 
        dados_tratados['PROCESSO2_CPU'] = dados_brutos['PROCESSO2_CPU'] 
        dados_tratados['PORCENTAGEM_PROCESSO2_CPU'] = dados_brutos['PORCENTAGEM_PROCESSO2_CPU'] 
        dados_tratados['PROCESSO3_CPU'] = dados_brutos['PROCESSO3_CPU'] 
        dados_tratados['PORCENTAGEM_PROCESSO3_CPU'] = dados_brutos['PORCENTAGEM_PROCESSO3_CPU']
        dados_tratados['PROCESSO1_RAM'] = dados_brutos['PROCESSO1_RAM']
        dados_tratados['PORCENTAGEM_PROCESSO1_RAM'] = dados_brutos['PORCENTAGEM_PROCESSO1_RAM']
        dados_tratados['PROCESSO2_RAM'] = dados_brutos['PROCESSO2_RAM']
        dados_tratados['PORCENTAGEM_PROCESSO2_RAM'] = dados_brutos['PORCENTAGEM_PROCESSO2_RAM']
        dados_tratados['PROCESSO3_RAM'] = dados_brutos['PROCESSO3_RAM']
        dados_tratados['PORCENTAGEM_PROCESSO3_RAM'] = dados_brutos['PORCENTAGEM_PROCESSO3_RAM']

hora_agora = timedelta.now()
with open('dados-tratados.csv', 'w', newline='', encoding='utf-8') as arquivo:
    escritor = csv.writer(arquivo)
    escritor.writerows(dados_tratados)

upload_file('dados-tratados.csv', 'smartdatabucket', f'treated/dados-tratados-{hora_agora}.csv')


############################################## PARA CADA EMPRESA SERÀ GERADO TrÊS CSVS: GESTOR, ANALISTA, ESPECIFICA 

#OLHA O TXT!
#VALLE: GESTOR E DOIS ULTIMOS GRAFICOS DA ESPECIFICA
#GABRIEL: ANALISTA E KPIS E PRIMEIRO GRAFICO DA ESPECIFICA



# EXEMPLO: dados-client-empresaX-gestor-12-10-26-22:33:00
# EXEMPLO: dados-client-empresaX-analista-12-10-26-22:33:00


for empresa in empresas:
    dados_client_gestor = {}
    dados_client_analista = {}
    dados_client_ESPECIFICA = {}
    
    for linhaT in dados_tratados:
        if(linhaT['EMPRESA'] == empresa[1]):
            #MONTAR O JSON GESTOR
            #MONTAR O JSON ANALISA
            #MONTAR O JSON ESPECIFICA



            #ENVIAR O JSON GESTOR PARA O CLIENT
            #ENVIAR O JSON ANALISA PARA O CLIENT
            #ENVIAR O JSON ESPECIFICA PARA O CLIENT

    




