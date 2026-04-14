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
    password="030979@Ma",
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

s3_cliente.download_file('s3-smart-data-teste', 'raw/dados-brutos_maquina.csv', 'dados-brutos_maquina.csv')
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


caminho_csv = 'dados-brutos_maquina.csv'
dados_brutos = pd.read_csv(caminho_csv, sep=';')



######################### Tratando os dados para o treated #########################

limite_tempo = pd.Timestamp.now() - timedelta(minutes=5)
qtd_linha_atual = len(dados_brutos) - 1
cont_linhas_soma = 0


dados_brutos['DATA_HORA'] = pd.to_datetime(dados_brutos['DATA_HORA'], format="%m/%d/%y %H:%M:%S %A")

while qtd_linha_atual >= 0:
    
    
    if(dados_brutos['DATA_HORA'][qtd_linha_atual]>= limite_tempo ):

         # Pegando os dados do servidor
        
        empresa = dados_brutos['EMPRESA'][qtd_linha_atual] 
        regiao = dados_brutos['REGIAO'][qtd_linha_atual] 
        datacenter = dados_brutos['DATACENTER'][qtd_linha_atual] 
        zona = dados_brutos['ZONA'][qtd_linha_atual] 
        servidor = dados_brutos['SERVIDOR'][qtd_linha_atual] 


        porcentagem_cpu  = dados_brutos['CPU'][qtd_linha_atual] 
        quantidade_nucleos = dados_brutos['QTD_NUCLEOS'][qtd_linha_atual] 
        ram_total = dados_brutos['RAM_TOTAL'][qtd_linha_atual]  / (1024 ** 3)
        ram_usado = dados_brutos['RAM_USADA'][qtd_linha_atual]  / (1024 ** 3)
        ram_percent = dados_brutos['RAM_PERCENT'][qtd_linha_atual] 
        disco_total = dados_brutos['DISCO_TOTAL'][qtd_linha_atual]  / (1024 ** 3)
        disco_usado = dados_brutos['DISCO_USADO'][qtd_linha_atual]  / (1024 ** 3)
        disco_percent = dados_brutos['DISCO_PERCENT'][qtd_linha_atual] 


        latencia = round(dados_brutos['LATENCIA'][qtd_linha_atual] , 2)
        pacotes_enviados = dados_brutos['PACOTES_ENVIADOS'][qtd_linha_atual] 
        pacotes_recebidos = dados_brutos['PACOTES_RECEBIDOS'][qtd_linha_atual] 
        pacotes_perdidos = dados_brutos['PACOTES_PERDIDOS'][qtd_linha_atual] 




        quantidade_processos = dados_brutos['QTD_PR'][qtd_linha_atual] 
        uso_usuario = dados_brutos['USO_USER'][qtd_linha_atual] 
        uso_sistema = dados_brutos['USO_SISTEM'][qtd_linha_atual] 




        processo_1_nome_cpu = dados_brutos['PROCESSO1_CPU'][qtd_linha_atual] 
        processo_1_porcentagem_cpu = dados_brutos['PORCENTAGEM_PROCESSO1_CPU'][qtd_linha_atual] 

        processo_2_nome_cpu = dados_brutos['PROCESSO2_CPU'][qtd_linha_atual] 
        processo_2_porcentagem_cpu = dados_brutos['PORCENTAGEM_PROCESSO2_CPU'][qtd_linha_atual] 

        processo_3_nome_cpu = dados_brutos['PROCESSO3_CPU'][qtd_linha_atual] 
        processo_3_porcentagem_cpu = dados_brutos['PORCENTAGEM_PROCESSO3_CPU'][qtd_linha_atual] 



        processo_1_nome_ram = dados_brutos['PROCESSO1_RAM'][qtd_linha_atual] 
        processo_1_ram_total = dados_brutos['PORCENTAGEM_PROCESSO1_RAM'][qtd_linha_atual]  / (1024 ** 3)
        processo_1_ram_percent =  (processo_1_porcentagem_cpu * 100) / ram_total

        processo_2_nome_ram = dados_brutos['PROCESSO2_RAM'][qtd_linha_atual] 
        processo_2_ram_total = dados_brutos['PORCENTAGEM_PROCESSO2_RAM'][qtd_linha_atual]  / (1024 ** 3)
        processo_2_ram_percent =  (processo_2_porcentagem_cpu * 100) / ram_total

        processo_3_nome_ram = dados_brutos['PROCESSO3_RAM'][qtd_linha_atual] 
        processo_3_ram_total = dados_brutos['PORCENTAGEM_PROCESSO3_RAM'][qtd_linha_atual]  / (1024 ** 3)
        processo_3_ram_percent =  (processo_3_porcentagem_cpu * 100) / ram_total


        memoria_cache = dados_brutos['MEMORIA_CACHE'][qtd_linha_atual]   / (1024 ** 3)
        memoria_livre = dados_brutos['MEMORIA_LIVRE'][qtd_linha_atual]  / (1024 ** 3)
        memoria_disponivel = dados_brutos['MEMORIA_DISPONIVEL'][qtd_linha_atual]  / (1024 ** 3)




        swap_total = dados_brutos['SWAP_TOTAL'][qtd_linha_atual]  / (1024 ** 3)
        swap_em_uso = dados_brutos['SWAP_USADA'][qtd_linha_atual]  / (1024 ** 3)
        swap_livre = dados_brutos['SWAP_LIVRE'][qtd_linha_atual]  / (1024 ** 3)
        swap_percent = dados_brutos['SWAP_PERCENT'][qtd_linha_atual] 



        
        boottime = pd.to_datetime(float(dados_brutos['BOOTTIME'][qtd_linha_atual] ), unit='s')
        hora = dados_brutos['DATA_HORA'][qtd_linha_atual]
        uptime = hora - boottime
        hora_tratamento = pd.Timestamp.now()
        


        with open('dados-tratados.csv', 'a', newline='', encoding='utf-8') as arquivo:
            colunas =['EMPRESA', 'REGIAO', 'DATACENTER', 'ZONA', 'SERVIDOR', 'CPU_PER', 'QTD_NUCLEOS',
                      'RAM_TOTAL', 'RAM_USADO', 'RAM_PER', 'DISCO_TOTAL', 'DISCO_USADO', 'DISCO_PER',
                      'LATENCIA', 'PACOTES_ENV', 'PACOTES_RCB', 'PACOTES_PER', 'QTR_PR', 'USO_USER', 'USO_SISTEM',
                      'PROCESSO01_CPU_N', 'PROCESSO1_CPU_P', 'PROCESSO2_CPU_N', 'PROCESSO2_CPU_P', 'PROCESSO3_CPU_N', 'PROCESSO3_CPU_P',
                      'PROCESSO01_RAM_N','PROCESSO1_RAM_T' ,'PROCESSO1_RAM_P','PROCESSO2_RAM_T'  ,'PROCESSO2_RAM_N', 'PROCESSO2_RAM_P', 'PROCESSO3_RAM_N', 'PROCESSO3_RAM_T' ,'PROCESSO3_RAM_P',
                      'MEMORIA_CACHE_T', 'MEMORIA_CACHE_L', 'MEMORIA_CACHE_D', 'SWAP_TOTAL', 'SWAP_USO', 'SWAP_LIVRE', 'SWAP_PERCENT', 'BOOTTIME', 'DATE', 'UPTIME','HORA_TRATAMENTO'
             ]
            escritor = csv.DictWriter(arquivo, fieldnames=colunas, delimiter=';')
    
    
            if arquivo.tell() == 0:
                    escritor.writeheader()
    
            dados_tratados = {
                'EMPRESA': empresa,
                'REGIAO': regiao,
                'DATACENTER': datacenter,
                'ZONA': zona,
                'SERVIDOR': servidor,
                'CPU_PER': porcentagem_cpu,
                'QTD_NUCLEOS': quantidade_nucleos,
                'RAM_TOTAL': ram_total, 
                'RAM_USADO': ram_usado,
                'RAM_PER': ram_percent,
                'DISCO_TOTAL': disco_total,
                'DISCO_USADO': disco_usado,
                'DISCO_PER': disco_percent, 
                'LATENCIA': latencia, 
                'PACOTES_ENV': pacotes_enviados,
                'PACOTES_RCB': pacotes_recebidos,
                'PACOTES_PER': pacotes_perdidos,
                'QTR_PR': quantidade_processos,
                'USO_USER': uso_usuario,
                'USO_SISTEM': uso_sistema,
                'PROCESSO01_CPU_N': processo_1_nome_cpu,
                'PROCESSO1_CPU_P': processo_1_porcentagem_cpu,
                'PROCESSO2_CPU_N':  processo_2_nome_cpu,
                'PROCESSO2_CPU_P': processo_2_porcentagem_cpu,
                'PROCESSO3_CPU_N': processo_3_nome_cpu,
                'PROCESSO3_CPU_P': processo_3_porcentagem_cpu,
                'PROCESSO01_RAM_N': processo_1_nome_ram,
                'PROCESSO1_RAM_T': processo_1_ram_total,
                'PROCESSO1_RAM_P': processo_1_ram_percent,
                'PROCESSO2_RAM_N': processo_2_nome_ram,
                'PROCESSO2_RAM_T': processo_2_ram_total,
                'PROCESSO2_RAM_P': processo_2_ram_percent,
                'PROCESSO3_RAM_N': processo_3_nome_ram,
                'PROCESSO3_RAM_T': processo_3_ram_total,
                'PROCESSO3_RAM_P': processo_3_ram_percent, 
                'MEMORIA_CACHE_T': memoria_cache,
                'MEMORIA_CACHE_L': memoria_livre,
                'MEMORIA_CACHE_D': memoria_disponivel,
                'SWAP_TOTAL': swap_total,
                'SWAP_LIVRE': swap_livre,
                'SWAP_USO': swap_em_uso,
                'SWAP_PERCENT': swap_percent,
                'BOOTTIME': boottime,
                'DATE': hora,
                'UPTIME': uptime,
                'HORA_TRATAMENTO': hora_tratamento
            }
            escritor.writerow(dados_tratados)
            arquivo.flush()
    
    qtd_linha_atual -= 1


        
if qtd_linha_atual <= 0: 
    upload_file('dados-tratados.csv', 's3-smart-data-teste', f'treated/dados_tratados-{hora}')
    print("dados enviados com sucesso!")

else:
    print("nenhum dado encontrado")




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

"""
KPI
Processo com maior consumo (VARCHAR)
Quantidade de alertas (INT)
Indice de Confiança para Manutenção Preventiva (ICMP) (%)
Servidores Ativos (INT)
Servidores Inativos (INT)

Tendencia de degradação (DIARIA)(ULTIMO 30 DIAS) {
29/03: {QTD_SOBRECARREADO: 30, QTD_ESTRESSADOS: 20},
28/03: {QTD_SOBRECARREADO: 30, QTD_ESTRESSADOS: 20},
29/03: {QTD_SOBRECARREADO: 30, QTD_ESTRESSADOS: 20}
....
}

Pico de Uso Semanal (DIARIA)(DIA SEMANA){
CPU:   {PICO_MAXIMO: 91%, PICO_MEDIO: 77%, PICO_ MINIMO: 30%, SEG: 30%,TER: 20% . . .},
RAM:   {PICO_MAXIMO: 91%, PICO_MEDIO: 77%, PICO_ MINIMO: 30%, SEG: 30%,TER: 20% . . .},
DISCO: {PICO_MAXIMO: 91%, PICO_MEDIO: 77%, PICO_ MINIMO: 30%, SEG: 30%,TER: 20% . . .},
}

"""

dados_client_gestor  = { 
    'Kpis': {
    'processo_com_maior_uso': 'aaa',
    'porcentagem_maior_uso_ram': 'aaaa',
    'porcentagem_maior_uso_cpu': 'aaaaa', 

    'quantidade de alertas': 'aaa',
    'indice de confiança para manutenção preventiva': 'aaa', 
    'servidores ativos': 'aaa',
    'servidores inativos': 'aaa'
    },
    'Tendencia_Degradacao': {
        
    } 
}

"""
****GESTOR PT2
--KPI
Processo com maior consumo (VARCHAR)
Quantidade de alertas (INT)
Indice de Confiança para Manutenção Preventiva (ICMP) (%)
Servidores Ativos (INT)
Servidores Inativos (INT)

Uso de Disco vs Rede (HORA)(ULTIMO 24 HRS) {
00:00: {UsoDisco: 30%, TrafegoRede: 20%},
03:00: {UsoDisco: 50%, TrafegoRede: 30%},
05:00: {UsoDisco: 70%, TrafegoRede: 20%},
...
}
Pico de Disco (%)
Horário do pico de Disco (VARCHAR)
Pico de Rede (%)
Horário do pico de Rede (VARCHAR)

Tempo de atividade vc Latência (HORA)(ULTIMO 24 HRS) {
00:00: {Uptime: 30%, Latencia: 20%},
03:00: {Uptime: 50%, Latencia: 30%},
05:00: {Uptime: 70%, Latencia: 20%},
...
}
Servidor com menor UPTIME (VARCHAR)
Horário do servidor com menor UPTIME (VARCHAR)
Pico de Latência (MS)

Servidores online (INT)
Servidores críticos (INT)
"""





########### JSON ANALISA

#Querys

query = "SELECT * FROM zonas"
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



for zona in zonas:
    nomeZona = zona[2]
    idZona = zona[0]

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
    agora = datetime.now()
    limite_tempo = agora - timedelta(minutes=5)
    df_ultimos5M = dados_tratados[dados_tratados['DATA_HORA'] >= limite_tempo]
    if not df_ultimos5M.empty:
        p99Ram_Perc = df_ultimos5M['RAM_PERCENT'].quantile(0.99)
    else:
        p99Ram_Perc = 0.0

    #KPI4 p99CPU_Perc

dados_client_analista = {
    'KPIS': {
        'Total de servidores (QTD)': total_servidores,
        'Servidores Inativos (QTD)': servidores_inativos,
        'P99 da RAM (%)': p99Ram_Perc,
        'P99 da CPU (%)': p99CPU_Perc,
        'Uso Disco (%)': UsoDisco_Perc,
        'Uso Disco (TB)': UsoDisco_TB,
        'Total Disco (TB)': TotalDisco_TB,
        'Servidores baixa latencia (QTD)': Qtd_serv_baixaLatencia
    }
}


#ENVIAR O JSON ANALISA PARA O CLIENT

#upload_file(f'dados-clint-{linhaT['EMPRESA']}-analista.json', 's3-smart-data-teste', f'treated/dados-client-{linhaT['EMPRESA']}-analista-{timedelta.now()}')
    
    
    
    
    

            
            #MONTAR O JSON GESTOR
            #MONTAR O JSON ESPECIFICA



            #ENVIAR O JSON GESTOR PARA O CLIENT
            #ENVIAR O JSON ESPECIFICA PARA O CLIENT


    







#Fechar a conexão com o mysql
conexao.close()