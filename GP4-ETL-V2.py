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
    password="102309",
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
        processo_1_ram_percent =  (processo_1_ram_total * 100) / ram_total

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
    upload_file('dados-tratados.csv', bucket_name, f'treated/dados_tratados-{hora}')
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
    df_ultimos5M = dados_tratados[dados_tratados['DATA_HORA'] >= limite_tempo]
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


    # =============
    # GRAFICOS
    # =============

    # UsoMedioRecursos (Media da zona toda)
    uso_medio_cpu = df_ultimos5MZonaX['CPU'].mean()
    uso_medio_ram = df_ultimos5MZonaX['RAM_PERCENT'].mean()
    uso_medio_disco = df_ultimos5MZonaX['DISCO_PERCENT'].mean() 

    # qtdTotalServidores 
    qtdTotalServidores = df_ultimos5MZonaX['SERVIDOR'].nunique()

    # servidoresCriticos
    # média dos últimos 5 min de CADA servidor individualmente
    df_media_servers = df_ultimos5MZonaX.groupby('SERVIDOR').mean(numeric_only=True)
    
    # Definindo a regra crítico se CPU > 85% OU RAM > 85% 
    criticos = df_media_servers[(df_media_servers['CPU'] > 85) | (df_media_servers['RAM_PERCENT'] > 85)]
    listaServersCriticos = criticos.index.tolist() # Retorna uma lista: ['AB043', 'AB045']


    # ------------------------------------------
    # TOP 3 PROCESSOS - RAM
    # ------------------------------------------
    # Como os processos estão em 3 colunas separadas, nós empilhamos eles para o Pandas conseguir rankear
    p1_ram = df_ultimos5MZonaX[['PROCESSO01_RAM_N', 'PROCESSO1_RAM_T']].rename(columns={'PROCESSO01_RAM_N': 'NOME', 'PROCESSO1_RAM_T': 'USO_GB'})
    p2_ram = df_ultimos5MZonaX[['PROCESSO2_RAM_N', 'PROCESSO2_RAM_T']].rename(columns={'PROCESSO2_RAM_N': 'NOME', 'PROCESSO2_RAM_T': 'USO_GB'})
    p3_ram = df_ultimos5MZonaX[['PROCESSO3_RAM_N', 'PROCESSO3_RAM_T']].rename(columns={'PROCESSO3_RAM_N': 'NOME', 'PROCESSO3_RAM_T': 'USO_GB'})
    
    todos_processos_ram = pd.concat([p1_ram, p2_ram, p3_ram])
    
    # Agrupa pelo nome do processo e tira a média de uso dele na zona, pegando os 3 maiores
    top3_ProcessosUsoRam = todos_processos_ram.groupby('NOME')['USO_GB'].mean().nlargest(3)

    # somaTop3_ProcUsoRamGB (Soma do uso médio em GB dos 3 processos mais pesados)
    somaTop3_ProcUsoRamGB = top3_ProcessosUsoRam.sum()

    # porcentagemSomaTop_ProcUsoRam3GB
    # Pegamos o Total de RAM Física dessa Zona (Média de RAM Total de um servidor * Qtd Servidores)
    ram_fisica_total_zona = df_ultimos5MZonaX['RAM_TOTAL'].mean() * qtdTotalServidores
    porcentagemSomaTop_ProcUsoRam = (somaTop3_ProcUsoRamGB / ram_fisica_total_zona) * 100


    # ------------------------------------------
    # TOP 3 PROCESSOS - CPU
    # ------------------------------------------
    # Mesma lógica, mas empilhando as colunas de CPU
    p1_cpu = df_ultimos5MZonaX[['PROCESSO01_CPU_N', 'PROCESSO1_CPU_P']].rename(columns={'PROCESSO01_CPU_N': 'NOME', 'PROCESSO1_CPU_P': 'USO_PERC'})
    p2_cpu = df_ultimos5MZonaX[['PROCESSO2_CPU_N', 'PROCESSO2_CPU_P']].rename(columns={'PROCESSO2_CPU_N': 'NOME', 'PROCESSO2_CPU_P': 'USO_PERC'})
    p3_cpu = df_ultimos5MZonaX[['PROCESSO3_CPU_N', 'PROCESSO3_CPU_P']].rename(columns={'PROCESSO3_CPU_N': 'NOME', 'PROCESSO3_CPU_P': 'USO_PERC'})
    
    todos_processos_cpu = pd.concat([p1_cpu, p2_cpu, p3_cpu])
    
    top3_ProcessosUsoCPU = todos_processos_cpu.groupby('NOME')['USO_PERC'].mean().nlargest(3)
    
    # porcentagemSomaTop_ProcUsoCPU
    porcentagemSomaTop_ProcUsoCPU = top3_ProcessosUsoCPU.sum()



    # ==========================================
    # MONTANDO O DICIONÁRIO NO FINAL DO LOOP
    # ==========================================
    
    dados_client_analista[nomeZona] = {
        'KPIS': {
            'Total de servidores (QTD)': int(total_servidores),
            'Servidores Inativos (QTD)': int(servidores_inativos),
            'P99 da RAM (%)': round(p99Ram_Perc, 2),
            'P99 da CPU (%)': round(p99CPU_Perc, 2),
            'Uso Disco (%)': round(uso_medio_disco, 2),
            'Uso Disco (TB)': round(UsoDisco_TB, 4),
            'Total Disco (TB)': round(TotalDisco_TB, 4),
            'Servidores baixa latencia (QTD)': int(Qtd_serv_baixaLatencia)
        },
        'GRAFICOS': {
            'Servidores Criticos': listaServersCriticos, 
            'Qtd total servidores': int(qtdTotalServidores),
            'Uso Medio CPU': round(uso_medio_cpu, 2),
            'Uso Medio RAM': round(uso_medio_ram, 2),
            
            
            'Top 3 Processos RAM (GB)': top3_ProcessosUsoRam.to_dict(),
            'Soma Top 3 RAM (GB)': round(somaTop3_ProcUsoRamGB, 2),
            'Top 3 RAM (%)': round(porcentagemSomaTop_ProcUsoRam, 2),
            
            'Top 3 Processos CPU (%)': top3_ProcessosUsoCPU.to_dict(),
            'Soma Top 3 CPU (%)': round(porcentagemSomaTop_ProcUsoCPU, 2)
        }
    }

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