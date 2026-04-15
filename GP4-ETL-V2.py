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
arquivo_download = 'dados-brutos_STEAM.csv'
try:
    s3_cliente.download_file(bucket_name, f'raw/{arquivo_download}', 'dados_brutos.csv')
    print(f"Arquivo dados-brutos.csv baixado com sucesso.")
except Exception as e:
    print(f"Não foi encontrado o arquivo: {e}")


def upload_file(file_name, bucket, object_name=None):
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.p(file_name)
    try:
        response = s3_cliente.upload_file(file_name, bucket, object_name)
    except:
        return False
    return True


caminho_csv = 'dados_brutos.csv'
dados_brutos = pd.read_csv(caminho_csv, sep=';')



######################### Tratando os dados para o treated #########################

dados_brutos['DATA_HORA'] = pd.to_datetime(dados_brutos['DATA_HORA'], format="%m/%d/%y %H:%M:%S %A")
limite_tempo = pd.Timestamp.now() - timedelta(minutes=5)
df_filtrado = dados_brutos[dados_brutos['DATA_HORA'] >= limite_tempo].copy()

print(f"Encontradas {len(df_filtrado)} linhas para processar.")

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
    query = f"SELECT count(idServidor) FROM servidor where fkZona = {idZona}"
    cursor.execute(query)
    queryAtual = cursor.fetchall()
    total_servidores = queryAtual[0][0]
    
    
    #KPI2 servidores_inativos
    query = f"SELECT count(idServidor) FROM servidor where fkZona = {idZona} AND estado = 'Inativo'"
    cursor.execute(query)
    queryAtual = cursor.fetchall()
    servidores_inativos = queryAtual[0][0]
    

    #KPI3 p99Ram_Perc
    p99Ram_Perc = df_ultimos5MZonaX['RAM_PERCENT'].quantile(0.99)

    #KPI4 p99CPU_Perc
    p99CPU_Perc = df_ultimos5MZonaX['CPU'].quantile(0.99)

    #KPI5 UsoDisco_Perc
    UsoDisco_Perc = df_ultimos5MZonaX['DISCO_PERCENT'].max()

    #KPI6 UsoDisco_TB 
    UsoDisco_TB = (df_ultimos5MZonaX['DISCO_USADO'] / (1024 ** 4)).max()

    #KPI7 TotalDisco_TB
    TotalDisco_TB = (df_ultimos5MZonaX['DISCO_TOTAL'] / (1024 ** 4)).max()

    #KPI8 Qtd_serv_baixaLatencia
    Qtd_serv_baixaLatencia = (df_ultimos5MZonaX['LATENCIA'] < 50).sum()


    # =====================
    # GRAFICOS
    # =====================

    # UsoMedioRecursos (Media da zona toda)
    uso_medio_cpu = df_ultimos5MZonaX['CPU'].mean() 
    uso_medio_ram = df_ultimos5MZonaX['RAM_PERCENT'].mean()
    uso_medio_disco = df_ultimos5MZonaX['DISCO_PERCENT'].mean() 

    # qtdTotalServidores 
    qtdTotalServidores = df_ultimos5MZonaX['SERVIDOR'].nunique()

    # servidoresCriticos
    df_media_servers = df_ultimos5MZonaX.groupby('SERVIDOR').mean(numeric_only=True)
    
    # Definindo a regra crítico se CPU > 85% OU RAM > 85% 
    criticos = df_media_servers[(df_media_servers['CPU'] > 85) | (df_media_servers['RAM_PERCENT'] > 85)]
    listaServersCriticos = criticos.index.tolist() # Retorna uma lista: ['AB043', 'AB045']

    # ------------------------------------------
    # TOP 3 PROCESSOS - RAM
    # ------------------------------------------

    # Como os processos estão em 3 colunas separadas, nós empilhamos eles para o Pandas conseguir rankear
    p1_ram = df_ultimos5MZonaX[['PROCESSO1_RAM', 'PORCENTAGEM_PROCESSO1_RAM']].rename(columns={'PROCESSO1_RAM': 'NOME', 'PORCENTAGEM_PROCESSO1_RAM': 'USO_BYTES'})
    p2_ram = df_ultimos5MZonaX[['PROCESSO2_RAM', 'PORCENTAGEM_PROCESSO2_RAM']].rename(columns={'PROCESSO2_RAM': 'NOME', 'PORCENTAGEM_PROCESSO2_RAM': 'USO_BYTES'})
    p3_ram = df_ultimos5MZonaX[['PROCESSO3_RAM', 'PORCENTAGEM_PROCESSO3_RAM']].rename(columns={'PROCESSO3_RAM': 'NOME', 'PORCENTAGEM_PROCESSO3_RAM': 'USO_BYTES'})

    todos_processos_ram = pd.concat([p1_ram, p2_ram, p3_ram])
    
    todos_processos_ram['USO_GB'] = todos_processos_ram['USO_BYTES'] / (1024 ** 3)

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
    p1_cpu = df_ultimos5MZonaX[['PROCESSO1_CPU', 'PORCENTAGEM_PROCESSO1_CPU']].rename(columns={'PROCESSO1_CPU': 'NOME', 'PORCENTAGEM_PROCESSO1_CPU': 'USO_PERC'})
    p2_cpu = df_ultimos5MZonaX[['PROCESSO2_CPU', 'PORCENTAGEM_PROCESSO2_CPU']].rename(columns={'PROCESSO2_CPU': 'NOME', 'PORCENTAGEM_PROCESSO2_CPU': 'USO_PERC'})
    p3_cpu = df_ultimos5MZonaX[['PROCESSO3_CPU', 'PORCENTAGEM_PROCESSO3_CPU']].rename(columns={'PROCESSO3_CPU': 'NOME', 'PORCENTAGEM_PROCESSO3_CPU': 'USO_PERC'})

    todos_processos_cpu = pd.concat([p1_cpu, p2_cpu, p3_cpu])
    
    top3_ProcessosUsoCPU = todos_processos_cpu.groupby('NOME')['USO_PERC'].mean().nlargest(3)
    
    # porcentagemSomaTop_ProcUsoCPU
    porcentagemSomaTop_ProcUsoCPU = top3_ProcessosUsoCPU.sum()


    print("Servidores criticos")
    print(listaServersCriticos)
    print("qtdTotalServidores")
    print(qtdTotalServidores)
    print("top3_ProcessosUsoRam")
    print(top3_ProcessosUsoRam)
    print("top3_ProcessosUsoCPU")
    print(top3_ProcessosUsoCPU)

    
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