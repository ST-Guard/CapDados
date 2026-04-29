import pandas as pd
from datetime import datetime, timedelta
import time 
import csv
import boto3
import os
import mysql.connector
import json
import numpy as np
from dotenv import load_dotenv
from colorama import Fore, Style, init
#pip install colorama
#pip install python-dotenv
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


#####
#####
#####
#####
#####
##### PARTE 1 - CONFIGURAÇÕES DE USUARIO/ARQUIVOS
#####
#####
#####
#####
#####


# RODANDO O ARQUIVO .ENV
load_dotenv()


# ACESSO AO BANCO DE DADOS
banco_host = os.getenv('DB_HOST')
banco_user = os.getenv('DB_USER')
banco_senha = os.getenv('DB_PASSWORD')
banco_nome = os.getenv('DB_NAME')
banco_porta = int(os.getenv('DB_PORT'))
try:
    conexao = mysql.connector.connect(
            host='127.0.0.1',
            user=banco_user,
            password=banco_senha,
            database=banco_nome,
            port=banco_porta
    )
except Exception as e:
        raise ValueError(f"Erro ao conectar ou buscar no banco de dados: {e}")
     
cursor = conexao.cursor()
query = "SELECT * FROM empresa"
cursor.execute(query)
empresas = cursor.fetchall()



# ACESSO AO BUCKET
chave_acesso = os.getenv('AWS_ACCESS_KEY_ID')
chave_secreta = os.getenv('AWS_SECRET_ACCESS_KEY')
token_sessao = os.getenv('AWS_SESSION_TOKEN')
s3_cliente = boto3.client(
        's3',
        aws_access_key_id=chave_acesso,
        aws_secret_access_key=chave_secreta,
        aws_session_token=token_sessao 
    )



# DEFININDO A FUNÇÃO DE CONEXÃO DO BOTO3
def upload_file(file_name, bucket, object_name=None):
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.p(file_name)
    try:
        print("mandado pra aws")
        response = s3_cliente.upload_file(file_name, bucket, object_name)
    except:
        print("nao foi enviado")
        return False
    return True





# CONVERTE OS TIPOS DE NUMEROS DO NUMPY, PARA QUE ELES POSSAM SER ENVIADOS AO JSON
def json_serial(obj):
    import numpy as np
    if isinstance(obj, (np.float64, np.float32)):
        return float(obj)
    raise TypeError(f"Tipo {type(obj)} não é serializável")





# INICIANDO O ETL NA CAMADA SILVER
while True:
    print(Fore.WHITE + "\n ---------- INICIANDO CICLO DE PROCESSAMENTO ETL (A cada 5 minutos) ----------" + Style.RESET_ALL)
    print()


#####
#####
#####
#####
#####   
##### PARTE 2 - EXTRAÇÃO
#####
#####
#####
#####


   
   # DEFININDO O NOME DO BUCKET E DA PASTA ONDE OS ARQUIVOS DO RAW  VÃO SER EXTRAIDOS 
    bucket_name = 's3-smart-data-teste'
    prefixo_pasta = 'raw/'
    pasta_local_destino = 'dados_brutos_raw'
    

    # TENTANDO LISTAR TODOS OS OS OBJETOS DO RAW
    try:
        
        resposta_s3 = s3_cliente.list_objects_v2(Bucket=bucket_name, Prefix=prefixo_pasta)
    except Exception as e:
        print(f"Erro ao acessar o bucket S3: {e}")
        conexao.close()
        continue

    # VERIFICANDO SE EXISTEM ARQUIVOS NA PASTA RAW
    if 'Contents' not in resposta_s3:
        print("Nenhum arquivo encontrado na pasta raw. Esperando proximo ciclo")
        conexao.close()
        continue
    

    # CRIANDO A PASTA ONDE OS ARQUIVOS DA RAW SERÃO INSTALADOS
    os.makedirs(pasta_local_destino, exist_ok=True)

    # DEFININDO A LISTA ONDE SERÃO JUNTADOS EM TODOS OS DATAFRAMES
    lista_dataframes = [] 
    
    print("\033[38;5;130m============================== CAMADA RAW ============================== \033[0m")


    # PEGANDO A LISTA DO LIST OBJECTS E ACESSANDO UMA POR UMA
    for resposta in resposta_s3['Contents']:

        # ACESSANDO O ID DO ARQUIVO EM ESPECIFICO
        caminho_s3 = resposta['Key'] 
        
        # O S3 as vezes retorna a propria pasta vazia como um respostaeto. Só ignora
        if caminho_s3.endswith('/') or resposta['Size'] == 0:
            continue
        

        # CRIA UM NOME TEMPORARIO PARA O ARQUIVO
        nome_puro = os.path.basename(caminho_s3)
        # CRIA UM NOME LOCAL TEMPORARIO PARA O ARQUIVO
        nome_arquivo_local = os.path.join(pasta_local_destino, nome_puro)
        

        # TENTANDO FAZER O DOWNLOAD DO ARQUIVO E LENDO 
        try:
            print(f"Baixando arquivo: {caminho_s3} ...")
            s3_cliente.download_file(bucket_name, caminho_s3, nome_arquivo_local)
            

            # LENDO OS ARQUIVOS E COLOCANDO NA LISTA DOS DATAFRAMES
            df_temp = pd.read_csv(nome_arquivo_local, sep=';')
            lista_dataframes.append(df_temp)
            
            
        except Exception as e:
            print(f"Erro ao baixar ou ler o arquivo {caminho_s3}: {e}")

   
    # JUNTAR TUDO NO PANDAS E COMEÇAR O TRATAMENTO
    if not lista_dataframes:
        print("Nenhum CSV válido foi lido. Aguardando proximo ciclo...")
        conexao.close()
        time.sleep(120)
        continue

    # JUNTA TOODS OS DATAFRAMES EM APENAS 1 SÓ PARA A INICIAÇÃO DA CAMADA SILVER
    dados_brutos = pd.concat(lista_dataframes, ignore_index=True)
    print(Fore.GREEN + f"Sucesso! {len(lista_dataframes)} arquivos combinados. Total de {len(dados_brutos)} linhas." + Style.RESET_ALL)


#####
#####
#####
#####
#####
# PARTE 3 INICIO DA CAMADA SILVER
#####
#####
#####
#####
#####

    print("\033[37m============================== CAMADA TRUSTED ==============================  \033[0m")

    # TRANSFORMANDO A DATA DOS DADOS BRUTOS DE STRING PARA TIPO DATA
    dados_brutos['DATA_HORA'] = pd.to_datetime(dados_brutos['DATA_HORA'], format="mixed", errors='coerce')

    # DEFININDO O LIMITE DE TEMPO
    limite_tempo = pd.Timestamp.now() - timedelta(minutes=5)
    df_filtrado = dados_brutos[dados_brutos['DATA_HORA'] >= limite_tempo].copy()


    #  PEGANDO OS DADOS FILTRADOS QUE ESTÃO NO LIMITE DE TEMPO E TRANSFORMANDO ELES EM DADOS ESPECIFICOS
    if df_filtrado.empty:
        print("Nenhum dado encontrado nos ultimos 5 minutos")
    else:
        print(f"Encontradas {len(df_filtrado)} linhas para processar.")

        df_filtrado['RAM_TOTAL_GB'] = (df_filtrado['RAM_TOTAL'] / (1024 ** 3)).round(2)
        df_filtrado['RAM_USADA_GB'] = (df_filtrado['RAM_USADA'] / (1024 ** 3)).round(2)
        df_filtrado['DISCO_TOTAL_GB'] = (df_filtrado['DISCO_TOTAL'] / (1024 ** 3)).round(2)
        df_filtrado['DISCO_USADO_GB'] = (df_filtrado['DISCO_USADO'] / (1024 ** 3)).round(2)
        df_filtrado['LATENCIA'] = df_filtrado['LATENCIA'].round(2)
        df_filtrado['PROCESSO1_RAM_GB'] = (df_filtrado['PORCENTAGEM_PROCESSO1_RAM'] / (1024 ** 3)).round(2)
        df_filtrado['PROCESSO2_RAM_GB'] = (df_filtrado['PORCENTAGEM_PROCESSO2_RAM'] / (1024 ** 3)).round(2)
        df_filtrado['PROCESSO3_RAM_GB'] = (df_filtrado['PORCENTAGEM_PROCESSO3_RAM'] / (1024 ** 3)).round(2)
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
        upload_file('dados-tratados.csv', bucket_name, f'treated/dados_tratados.csv')
        print(Fore.GREEN +"Dados tratados e enviados com sucesso para a AWS!"+ Style.RESET_ALL)



    # TRATANDO OS DADOS DO DIA ANTERIOR


    hoje = pd.Timestamp.now().normalize()
    ontem = hoje - timedelta(days=1)
    # o normalize serve para colocar sempre a data em 00:00:00
    df_filtrado1D = dados_brutos[(dados_brutos['DATA_HORA'] >= ontem) & (dados_brutos['DATA_HORA'] < hoje)].copy()

    if df_filtrado1D.empty:
        print("Nas 24 horas anteriores")
    else:
        print(f"Encontradas {len(df_filtrado)} linhas para processar.")

        df_filtrado1D['RAM_TOTAL_GB'] = (df_filtrado1D['RAM_TOTAL'] / (1024 ** 3)).round(2)
        df_filtrado1D['RAM_USADA_GB'] = (df_filtrado1D['RAM_USADA'] / (1024 ** 3)).round(2)
        df_filtrado1D['DISCO_TOTAL_GB'] = (df_filtrado1D['DISCO_TOTAL'] / (1024 ** 3)).round(2)
        df_filtrado1D['DISCO_USADO_GB'] = (df_filtrado1D['DISCO_USADO'] / (1024 ** 3)).round(2)
        df_filtrado1D['LATENCIA'] = df_filtrado1D['LATENCIA'].round(2)
        df_filtrado1D['PROCESSO1_RAM_GB'] = (df_filtrado1D['PORCENTAGEM_PROCESSO1_RAM'] / (1024 ** 3)).round(2)
        df_filtrado1D['PROCESSO2_RAM_GB'] = (df_filtrado1D['PORCENTAGEM_PROCESSO2_RAM'] / (1024 ** 3)).round(2)
        df_filtrado1D['PROCESSO3_RAM_GB'] = (df_filtrado1D['PORCENTAGEM_PROCESSO3_RAM'] / (1024 ** 3)).round(2)
        df_filtrado1D['PROCESSO1_RAM_PERC'] = (df_filtrado1D['PROCESSO1_RAM_GB'] * 100) / df_filtrado1D['RAM_TOTAL_GB']
        df_filtrado1D['PROCESSO2_RAM_PERC'] = (df_filtrado1D['PROCESSO2_RAM_GB'] * 100) / df_filtrado1D['RAM_TOTAL_GB']
        df_filtrado1D['PROCESSO3_RAM_PERC'] = (df_filtrado1D['PROCESSO3_RAM_GB'] * 100) / df_filtrado1D['RAM_TOTAL_GB']

        #TEMPOOOOO
        df_filtrado1D['BOOTTIME_DT'] = pd.to_datetime(df_filtrado1D['BOOTTIME'], unit='s')
        df_filtrado1D['UPTIME'] = df_filtrado1D['DATA_HORA'] - df_filtrado1D['BOOTTIME_DT']
        df_filtrado1D['HORA_TRATAMENTO'] = pd.Timestamp.now()


    # TRATANDO OS DADOS DA SEMANA ANTERIROR
    limite_tempo7 = pd.Timestamp.now() - timedelta(days=7)

    df_filtrado7D = dados_brutos[dados_brutos['DATA_HORA'] >= limite_tempo7].copy()

    if df_filtrado7D.empty:
        print("Nenhum dado encontrado nos ultimos 7 DIAS")
    else:
        print(f"Encontradas {len(df_filtrado)} linhas para processar.")

        df_filtrado7D['RAM_TOTAL_GB'] = (df_filtrado7D['RAM_TOTAL'] / (1024 ** 3)).round(2)
        df_filtrado7D['RAM_USADA_GB'] = (df_filtrado7D['RAM_USADA'] / (1024 ** 3)).round(2)
        df_filtrado7D['DISCO_TOTAL_GB'] = (df_filtrado7D['DISCO_TOTAL'] / (1024 ** 3)).round(2)
        df_filtrado7D['DISCO_USADO_GB'] = (df_filtrado7D['DISCO_USADO'] / (1024 ** 3)).round(2)
        df_filtrado7D['LATENCIA'] = df_filtrado7D['LATENCIA'].round(2)
        df_filtrado7D['PROCESSO1_RAM_GB'] = (df_filtrado7D['PORCENTAGEM_PROCESSO1_RAM'] / (1024 ** 3)).round(2)
        df_filtrado7D['PROCESSO2_RAM_GB'] = (df_filtrado7D['PORCENTAGEM_PROCESSO2_RAM'] / (1024 ** 3)).round(2)
        df_filtrado7D['PROCESSO3_RAM_GB'] = (df_filtrado7D['PORCENTAGEM_PROCESSO3_RAM'] / (1024 ** 3)).round(2)
        df_filtrado7D['PROCESSO1_RAM_PERC'] = (df_filtrado7D['PROCESSO1_RAM_GB'] * 100) / df_filtrado7D['RAM_TOTAL_GB']
        df_filtrado7D['PROCESSO2_RAM_PERC'] = (df_filtrado7D['PROCESSO2_RAM_GB'] * 100) / df_filtrado7D['RAM_TOTAL_GB']
        df_filtrado7D['PROCESSO3_RAM_PERC'] = (df_filtrado7D['PROCESSO3_RAM_GB'] * 100) / df_filtrado7D['RAM_TOTAL_GB']

        #TEMPOOOOO
        df_filtrado7D['BOOTTIME_DT'] = pd.to_datetime(df_filtrado7D['BOOTTIME'], unit='s')
        df_filtrado7D['UPTIME'] = df_filtrado7D['DATA_HORA'] - df_filtrado7D['BOOTTIME_DT']
        df_filtrado7D['HORA_TRATAMENTO'] = pd.Timestamp.now()

    




    # TRATANDO OS DADOS DA SEMANA ANTERIROR
    limite_tempo7 = pd.Timestamp.now() - timedelta(days=14)
    
    semana_anterior = limite_tempo7 + timedelta(days=7)
    df_filtrado7DA = dados_brutos[(dados_brutos['DATA_HORA'] >= limite_tempo7) & (dados_brutos['DATA_HORA'] <= semana_anterior)].copy()

    if df_filtrado7D.empty:
        print("Nenhum dado encontrado nos ultimos 7 DIAS")
    else:
        print(f"Encontradas {len(df_filtrado)} linhas para processar.")

        df_filtrado7DA['RAM_TOTAL_GB'] = (df_filtrado7DA['RAM_TOTAL'] / (1024 ** 3)).round(2)
        df_filtrado7DA['RAM_USADA_GB'] = (df_filtrado7DA['RAM_USADA'] / (1024 ** 3)).round(2)
        df_filtrado7DA['DISCO_TOTAL_GB'] = (df_filtrado7DA['DISCO_TOTAL'] / (1024 ** 3)).round(2)
        df_filtrado7DA['DISCO_USADO_GB'] = (df_filtrado7DA['DISCO_USADO'] / (1024 ** 3)).round(2)
        df_filtrado7DA['LATENCIA'] = df_filtrado7DA['LATENCIA'].round(2)
        df_filtrado7DA['PROCESSO1_RAM_GB'] = (df_filtrado7DA['PORCENTAGEM_PROCESSO1_RAM'] / (1024 ** 3)).round(2)
        df_filtrado7DA['PROCESSO2_RAM_GB'] = (df_filtrado7DA['PORCENTAGEM_PROCESSO2_RAM'] / (1024 ** 3)).round(2)
        df_filtrado7DA['PROCESSO3_RAM_GB'] = (df_filtrado7DA['PORCENTAGEM_PROCESSO3_RAM'] / (1024 ** 3)).round(2)
        df_filtrado7DA['PROCESSO1_RAM_PERC'] = (df_filtrado7DA['PROCESSO1_RAM_GB'] * 100) / df_filtrado7DA['RAM_TOTAL_GB']
        df_filtrado7DA['PROCESSO2_RAM_PERC'] = (df_filtrado7DA['PROCESSO2_RAM_GB'] * 100) / df_filtrado7DA['RAM_TOTAL_GB']
        df_filtrado7DA['PROCESSO3_RAM_PERC'] = (df_filtrado7DA['PROCESSO3_RAM_GB'] * 100) / df_filtrado7DA['RAM_TOTAL_GB']

        #TEMPOOOOO
        df_filtrado7DA['BOOTTIME_DT'] = pd.to_datetime(df_filtrado7DA['BOOTTIME'], unit='s')
        df_filtrado7DA['UPTIME'] = df_filtrado7DA['DATA_HORA'] - df_filtrado7DA['BOOTTIME_DT']
        df_filtrado7DA['HORA_TRATAMENTO'] = pd.Timestamp.now()



#####
#####
#####
#####
#####
# PARTE 4 INICIANDO A CAMADA GOLD PARA OS JSONS
#####
#####
#####
#####
#####

    print("\033[38;2;255;215;0m============================== CAMADA CLIENT ============================== \033[0m")

    #PARA CADA EMPRESA SERÀ GERADO TRÊS CSVS: GESTOR, ANALISTA, ESPECIFICA 



    # INICIALIZANDO OS SETS
    dados_client_analista = {}
    dados_client_gestor = {}
    dados_client_especifica = {}

    ########################################## JSON ANALISA ##########################################


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


    df_filtrado['ZONA'] = df_filtrado['ZONA'].astype(str).str.strip().str.upper()
    #LIMPEZA: Tranforma em STRING, Remove espaço em branco, joga tudo para upper


    # PRIMEIRA QUERY PARA VERIFICAR AS ZONAS DAQUELA EMPRESA
    query = """
        SELECT zona.*, empresa.razaoSocial FROM empresa 
        JOIN regiao ON idEmpresa = fkRegiaoEmpresa
        JOIN datacenter ON idDataCenter = fkRegiaoDataCenter 
        JOIN zona ON fkDataCenter = idDataCenter;
        """
    #Busca todas as zonas e suas empresas
    cursor.execute(query)
    zonas = cursor.fetchall()


    # PASSANDO POR AQUELA EMPRESA
    for empresa in empresas:
        dados_client_analista = {} 
        nome_empresa_atual = str(empresa[1])
        print(Fore.LIGHTWHITE_EX + f"Analisando dados para a empresa {nome_empresa_atual} " + Style.RESET_ALL)
        # PASSANDO PELA ZONA DAQUELA EMPRESA
        for zona in zonas:
            nomeZona = str(zona[1]).strip().upper() 
            idZona = zona[0]
            EmpresaZona = zona[4]
            if EmpresaZona != nome_empresa_atual: 
                continue

            agora = datetime.now()
            limite_tempo = agora - timedelta(minutes=5)
            df_ultimos5M = df_filtrado[df_filtrado['DATA_HORA'] >= limite_tempo]
            if df_ultimos5M.empty:
                continue

            df_ultimos5MZonaX = df_ultimos5M[df_ultimos5M['ZONA'] == nomeZona]

            #VErifica se foi encontrado alguma informação para essa zona
            if df_ultimos5MZonaX.empty:
                print(Fore.RED + f"Nenhum dado encontrado para a zona: {nomeZona} " + Style.RESET_ALL)
                dados_client_analista[nomeZona] = {
                'KPIS': 'Nenhum dado encontrado para essa zona!',
                'GRAFICOS': 'Nenhum dado encontrado para essa zona!'
                }
                continue
            print(Fore.LIGHTMAGENTA_EX+ f"Dados encontrados para a zona: {nomeZona}!" + Style.RESET_ALL)
            

            # *******************************
            # KPI

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


            # ************************
            # GRAFICOS

            # UsoMedioRecursos (Media da zona toda)
            uso_medio_cpu = df_ultimos5MZonaX['CPU'].mean() 
            uso_medio_ram = df_ultimos5MZonaX['RAM_PERCENT'].mean()
            uso_medio_disco = df_ultimos5MZonaX['DISCO_PERCENT'].mean() 

            # qtdTotalServidores  // CONTA OS SERVIDORES
            qtdTotalServidores = df_ultimos5MZonaX['SERVIDOR'].nunique()

            # servidoresCriticos
            df_media_servers = df_ultimos5MZonaX.groupby('SERVIDOR').mean(numeric_only=True)
            #EXPLICAÇÂO: Agrupa todos os servidores com o mesmo nome nessa nova df
            #Em seguida faz uma media das colunas que somente são numericas, ignorando todas as colunas de texto
            #OBS: O indece dessa lista passa a ser os valores de SERVIDOR

            criticos = df_media_servers[(df_media_servers['CPU'] > 90) | (df_media_servers['RAM_PERCENT'] > 85)]
            #Aqui é complicado, pegamos os servidores criticos com a CPU > 80 e a RAM PERCENT > 90
            if criticos.empty:
                listaServersCriticos = 'Nenhum servidor critico encontrado!'
            else:
                df_criticos_formatado = criticos[['CPU', 'RAM_PERCENT']].rename(
                    columns={'CPU': 'USO_CPU', 'RAM_PERCENT': 'USO_RAM'}
                ).round(2)
                listaServersCriticos = df_criticos_formatado.to_dict(orient='index')
            qtdServidoresCritivos = len(criticos)

            # TOP 3 PROCESSOS - RAM
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

            # TOP 3 PROCESSOS - CPU
            # Mesma lógica, mas empilhando as colunas de CPU
            p1_cpu = df_ultimos5MZonaX[['PROCESSO1_CPU', 'PORCENTAGEM_PROCESSO1_CPU']].rename(columns={'PROCESSO1_CPU': 'NOME', 'PORCENTAGEM_PROCESSO1_CPU': 'USO_PERC'})
            p2_cpu = df_ultimos5MZonaX[['PROCESSO2_CPU', 'PORCENTAGEM_PROCESSO2_CPU']].rename(columns={'PROCESSO2_CPU': 'NOME', 'PORCENTAGEM_PROCESSO2_CPU': 'USO_PERC'})
            p3_cpu = df_ultimos5MZonaX[['PROCESSO3_CPU', 'PORCENTAGEM_PROCESSO3_CPU']].rename(columns={'PROCESSO3_CPU': 'NOME', 'PORCENTAGEM_PROCESSO3_CPU': 'USO_PERC'})

            todos_processos_cpu = pd.concat([p1_cpu, p2_cpu, p3_cpu])
            top3_ProcessosUsoCPU = todos_processos_cpu.groupby('NOME')['USO_PERC'].mean().nlargest(3)

            # porcentagemSomaTop_ProcUsoCPU
            porcentagemSomaTop_ProcUsoCPU = top3_ProcessosUsoCPU.sum()

            # =================================================================
            # GRÁFICO DE LINHA: ESTRESSADOS VS SOBRECARREGADOS (ÚLTIMOS 7 DIAS)
            # VOU PEGAR DOS ULTIMSO 7 DIAS, ULTIMA SEMANA
            # SIM, LOUCURA

            limite_7_dias = agora - timedelta(days=7)
            df_7dias_zona = dados_brutos[(dados_brutos['DATA_HORA'] >= limite_7_dias)].copy()
            df_7dias_zona['ZONA'] = df_7dias_zona['ZONA'].astype(str).str.strip().str.upper()
            df_7dias_zona = df_7dias_zona[(df_7dias_zona['ZONA'] == nomeZona)].copy()
            #To filtrando a zona e pegando os dados dos ultimos 7 dias
           
            if not df_7dias_zona.empty:
               
                df_7dias_zona['DIA'] = df_7dias_zona['DATA_HORA'].dt.date.astype(str)
                
                df_media_diaria = df_7dias_zona.groupby(['DIA', 'SERVIDOR'])[['CPU', 'RAM_PERCENT']].mean(numeric_only=True).reset_index()
                df_media_diaria['Qtd_Sobrecarregados'] = ((df_media_diaria['CPU'] > 85) | (df_media_diaria['RAM_PERCENT'] > 85)).astype(int)

                df_media_diaria['Qtd_Estressados'] = (
                    ((df_media_diaria['CPU'] > 70) & (df_media_diaria['CPU'] <= 85)) | 
                    ((df_media_diaria['RAM_PERCENT'] > 70) & (df_media_diaria['RAM_PERCENT'] <= 85))
                ).astype(int)

                agrupado_por_dia = df_media_diaria.groupby('DIA')[['Qtd_Estressados', 'Qtd_Sobrecarregados']].sum()

                historico_7_dias_dict = agrupado_por_dia.to_dict(orient='index')
            else:
                historico_7_dias_dict = "Sem dados suficientes para os últimos 7 dias."

            
            
            
            
            
            
            # =================================================================
            
            # MONTANDO O DICIONÁRIO NO FINAL DO LOOP
            dados_client_analista[nomeZona] = {
                'KPIS': {
                    'Total de servidores (QTD)': int(total_servidores),
                    'Servidores Inativos (QTD)': int(servidores_inativos),
                    'Servidores Criticos (QTD)': int(qtdServidoresCritivos),
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
                    'Servidores estressados X Sobrecarregados': historico_7_dias_dict,


                    'Top 3 Processos RAM (GB)': top3_ProcessosUsoRam.to_dict(),
                    'Soma Top 3 RAM (GB)': round(somaTop3_ProcUsoRamGB, 2),
                    'Top 3 RAM (%)': round(porcentagemSomaTop_ProcUsoRam, 2),

                    'Top 3 Processos CPU (%)': top3_ProcessosUsoCPU.to_dict(),
                    'Soma Top 3 CPU (%)': round(porcentagemSomaTop_ProcUsoCPU, 2)
                }
            }


        # ENVIANDO O ARQUIVO PARA AWS
        if dados_client_analista:
            hora_envio = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S') 

            pasta_destino_json = 'dados_clientes_json'
            os.makedirs(pasta_destino_json, exist_ok=True)
            nome_arquivo_json = f"dados-client-{nome_empresa_atual}-analista.json"
            caminho_local_json = os.path.join(pasta_destino_json, nome_arquivo_json)

            with open(caminho_local_json, 'w', encoding='utf-8') as f:
                json.dump(dados_client_analista, f, indent=4, ensure_ascii=False, default=json_serial)

            print(Fore.GREEN + f"Arquivo {nome_arquivo_json} criado com sucesso!"+ Style.RESET_ALL)

            # Fazendo o upload apontando o arquivo correto  e colocando a extensão .json no final do destino
            caminho_s3 = f'client/dados-client-{nome_empresa_atual}-analista.json'
            upload_file(caminho_local_json, bucket_name, caminho_s3)
        else:
            print(f"Não foi encontrado nenhum dado para a empresa: {nome_empresa_atual}")

        ########################################## JSON ESPECIFICA ##########################################



        df_filtrado['SERVIDOR'] = df_filtrado['SERVIDOR'].astype(str).str.strip().str.upper()

    


    query_empresas = "SELECT * FROM empresa"
    cursor.execute(query_empresas)
    emrpesas_especifica = cursor.fetchall()




    for empresa in emrpesas_especifica:
        dados_client_especifica = {}
        
        id_empresa = empresa[0]
        nome_empresa = empresa[1]


        query_datacenters = f"SELECT * FROM datacenter JOIN regiao ON fkRegiaoDataCenter = idDatacenter JOIN empresa ON fkRegiaoEmpresa = idEmpresa WHERE fkRegiaoEmpresa = {id_empresa};"
        cursor.execute(query_datacenters)
        datacenters = cursor.fetchall()




        if len(datacenters) < 1:
            print(f"A empresa {nome_empresa} não tem datacenters cadastrados")
            
        else:
            for datacenter in datacenters:
        
                id_datacenter = datacenter[0]
                nome_datacenter = datacenter[1]

                dados_client_especifica[nome_datacenter] = {}

                query_zonas = f"SELECT * FROM zona WHERE fkDataCenter = {id_datacenter};"
                cursor.execute(query_zonas)
                zonas = cursor.fetchall()

                if len(zonas) < 1:
                    print(f"o data center {nome_datacenter} não possui zonas")

                else:
                    for zona in zonas:
                        id_zona = zona[0]
                        nome_zona = zona[1]

                        dados_client_especifica[nome_datacenter][nome_zona] = {}

                        query_servidores = f"SELECT * FROM servidor WHERE fkZona = {id_zona};"
                        cursor.execute(query_servidores)
                        servidores = cursor.fetchall()


                        if len(servidores) < 1:
                            print("szona sem servidores")

                        else:
                            for servidor in servidores:

                                id_servidor = servidor[0]
                                nome_servidor = servidor[1]      


                                df_servidor = df_filtrado[df_filtrado['SERVIDOR'] == nome_servidor.strip().upper()]
                            


                                if df_servidor.empty:
                #KPIS
                                    componente_mais_sobreccaregado = []
                                    processo_com_maior_consumo_geral = []
                                    processo_com_maior_consumo_geral_ram  = ""
                                    processo_com_maior_consumo_geral_cpu  = ""    
                                    uso_de_ram  = 0
                                    uso_de_cpu = 0 
                                    uso_disco = 0 
                                    componente_com_maior_uso_na_semana = []
                                    ping = 0


                                    #GRAFICOS
                                    grafico_uso_ram = 0
                                    grafico_uso_cpu = 0
                                    grafico_uso_disco = 0
                                    ranking_componenttes = 0
                                else:
                                    componente_mais_sobreccaregado = []

                                    print(df_servidor.columns.tolist())

                        
                                    processo_com_maior_consumo_geral_ram  = df_servidor['PROCESSO1_CPU'].iloc[0]
                                    processo_com_maior_consumo_geral_cpu  = df_servidor['PROCESSO1_RAM'].iloc[0]    
                                    uso_de_ram  = round(df_servidor['RAM_PERCENT'].mean(), 2)
                                    uso_de_cpu = round(df_servidor['CPU'].mean(), 2)
                                    uso_disco = round(df_servidor['DISCO_PERCENT'].mean(), 2)
                                    ping = round(df_servidor['LATENCIA'].mean(), 2)
                                    componente_com_maior_uso_na_semana = []


                                    if uso_de_ram > uso_de_cpu and uso_de_ram > uso_disco:
                                        componente_com_maior_uso_na_semana = "RAM"
                                        componente_mais_sobreccaregado ="RAM"
                                    elif uso_de_cpu > uso_de_ram and uso_de_cpu > uso_disco:
                                        componente_com_maior_uso_na_semana = "CPU"
                                        componente_mais_sobreccaregado = "CPU"
                                    elif uso_disco > uso_de_cpu and  uso_disco > uso_de_ram:
                                        componente_com_maior_uso_na_semana = "DISCO"
                                        componente_mais_sobreccaregado = "DISCO"
                                    else:
                                        componente_com_maior_uso_na_semana = "NDA"
                                        componente_mais_sobreccaregado = "NDA"



                                    ranking_componenttes = [["CPU", uso_de_cpu], ["RAM", uso_de_ram], ["DISCO", uso_disco]]

                                    nova_lista_ranking = []


                                    
                                    if componente_mais_sobreccaregado == "RAM":
                                        if uso_de_cpu > uso_disco:
                                            ranking_componenttes = [["RAM", uso_de_ram], ["CPU", uso_de_cpu]], ["DISCO", uso_disco]
                                        else:
                                            ranking_componenttes = [["RAM", uso_de_ram], ["DISCO", uso_disco]], ["CPU", uso_de_cpu]
                                    elif componente_mais_sobreccaregado == "CPU":
                                        if uso_de_ram > uso_disco:
                                            ranking_componenttes = [["CPU", uso_de_cpu], ["RAM", uso_de_ram]], ["DISCO", uso_disco]
                                        else:
                                            ranking_componenttes = [["CPU", uso_de_cpu], ["DISCO", uso_disco]], ["RAM", uso_de_ram]
                                    elif componente_mais_sobreccaregado == "DISCO":
                                        if uso_de_ram > uso_de_cpu:
                                            ranking_componenttes = [["DISCO", uso_disco], ["RAM", uso_de_ram]], ["CPU", uso_de_cpu]
                                        else:
                                            ranking_componenttes = [["CPU", uso_de_cpu], ["CPU", uso_de_cpu]], ["RAM", uso_de_ram]                                              






                                    

                                    

                                    grafico_uso_ram = round(df_servidor['RAM_PERCENT'].mean(), 2)
                                    grafico_uso_cpu = round(df_servidor['CPU'].mean(), 2)
                                    grafico_uso_disco = round(df_servidor['DISCO_PERCENT'].mean(), 2)

                                    






                                dados_client_especifica[nome_datacenter][nome_zona][nome_servidor] = {
                                    'KPIS': {
                                        'COMPONENTE_SOBRECARREGADO': componente_mais_sobreccaregado,
                                        'PROCESSO_COM_MAIOR_CONSUMO_RAM': processo_com_maior_consumo_geral_cpu,
                                        'PROCESSO_COM_MAOOR_CONSUMO_CPU': processo_com_maior_consumo_geral_ram,
                                        'USO_CPU': uso_de_cpu,
                                        'USO_RAM': uso_de_ram,
                                        'USO_DISCO': uso_disco,
                                        'COMPONENTE_MAIOR_USO': componente_com_maior_uso_na_semana,
                                        'LATENCIA': ping
                                    },

                                    'GRAFICOS': {
                                        'USO_CPU': grafico_uso_cpu,
                                        'USO_RAM': grafico_uso_ram,
                                        'USO_DISCO': grafico_uso_disco,
                                        'LATENCIA': ping,
                                        'RANKING_COMPONENTES': ranking_componenttes
                                     },
                                }
                        
                        

                

        

        


        if dados_client_especifica:
            hora_envio = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S') 

            pasta_destino_json = 'dados_clientes_json'
            os.makedirs(pasta_destino_json, exist_ok=True)
            nome_arquivo_json = f"dados-client-{nome_empresa}-especifica.json"
            caminho_local_json = os.path.join(pasta_destino_json, nome_arquivo_json)

            with open(caminho_local_json, 'w', encoding='utf-8') as f:
                json.dump(dados_client_especifica, f, indent=4, ensure_ascii=False, default=json_serial)

            print(Fore.GREEN + f"Arquivo {nome_arquivo_json} criado com sucesso!"+ Style.RESET_ALL)

            # Fazendo o upload apontando o arquivo correto  e colocando a extensão .json no final do destino
            caminho_s3 = f'client/dados-client-{nome_empresa_atual}-especifica.json'
            upload_file(caminho_local_json, bucket_name, caminho_s3)
        else:
            print(f"Não foi encontrado nenhum dado para a empresa: {nome_empresa_atual}")
            
        

        ########################################## JSON GESTOR ##########################################

    query_empresas = "SELECT * FROM empresa"
    cursor.execute(query_empresas)
    emrpesas_gestor = cursor.fetchall()

    for empresa_gestor in emrpesas_gestor:

        dados_client_gestor = {}
        
        id_empresa = empresa_gestor[0]
        nome_empresa = empresa_gestor[1]


        query_datacenters = f"SELECT * FROM datacenter JOIN regiao ON fkRegiaoDataCenter = idDatacenter JOIN empresa ON fkRegiaoEmpresa = idEmpresa WHERE fkRegiaoEmpresa = {id_empresa};"
        cursor.execute(query_datacenters)
        datacenters_gestor  = cursor.fetchall()


        for datacenter in datacenters_gestor:
    


            nome_datacenter = datacenter[1]

            id_datacenter = datacenter[0]

            df_datacenter = df_filtrado[df_filtrado['DATACENTER'] == nome_datacenter.strip().upper()]
            df_datacenter1D = df_filtrado1D[df_filtrado1D['DATACENTER'] == nome_datacenter.strip().upper()]
            df_datacenter7D = df_filtrado7D[df_filtrado7D['DATACENTER'] == nome_datacenter.strip().upper()]
            df_datacenter7DA = df_filtrado7DA[df_filtrado7DA['DATACENTER'] == nome_datacenter.strip().upper()]
            query_servidores_totais = f"SELECT COUNT(idServidor) FROM servidor JOIN  zona ON fkZona = idZona JOIN datacenter ON fkDataCenter = idDataCenter WHERE  fkDataCenter = {id_datacenter};"
            cursor.execute(query_servidores_totais)
            servidores_totais = cursor.fetchall()


            query_servidores_ativos = f"""SELECT COUNT(idServidor) FROM servidor JOIN  zona ON fkZona = idZona JOIN datacenter ON fkDataCenter = idDataCenter WHERE estado = "Ativo" and fkDataCenter = {id_datacenter};"""
            cursor.execute(query_servidores_ativos)
            servidores_ativos = cursor.fetchall()


    


            if df_datacenter.empty:

                processo_maior_consumo = "nao possui"
                processo_componente = "nao tem"

                cpu_p99 = 0
                ram_p99 = 0
                disco_p99 = 0
                latencia_p99 = 0
                cpu_dia_anterior = 0
                ram_dia_anteiror = 0
                disco_dia_anterior = 0
                latencia_dia_anterior = 0



            else:

            

                processo_maior_consumo = [df_datacenter['PROCESSO1_CPU'].iloc[-1], df_datacenter['PROCESSO1_RAM'].iloc[-1]]
            


                

                # para o momento atual
                cpu_p99 = round(df_datacenter['CPU'].quantile(0.99),2 )
                ram_p99 = round(df_datacenter['RAM_PERCENT'].quantile(0.99),  2)
                disco_p99 = round(df_datacenter['DISCO_PERCENT'].quantile(0.99), 2)
                latencia_p99 = round(df_datacenter['LATENCIA'].quantile(0.99), 2)
                cpu_dia_anterior = round(df_datacenter1D['CPU'].quantile(0.99),2 )
                ram_dia_anteiror = round(df_datacenter1D['RAM_PERCENT'].quantile(0.99),  2)
                disco_dia_anterior = round(df_datacenter1D['DISCO_PERCENT'].quantile(0.99), 2)
                latencia_dia_anterior = round(df_datacenter1D['LATENCIA'].quantile(0.99), 2)
                

        
            if df_datacenter7D.empty:


                # para os ultimos 7 dias


                cpu_p99_7D = 0
                ram_p99_7D = 0
                tendencia_ram_7D = 0
                tendencia_cpu_7D = 0
                disco_p99_7D = 0
                latencia_p99_7D = 0
                cpu_semana_anterior = 0
                ram_semana_anterior = 0
                disco_semana_anterior = 0
                latencia_semana_anterior = 0                

                
            else:

                

                cpu_p99_7D = round(df_datacenter7D['CPU'].quantile(0.99),2 )
                ram_p99_7D = round(df_datacenter7D['RAM_PERCENT'].quantile(0.99),  2)
                disco_p99_7D = round(df_datacenter7D['DISCO_PERCENT'].quantile(0.99), 2)
                latencia_p99_7D =  round(df_datacenter7D['LATENCIA'].quantile(0.99), 2)
                cpu_semana_anterior = round(df_datacenter7DA['CPU'].quantile(0.99),2 )
                ram_semana_anterior = round(df_datacenter7DA['RAM_PERCENT'].quantile(0.99),  2)
                disco_semana_anterior = round(df_datacenter7DA['DISCO_PERCENT'].quantile(0.99), 2)
                latencia_semana_anterior = round(df_datacenter7DA['LATENCIA'].quantile(0.99), 2)







            dados_client_gestor[nome_datacenter] = {
                "KPI": {
                    "SERVIDORES_ATIVOS": servidores_ativos[0][0],
                    "SERVIDORES_TOTAIS": servidores_totais[0][0], 
                    "PROCESSO_MAIOR_CONSUMO": processo_maior_consumo,

                    
                },
                "GRAFICOS": {
                    
                    "DADOS_ATUAIS": {
                    "CPU_P99": cpu_p99,
                    "RAM_P99": ram_p99,
                    "DISCO_P99": disco_p99,
                    "LATENCIA_P99": latencia_p99,
                    "CPU_DIA_ANTERIOR": cpu_dia_anterior,
                    "RAM_DIA_ANTERIOR": ram_dia_anteiror,
                    "DISCO_DIA_ANTERIOR": disco_dia_anterior,
                    "LATENCIA_DIA_ANTERIOR": latencia_dia_anterior,
                    },


                    "DADOS_7_DIAS": {
                    "CPU_P99": cpu_p99_7D,
                    "RAM_P99": ram_p99_7D,
                    "DISCO_P99": disco_p99_7D,
                    "LATENCIA_P99": latencia_p99_7D,
                    "CPU_SEMANA_ANTERIOR": cpu_semana_anterior,
                    "RAM_SEMANA_ANTERIOR": ram_semana_anterior,
                    "DISCO_SEMANA_ANTERIOR":disco_semana_anterior ,
                    "LATENCIA_SEMANA_ANTERIOR": latencia_semana_anterior,
                    },
                }

        }

    
    if dados_client_especifica:
            hora_envio = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S') 

            pasta_destino_json = 'dados_clientes_json'
            os.makedirs(pasta_destino_json, exist_ok=True)
            nome_arquivo_json = f"dados-client-{nome_empresa}-gestor.json"
            caminho_local_json = os.path.join(pasta_destino_json, nome_arquivo_json)

            with open(caminho_local_json, 'w', encoding='utf-8') as f:
                json.dump(dados_client_gestor, f, indent=4, ensure_ascii=False, default=json_serial)

            print(Fore.GREEN + f"Arquivo {nome_arquivo_json} criado com sucesso!"+ Style.RESET_ALL)

            # Fazendo o upload apontando o arquivo correto  e colocando a extensão .json no final do destino
            caminho_s3 = f'client/dados-client-{nome_empresa_atual}-gestor.json'
            upload_file(caminho_local_json, bucket_name, caminho_s3)
    else:
            print(f"Não foi encontrado nenhum dado para a empresa: {nome_empresa_atual}")





    print(Fore.WHITE + "Processamento concluído. Aguardando 5 minutos para o próximo ciclo..." + Style.RESET_ALL)
    time.sleep(20)



   