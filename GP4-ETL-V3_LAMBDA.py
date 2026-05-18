import csv
import json
import boto3
from urllib.parse import unquote_plus
from datetime import datetime, timedelta
import pandas as pd
import io

s3 = boto3.client('s3')

# Função inicial que chama as demais
def lambda_handler(event, context):
    print("Lambda Iniciada! 📍" )
    print(f"Evento recebido: {event}")

    #Verifica se é o JSON de metricas
    registro = event["Records"][0]["s3"]
    key = unquote_plus(registro["object"]["key"])
    if key.lower().endswith(".json"):
        return f"Arquivo JSON detectado ({key}). Processamento ignorado."
        
    try:
        print("Iniciando Trusted")
        resultado_trusted = TrustedCsv(event, context)

        if isinstance(resultado_trusted, dict) and "chave" in resultado_trusted:
            print(f"Sucesso Trusted: {resultado_trusted['mensagem']}")
            
            resultado_client = ClientGeral(resultado_trusted['bucket'], resultado_trusted['chave'])
            print(f"Status ClientGeral: {resultado_client}")
            
            return {
                "statusCode": 200,
                "body": "Pipeline completo executado com sucesso."
            }
        else:
            print(f"Aviso Trusted: {resultado_trusted}")
            return {
                "statusCode": 200,
                "body": resultado_trusted
            }
    except Exception as e:
        print(f"❌ Erro fatal: {e}")
        return {
            "statusCode": 500,
            "body": str(e)
        }


# Função que faz o tratamento dos dados
def TrustedCsv(event, context):
    #Pega o arquivo que chegou no Lambda
    registro = event["Records"][0]["s3"]
    bucket = registro["bucket"]["name"]
    key = unquote_plus(registro["object"]["key"])

    #Valida se o evento que chamou o lambda seja outra coisa
    if key.endswith("/") or registro["object"]["size"] == 0:
        return f"Ignorado: {key} é um diretório ou arquivo vazio."

    #Pega informações do arquivo e pasta de origem e finalidade
    nome_arquivo = key.split("/")[-1]

    caminho_local_entrada = f"/tmp/{nome_arquivo}"
    caminho_local_mestre = "/tmp/dados_mestre.csv"
    chave_destino_mestre = "trusted/dados_tratados.csv"

    #Baixa o arquivo em uma pasta temporaria
    print(f"Baixando o arquivo entrada: {key}")
    s3.download_file(bucket, key, caminho_local_entrada)

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
        'BOOTTIME_DT': 'BOOTTIME', 'DATA_HORA': 'DATE', 'UPTIME': 'UPTIME', 'HORA_TRATAMENTO': 'HORA_TRATAMENTO', 'DIA_SEMANA': 'DIA_SEMANA'
    }

    #Defini um limite de tempo de 7 dias para tratar os dados
    limite_tempo7 = datetime.now() - timedelta(days=7)

    #Ler o CSV
    df = pd.read_csv(caminho_local_entrada, delimiter=";", encoding="utf-8-sig")
    df.columns = df.columns.str.strip()

    df["DATA_HORA"] = pd.to_datetime(df["DATA_HORA"])

    # Pula a linha se for mais velha que 7 dias
    df = df[df["DATA_HORA"] >= limite_tempo7]

    if df.empty:
        return f"Arquivo {nome_arquivo} lido, mas nenhuma linha se qualificou (dentro de 7 dias)."

    df["RAM_TOTAL_GB"]  = (df["RAM_TOTAL"].astype(float)  / (1024 ** 3)).round(2)
    df["RAM_USADA_GB"]  = (df["RAM_USADA"].astype(float)  / (1024 ** 3)).round(2)
    df["DISCO_TOTAL_GB"]= (df["DISCO_TOTAL"].astype(float) / (1024 ** 3)).round(2)
    df["DISCO_USADO_GB"]= (df["DISCO_USADO"].astype(float) / (1024 ** 3)).round(2)
    df["LATENCIA"]      = df["LATENCIA"].astype(float).round(2)

    df["PROCESSO1_RAM_GB"] = (df["PORCENTAGEM_PROCESSO1_RAM"].astype(float) / (1024 ** 3)).round(2)
    df["PROCESSO2_RAM_GB"] = (df["PORCENTAGEM_PROCESSO2_RAM"].astype(float) / (1024 ** 3)).round(2)
    df["PROCESSO3_RAM_GB"] = (df["PORCENTAGEM_PROCESSO3_RAM"].astype(float) / (1024 ** 3)).round(2)

    df["PROCESSO1_RAM_PERC"] = ((df["PROCESSO1_RAM_GB"] * 100) / df["RAM_TOTAL_GB"]).where(df["RAM_TOTAL_GB"] > 0, 0.0).round(2)
    df["PROCESSO2_RAM_PERC"] = ((df["PROCESSO2_RAM_GB"] * 100) / df["RAM_TOTAL_GB"]).where(df["RAM_TOTAL_GB"] > 0, 0.0).round(2)
    df["PROCESSO3_RAM_PERC"] = ((df["PROCESSO3_RAM_GB"] * 100) / df["RAM_TOTAL_GB"]).where(df["RAM_TOTAL_GB"] > 0, 0.0).round(2)

    df["BOOTTIME_DT"]     = df["BOOTTIME"].astype(float).apply(datetime.fromtimestamp)
    df["UPTIME"]          = (df["DATA_HORA"] - df["BOOTTIME_DT"]).astype(str)
    df["HORA_TRATAMENTO"] = str(datetime.now())

    df = df[list(colunas_finais.keys())].rename(columns=colunas_finais)

    df_mestre = pd.DataFrame()
    try:
        print(f"Tentando ler arquivo mestre existente: {chave_destino_mestre}")
        resposta_mestre = s3.get_object(Bucket=bucket, Key=chave_destino_mestre)
        conteudo_mestre = resposta_mestre['Body'].read().decode('utf-8')
        df_mestre = pd.read_csv(io.StringIO(conteudo_mestre), delimiter=";")
        print(f"Arquivo mestre carregado com {len(df_mestre)} linhas.")
    except Exception as e:
        print("Arquivo mestre nao encontrado. Criando um novo do zero.")

    df_unificado = pd.concat([df_mestre, df], ignore_index=True)

    df_unificado.to_csv(caminho_local_mestre, sep=";", index=False, encoding="utf-8")

    print(f"Fazendo upload do CSV unificado para: {chave_destino_mestre}")
    s3.upload_file(caminho_local_mestre, bucket, chave_destino_mestre)

    return {
        "mensagem": f"Arquivo unificado. Adicionadas {len(df)} novas linhas. Total agora: {len(df_unificado)}",
        "bucket": bucket,
        "chave": chave_destino_mestre
    }


#Função de envio dos JSON para o Client
def ClientGeral(bucket, chave):
    print(f"Lendo arquivo Trusted no S3: {chave}")
    
    resposta = s3.get_object(Bucket=bucket, Key=chave)
    conteudo_texto = resposta['Body'].read().decode('utf-8')
    df = pd.read_csv(io.StringIO(conteudo_texto), delimiter=";")
    dados_dicionario = df.to_dict(orient="records")

    respFinanceiro = dashFinanceiro(dados_dicionario)
    respGestora = dashGestora(dados_dicionario)
    respAnalista = dashAnalista(dados_dicionario)
    respAlertas = dashAlertas(dados_dicionario)
    respServidores = dashServidores(dados_dicionario)

    s3.put_object(
        Bucket=bucket,
        Key="client/financeiro_master.json",
        Body=json.dumps(respFinanceiro, default=str, indent=4)
    )

    s3.put_object(
        Bucket=bucket,
        Key="client/gestora_master.json",
        Body=json.dumps(respGestora, default=str, indent=4)
    )

    s3.put_object(
        Bucket=bucket,
        Key="client/analista_master.json",
        Body=json.dumps(respAnalista, default=str, indent=4)
    )

    s3.put_object(
        Bucket=bucket,
        Key="client/alertas_master.json",
        Body=json.dumps(respAlertas, default=str, indent=4)
    )

    s3.put_object(
        Bucket=bucket,
        Key="client/servidores_master.json",
        Body=json.dumps(respServidores, default=str, indent=4)
    )

    print("Todas as paginas processadas e atualizadas.")
    return "Lambda concluida com sucesso! ✅"


# ZONA DE TRABALHO

def dashGestora(dados):
    return {"tipo": "gestora", "total_dados": len(dados)}

def dashAnalista(dados):
    return {"tipo": "analista", "total_dados": len(dados)}

def dashAlertas(dados):
    return {"tipo": "analista", "total_dados": len(dados)}

def dashServidores(dados):
    return {"tipo": "analista", "total_dados": len(dados)}



############################################################## 💵 FINANCEIRO 💵 ###############################################################

# *********************************************
#  CONSTANTES — RECEITA
# Receita da Steam por minuto em real: R$ 105.349,98
# Receita a cada 5 minutos: R$ 526.749,90
RECEITA_BASE_5MIN = 526_749.9

# *********************************************
#  CONSTANTES — CUSTO FIXO
 
CUSTO_LICENCA_MES  =  800.0   # R$ — SO (Ubuntu Pro) + monitoramento
CUSTO_HARDWARE_MES = 1200.0  # R$ — amortização do hardware (~R$60k ÷ 60 meses)
CUSTO_OPERACAO_MES =  500.0   # R$ — salário dos funcionários (rateio por servidor)

INTERVALOS_MES = 30 * 24 * 12 

# Custo fixo por servidor a cada 5 minutos
CUSTO_FIXO_5MIN = (CUSTO_LICENCA_MES + CUSTO_HARDWARE_MES + CUSTO_OPERACAO_MES) / INTERVALOS_MES

# *********************************************
#  CONSTANTES — CUSTO VARIÁVEL (ENERGIA)

POTENCIA_MIN_W = 150   # Watts — servidor ligado sem nenhuma carga
POTENCIA_MAX_W = 500   # Watts — servidor a 100% de CPU + RAM + Disco
TARIFA_KWH = 0.75      # R$/kWh — tarifa comercial media brasileira
HORAS_5MIN = 5 / 60    # Duração do intervalo convertida em horas (= 0,0833h)
 
# Pesos de cada recurso no consumo de energia
PESO_CPU_ENERGIA   = 0.65
PESO_RAM_ENERGIA   = 0.25
PESO_DISCO_ENERGIA = 0.10

# *********************************************
#  FUNÇÃO: EstimarReceita

def EstimarReceita(df):
    receita = RECEITA_BASE_5MIN
 
    latencia = float(df.get('LATENCIA',  0))
    cpu      = float(df.get('CPU_PER',   0))
    ram      = float(df.get('RAM_PER',   0))
    disco    = float(df.get('DISCO_PER', 0))
 
    # — Penalidades de latência —
    if latencia > 500:
        receita *= (1 - 0.25)    # −25%: falha de serviço
    elif latencia > 200:
        receita *= (1 - 0.12)    # −12%: abandono de sessão
    elif latencia > 100:
        receita *= (1 - 0.05)    # −5%: lentidão perceptível
 
    # — Penalidades de CPU —
    if cpu > 90:
        receita *= (1 - 0.12)    # −12%: risco de queda
    elif cpu > 85:
        receita *= (1 - 0.05)    # −5%: sobrecarga
    elif cpu < 15:
        receita *= (1 - 0.02)    # −2%: servidor ocioso (capacidade desperdiçada)
 
    # — Penalidades de RAM —
    if ram > 95:
        receita *= (1 - 0.15)    # −15%: crítico — possível falta de memória (OOM)
    elif ram > 85:
        receita *= (1 - 0.07)    # −7%: pressão de memória
 
    # — Penalidades de Disco —
    if disco > 95:
        receita *= (1 - 0.10)    # −10%: disco cheio
    elif disco > 85:
        receita *= (1 - 0.04)    # −4%: I/O degradado
 
    return round(receita, 2)

def dashFinanceiro(dados):
    if not dados:
        print("ERRO: Sem dados para processar")
        return
    
    df = pd.DataFrame(dados)

    # Converte as colunas numéricas — o CSV pode usar vírgula como decimal
    colunas_numericas = ['CPU_PER', 'RAM_PER', 'DISCO_PER', 'LATENCIA', 'PACOTES_ENV', 'PACOTES_RCB', 'PACOTES_PER']
    
    # troca vírgula por ponto
    for coluna in colunas_numericas:
        df[coluna] = (
            df[coluna]
            .astype(str)
            .str.replace(',', '.', regex=False)  
            .astype(float)
        )
    
    df['DATE'] = pd.to_datetime(df['DATE'])
    df['MES']  = df['DATE'].dt.to_period('M').astype(str)

    # ── RECEITA ───────────────────────────────────────────────────
    #
    # A receita é global — não pertence a um servidor específico
    # Por isso agrupa todos os servidores do mesmo instante (DATE)
    # e calcula uma média de cada métrica para representar a infra toda
    #
    # Exemplo:
    #   10:00  SRV-01  CPU 80%
    #   10:00  SRV-02  CPU 40%
    #   10:00  SRV-03  CPU 60%
    #   ─────────────────────
    #   MÉDIA: 10:00   CPU 60%  → entra na função EstimarReceita

    #Agrupa
    medias_por_tempo = (
        df.groupby('DATE')[['LATENCIA', 'CPU_PER', 'RAM_PER', 'DISCO_PER']]
        .mean()
        .reset_index()
    )
 
    # Aplica EstimarReceita em cada linha de médias
    # (cada linha representa um instante de 5 minutos)
    medias_por_tempo['RECEITA_5MIN'] = medias_por_tempo.apply(
        lambda row: EstimarReceita(row.to_dict()), axis=1
    )
 
    # Conta quantos servidores estavam ativos em cada instante
    # para distribuir a receita proporcionalmente entre eles
    contagem_por_tempo = df.groupby('DATE')['SERVIDOR'].count().reset_index()
    contagem_por_tempo.columns = ['DATE', 'QTD_SERVIDORES']
 
    # Junta a contagem com as médias e divide a receita por servidor
    # Resultado:
    #   DATE   | RECEITA_5MIN | QTD_SERVIDORES | RECEITA_POR_SERVIDOR
    #   10:00  | R$ 98.958    |       3        |      R$ 32.986
    medias_por_tempo = medias_por_tempo.merge(contagem_por_tempo, on='DATE')
    medias_por_tempo['RECEITA_POR_SERVIDOR'] = (
        medias_por_tempo['RECEITA_5MIN'] / medias_por_tempo['QTD_SERVIDORES']
    )
    
    # RECEITA FEITAAAAAAAAAA VAMO BORAAAAAAA
    
    # ── CUSTO ─────────────────────────────────────────────────────
    #
    # O custo é calculado por servidor individualmente
    # Cada servidor tem seu próprio CPU/RAM/Disco, logo cada um
    # recebe um custo diferente no mesmo instante
 
    cpu = df['CPU_PER'] / 100
    ram = df['RAM_PER'] / 100
    disco = df['DISCO_PER'] / 100
 
    # Média ponderada da carga — representa o quanto o servidor está sendo exigido
    fator_carga = (
        (cpu   * PESO_CPU_ENERGIA)   +
        (ram   * PESO_RAM_ENERGIA)   +
        (disco * PESO_DISCO_ENERGIA)
    )
 
    potencia_w = POTENCIA_MIN_W + (POTENCIA_MAX_W - POTENCIA_MIN_W) * fator_carga
    # Potência estimada de cada servidor nesse instante
    # 150W |----[?W]----------| 500W
    # ocioso                    máximo

    energia_kwh = (potencia_w / 1000) * HORAS_5MIN
    # Energia consumida no intervalo (em kWh)
    # Fórmula: Energia (kWh) = Potência (kW) × Tempo (h)
 
    # Aplica a tarifa e soma o custo fixo
    custo_energia = energia_kwh * TARIFA_KWH
    df['CUSTO_5MIN'] = (custo_energia + CUSTO_FIXO_5MIN).round(4)
 
    # Junta a receita calculada de volta no DataFrame principal
    df = df.merge(
        medias_por_tempo[['DATE', 'RECEITA_5MIN', 'RECEITA_POR_SERVIDOR']],
        on='DATE', how='left'
    )
    
    # CUSTO FEITOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO 
 
    print(df)
    print("Colunas encontradas:", df.columns.tolist())
    print("Primeira linha:", df.iloc[0].to_dict())
    return {"tipo": "financeiro", "total_dados": len(dados)}







#################################################################################################################################################