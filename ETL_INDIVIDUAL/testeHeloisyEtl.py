import csv
import json
import boto3
from urllib.parse import unquote_plus
from datetime import datetime, timedelta
import pandas as pd
import requests
import io
import pymysql #biblioteca boa no lambda para acessar o banco!
import os

def conectarBanco():
    return pymysql.connect(
        host=os.environ["DB_HOST"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"],
        cursorclass=pymysql.cursors.DictCursor
    )
s3 = boto3.client('s3')

# Função inicial que chama as demais
def lambda_handler(event, context):
    print("Lambda Iniciada!" )
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
        print(f"Erro fatal: {e}")
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
    

    # Le os arquivos do JSON feito pelo SAMU

    geral = {}
    try:
        resp_geral = s3.get_object(Bucket=bucket, Key="raw/geral.json")
        geral = json.loads(resp_geral['Body'].read().decode('utf-8'))
        print(f"geral.json carregado com sucesso.")
    except Exception as e:
        print(f"geral.json não encontrado — . Erro: {e}")
 

    
    df = pd.read_csv(io.StringIO(conteudo_texto), delimiter=";")
    dados_dicionario = df.to_dict(orient="records")

  
    respGestoraOp = dashOperacional(dados_dicionario, geral, bucket)
   
 


    s3.put_object(
        Bucket=bucket,
        Key="client/gestoraOp_master.json",
        Body=json.dumps(respGestoraOp, default=str, indent=4)
    )

    
    print("Todas as paginas processadas e atualizadas.")
    return "Lambda concluida com sucesso! ✅"
    # DASH DE ALERTAS - Victor G

    

    

# ZONA DE TRABALHO

def dashGestora(dados):
    
    return {"tipo": "gestora", "total_dados": len(dados)}

def dashAnalista(dados):
    return {"tipo": "analista", "total_dados": len(dados)}

def dashServidores(dados):
    return {"tipo": "analista", "total_dados": len(dados)}



###########################################################DASHBOARD OPERACIONAL GESTOR####################################################################


#SCORE SAUDE SERVIDOR
LIMITE_CPU = 80
LIMITE_RAM = 85
LIMITE_DISCO = 70    
LIMITE_LATENCIA = 50


def converter_float(valor, padrao=0.0):
    try:
        return float(str(valor).replace(",", "."))
    except (ValueError, TypeError):
        return padrao


def calcularPenalidadePadrao(persistencia):
    if persistencia < 0.20:
        return 0
    elif persistencia < 0.40:
        return 5
    elif persistencia < 0.60:
        return 10
    elif persistencia < 0.80:
        return 15
    return 20


def calcularPenalidadeRam(persistencia):
    if persistencia < 0.20:
        return 0
    elif persistencia < 0.40:
        return 8
    elif persistencia < 0.60:
        return 15
    elif persistencia < 0.80:
        return 20
    return 25


def calcularPenalidadeDisco(persistencia):
    if persistencia < 0.20:
        return 0
    elif persistencia < 0.40:
        return 7
    elif persistencia < 0.60:
        return 12
    elif persistencia < 0.80:
        return 16
    return 20


def calcularPenalidadeLatencia(persistencia):
    if persistencia < 0.20:
        return 0
    elif persistencia < 0.40:
        return 3
    elif persistencia < 0.60:
        return 6
    elif persistencia < 0.80:
        return 8
    return 10


def calcularPenalidadeTendencia(queda):
    if queda < 5:
        return 0
    elif queda < 10:
        return 3
    elif queda < 20:
        return 6
    return 10


def classificarStatusScore(score):
    if score >= 80:
        return "Saudável"
    elif score >= 60:
        return "Atenção"
    return "Crítico"


def calcularScoreParcial(janela,limites):
    qntCpuCritica = 0
    qntRamCritica = 0
    qntDiscoCritico = 0
    qntLatCritica = 0
    qntColetasProblematicas = 0

    total_coletas = len(janela)

    if total_coletas == 0:
        return 100

    limiteCpu = converter_float(limites.get("CPU"), LIMITE_CPU)
    limiteRam = converter_float(limites.get("RAM"), LIMITE_RAM)
    limiteDisco = converter_float(limites.get("DISCO"), LIMITE_DISCO)
    limiteLatencia = converter_float(limites.get("REDE"), LIMITE_LATENCIA)

    for coleta in janela:
        cpu = converter_float(coleta.get("CPU_PER"))
        ram = converter_float(coleta.get("RAM_PER"))
        disco = converter_float(coleta.get("DISCO_PER"))
        latencia = converter_float(coleta.get("LATENCIA"))

        coletaProblematica = False

        if cpu > limiteCpu:
            qntCpuCritica += 1
            coletaProblematica = True

        if ram > limiteRam:
            qntRamCritica += 1
            coletaProblematica = True

        if disco > limiteDisco:
            qntDiscoCritico += 1
            coletaProblematica = True

        if latencia > limiteLatencia:
            qntLatCritica += 1
            coletaProblematica = True

        if coletaProblematica:
            qntColetasProblematicas += 1

    persistenciaGeral = qntColetasProblematicas / total_coletas
    persistenciaCpu = qntCpuCritica / total_coletas
    persistenciaRam = qntRamCritica / total_coletas
    persistenciaDisco = qntDiscoCritico / total_coletas
    persistenciaLatencia = qntLatCritica / total_coletas

    penalidadeGeral = calcularPenalidadePadrao(persistenciaGeral)
    penalidadeCpu = calcularPenalidadePadrao(persistenciaCpu)
    penalidadeRam = calcularPenalidadeRam(persistenciaRam)
    penalidadeDisco = calcularPenalidadeDisco(persistenciaDisco)
    penalidadeLatencia = calcularPenalidadeLatencia(persistenciaLatencia)

    penalidadeComp = (
        penalidadeCpu
        + penalidadeRam
        + penalidadeDisco
        + penalidadeLatencia
    )

    score_parcial = 100 - penalidadeComp - penalidadeGeral

    return max(0, min(100, score_parcial))


def calcularScoreServidor(coletaServidor, limites):
    if not coletaServidor:
        return {
            "score": 100,
            "status": "Saudável",
            "scoreParcialAtual": 100,
            "scoreParcialAnterior": 100,
            "queda": 0,
            "penalidadeTendencia": 0,
            "projecaoSaude": {
                "scoreAtual": 100,
                "scoreProjetado": 100,
                "risco": "Saudável",
                "motivo": "Sem dados suficientes para projeção",
                "componentesTendencia": [],
                "penalidadeProjecao": 0
            }
        }

    coletas_ordenadas = sorted(
        coletaServidor,
        key=lambda coleta: str(coleta.get("DATE", ""))
    )

    ultimas60Linhas = coletas_ordenadas[-60:]

    janelaAnterior = ultimas60Linhas[-60:-30]
    janelaAtual = ultimas60Linhas[-30:]

    scoreParcialAnterior = calcularScoreParcial(janelaAnterior, limites)
    scoreParcialAtual = calcularScoreParcial(janelaAtual, limites)

    queda = scoreParcialAnterior - scoreParcialAtual

    penalidadeTendencia = calcularPenalidadeTendencia(queda)

    scoreFinal = scoreParcialAtual - penalidadeTendencia
    scoreFinal = max(0, min(100, scoreFinal))

    componentesTendencia = calcularTendenciaComponentes(
    janelaAnterior,
    janelaAtual,
    limites)

    penalidadeProjecao = calcularPenalidadeProjecao(componentesTendencia)

    scoreProjetado = scoreFinal - penalidadeProjecao
    scoreProjetado = max(0, min(100, scoreProjetado))
    
    motivoProjecao = gerarMotivoProjecao(componentesTendencia)

    print("\nPROJEÇÃO")
    print("Score Atual:", scoreFinal)
    print("Score Projetado:", scoreProjetado)
    print("Motivo:", motivoProjecao)
    print("Componentes:", componentesTendencia)
    return {
        "score": round(scoreFinal, 2),
        "status": classificarStatusScore(scoreFinal),
        "scoreParcialAtual": round(scoreParcialAtual, 2),
        "scoreParcialAnterior": round(scoreParcialAnterior, 2),
        "queda": round(queda, 2),
        "penalidadeTendencia": penalidadeTendencia,
        "projecaoSaude": {
            "scoreAtual": round(scoreFinal, 2),
            "scoreProjetado": round(scoreProjetado, 2),
            "risco": classificarStatusScore(scoreProjetado),
            "motivo": motivoProjecao,
            "componentesTendencia": componentesTendencia,
            "penalidadeProjecao": penalidadeProjecao
        }
    }
   

#SCORE SAUDE ZONA 
def calcularScoreZona(servidoresZona):
    total = len(servidoresZona)

    if total == 0:
        return {
            "score": 100,
            "status": "Saudável"
        }

    qntCriticos = 0
    qntAtencao = 0
    srvPiorScore = 100

    for servidor in servidoresZona:
        score = servidor["score"]
        status = servidor["status"]

        if status == "Crítico":
            qntCriticos += 1
        elif status == "Atenção":
            qntAtencao += 1

        if score < srvPiorScore:
            srvPiorScore = score

    percentCriticos = qntCriticos / total
    percentAtencao = qntAtencao / total

    if percentCriticos > 0.40:
        penalidadeCritico = 40
    elif percentCriticos > 0.25:
        penalidadeCritico = 30
    elif percentCriticos > 0.10:
        penalidadeCritico = 20
    elif percentCriticos > 0:
        penalidadeCritico = 10
    else:
        penalidadeCritico = 0

    if percentAtencao > 0.50:
        penalidadeAtencao = 15
    elif percentAtencao > 0.25:
        penalidadeAtencao = 10
    elif percentAtencao > 0:
        penalidadeAtencao = 5
    else:
        penalidadeAtencao = 0

    if srvPiorScore < 40:
        penalidadePiorSrv = 15
    elif srvPiorScore < 60:
        penalidadePiorSrv = 10
    elif srvPiorScore < 80:
        penalidadePiorSrv = 5
    else:
        penalidadePiorSrv = 0

    score_zona = 100 - penalidadeCritico - penalidadeAtencao - penalidadePiorSrv
    score_zona = max(0, min(100, score_zona))

    return {
        "score": round(score_zona, 2),
        "status": classificarStatusScore(score_zona),
        "qntServidores": total,
        "qntCriticos": qntCriticos,
        "qntAtencao": qntAtencao,
        "srvPiorScore": round(srvPiorScore, 2)
    }

#SCORE SAUDE DATACENTER 

def calcularScoreDatacenter(zonas):
    total = len(zonas)

    if total == 0:
        return {
            "score": 100,
            "status": "Saudável"
        }

    qntCriticos = 0
    qntAtencao = 0
    zonaPiorScore = 100

    for zona in zonas:
        score = zona["score"]
        status = zona["status"]

        if status == "Crítico":
            qntCriticos += 1
        elif status == "Atenção":
            qntAtencao += 1

        if score < zonaPiorScore:
            zonaPiorScore = score

    percentCriticos = qntCriticos / total
    percentAtencao = qntAtencao / total

    if percentCriticos > 0.50:
        penalidadeCritico = 40
    elif percentCriticos > 0.25:
        penalidadeCritico = 25
    elif percentCriticos > 0:
        penalidadeCritico = 15
    else:
        penalidadeCritico = 0

    if percentAtencao > 0.50:
        penalidadeAtencao = 15
    elif percentAtencao > 0:
        penalidadeAtencao = 8
    else:
        penalidadeAtencao = 0

    if zonaPiorScore < 40:
        penalidadePiorZona = 20
    elif zonaPiorScore < 60:
        penalidadePiorZona = 12
    elif zonaPiorScore < 80:
        penalidadePiorZona = 6
    else:
        penalidadePiorZona = 0

    score_datacenter = 100 - penalidadeCritico - penalidadeAtencao - penalidadePiorZona
    score_datacenter = max(0, min(100, score_datacenter))

    return {
        "score": round(score_datacenter, 2),
        "status": classificarStatusScore(score_datacenter),
        "qntZonas": total,
        "qntZonasCriticas": qntCriticos,
        "qntZonasAtencao": qntAtencao,
        "zonaPiorScore": round(zonaPiorScore, 2)
    }

#------------------------------------------------------------------------------------------------------------------------------------------------
#Projecao de score e tendencia de componentesque vão aumentar criticamente

#calculando a média de cada componente
def calcularMediaComponente(janela, campo):
    if not janela:
        return 0

    soma = 0

    for coleta in janela:
        soma += converter_float(coleta.get(campo))

    return soma / len(janela)



#def que calcula a persistencia de uso de cada componente e a sua relação com o limite dele
def calcularPersistenciaComponente(janela, campo, limite):
    if not janela:
        return 0

    qntColetasCriticas = 0

    for coleta in janela:
        valor = converter_float(coleta.get(campo))

        if valor > limite:
            qntColetasCriticas += 1

    return qntColetasCriticas / len(janela)


def obterPesoComponente(nomeComponente):
    pesos = {
        "CPU": 1.0,
        "RAM": 1.2,
        "Disco": 1.3,
        "Latência": 0.9
    }

    return pesos.get(nomeComponente, 1.0)

#def que calcula a tendencia do socmponentes levando em consideração a persistencia atual comparando com a anterior e tambḿe com amédia de uso de cadac coponente da atual vs anterior, com um peso para cada situação
def calcularTendenciaComponentes(janelaAnterior, janelaAtual, limites):

    if len(janelaAnterior) < 10 or len(janelaAtual) < 10:
        return []

    limiteCpu = converter_float(limites.get("CPU"), LIMITE_CPU)
    limiteRam = converter_float(limites.get("RAM"), LIMITE_RAM)
    limiteDisco = converter_float(limites.get("DISCO"), LIMITE_DISCO)
    limiteLatencia = converter_float(limites.get("REDE"), LIMITE_LATENCIA)

    componentes = [
        {
            "nome": "CPU",
            "campo": "CPU_PER",
            "limite": limiteCpu
        },
        {
            "nome": "RAM",
            "campo": "RAM_PER",
            "limite": limiteRam
        },
        {
            "nome": "Disco",
            "campo": "DISCO_PER",
            "limite": limiteDisco
        },
        {
            "nome": "Latência",
            "campo": "LATENCIA",
            "limite": limiteLatencia
        }
    ]

    tendencias = []

    for componente in componentes:

        persistenciaAnterior = calcularPersistenciaComponente(
            janelaAnterior,
            componente["campo"],
            componente["limite"]
        )

        persistenciaAtual = calcularPersistenciaComponente(
            janelaAtual,
            componente["campo"],
            componente["limite"]
        )

        aumentoPersistencia = persistenciaAtual - persistenciaAnterior

        mediaAtual = calcularMediaComponente(
            janelaAtual,
            componente["campo"]
        )

        if componente["limite"] > 0:
            proximidadeLimite = mediaAtual / componente["limite"]
        else:
            proximidadeLimite = 0

        riscoTendencia = 0

        if aumentoPersistencia >= 0.50:
            riscoTendencia += 15
        elif aumentoPersistencia >= 0.30:
            riscoTendencia += 10
        elif aumentoPersistencia >= 0.15:
            riscoTendencia += 5

        if persistenciaAtual >= 0.80:
            riscoTendencia += 8
        elif persistenciaAtual >= 0.60:
            riscoTendencia += 5
        elif persistenciaAtual >= 0.40:
            riscoTendencia += 3

        if proximidadeLimite >= 1:
            riscoTendencia += 10
        elif proximidadeLimite >= 0.90:
            riscoTendencia += 7
        elif proximidadeLimite >= 0.80:
            riscoTendencia += 4

        pesoComponente = obterPesoComponente(componente["nome"])
        riscoTendencia = riscoTendencia * pesoComponente

        if riscoTendencia >= 5:
            tendencias.append({
                "componente": componente["nome"],
                "persistenciaAnterior": round(persistenciaAnterior * 100, 2),
                "persistenciaAtual": round(persistenciaAtual * 100, 2),
                "aumentoPersistencia": round(aumentoPersistencia * 100, 2),
                "mediaAtual": round(mediaAtual, 2),
                "proximidadeLimite": round(proximidadeLimite, 2),
                "riscoTendencia": round(riscoTendencia, 2)
            })

    tendencias.sort(
        key=lambda item: item["riscoTendencia"],
        reverse=True
    )

    return tendencias

#def que identifica o motivo da projeção, quais componentes vão resultar naquilo
def gerarMotivoProjecao(componentesTendencia):
    if len(componentesTendencia) == 0:
        return "Sem riscos relevantes"

    if len(componentesTendencia) == 1:
        componente = componentesTendencia[0]

        return (
            f"{componente['componente']} com aumento de persistência crítica "
            f"({componente['aumentoPersistencia']} p.p.)"
        )

    componente1 = componentesTendencia[0]
    componente2 = componentesTendencia[1]

    return (
        f"{componente1['componente']} e {componente2['componente']} "
        f"com aumento de persistência crítica"
    )

#def que calcula a penalidade da previsão para montarnos o score previsto
def calcularPenalidadeProjecao(componentesTendencia):
    penalidade = 0

    for componente in componentesTendencia:
        risco = componente["riscoTendencia"]

        if risco >= 25:
            penalidade += 10
        elif risco >= 15:
            penalidade += 7
        elif risco >= 5:
            penalidade += 4

    return min(penalidade, 25)

#------------------------------------------------------------------------------------------------------------------------------------------------
#gerando o uptime de cada servidor

def classificarStatusUptime(uptime):
    if uptime >= 99:
        return "Saudável"
    elif uptime >= 95:
        return "Atenção"
    return "Crítico"

def obterIntervaloIndisponibilidade(chamado, inicioPeriodo, fimPeriodo):
    abertoEm = converterDatetime(chamado.get("aberto_em"))
    resolvidoEm = converterDatetime(chamado.get("resolvido_em"))

    if abertoEm is None:
        return None

    if resolvidoEm is None:
        resolvidoEm = fimPeriodo

    inicioIntervalo = max(abertoEm, inicioPeriodo)
    fimIntervalo = min(resolvidoEm, fimPeriodo)

    if fimIntervalo <= inicioIntervalo:
        return None

    return {
        "inicio": inicioIntervalo,
        "fim": fimIntervalo
    }

def juntarIntervalos(intervalos):
    if not intervalos:
        return []

    intervalosOrdenados = sorted(
        intervalos,
        key=lambda intervalo: intervalo["inicio"]
    )

    intervalosUnidos = [intervalosOrdenados[0]]

    for intervaloAtual in intervalosOrdenados[1:]:
        ultimoIntervalo = intervalosUnidos[-1]

        if intervaloAtual["inicio"] <= ultimoIntervalo["fim"]:
            ultimoIntervalo["fim"] = max(
                ultimoIntervalo["fim"],
                intervaloAtual["fim"]
            )
        else:
            intervalosUnidos.append(intervaloAtual)

    return intervalosUnidos
def calcularUptimeServidor(chamadosServidor, servidor, zona, inicioPeriodo, fimPeriodo):
    tempoTotalHoras = (fimPeriodo - inicioPeriodo).total_seconds() / 3600

    intervalos = []
    qtdChamadosCriticos = 0

    for chamado in chamadosServidor:
        if str(chamado.get("severidade", "")).lower() != "critico":
            continue

        qtdChamadosCriticos += 1

        intervalo = obterIntervaloIndisponibilidade(
            chamado,
            inicioPeriodo,
            fimPeriodo
        )

        if intervalo is not None:
            intervalos.append(intervalo)

    intervalosUnidos = juntarIntervalos(intervalos)

    tempoIndisponivelHoras = 0

    for intervalo in intervalosUnidos:
        duracaoHoras = (
            intervalo["fim"] - intervalo["inicio"]
        ).total_seconds() / 3600

        tempoIndisponivelHoras += duracaoHoras

    if tempoTotalHoras <= 0:
        uptime = 100
    else:
        uptime = (
            (tempoTotalHoras - tempoIndisponivelHoras)
            / tempoTotalHoras
        ) * 100

    uptime = max(0, min(100, uptime))

    return {
        "servidor": servidor,
        "zona": zona,
        "uptime": round(uptime, 2),
        "tempoIndisponivelHoras": round(tempoIndisponivelHoras, 2),
        "qtdChamadosCriticos": qtdChamadosCriticos,
        "qtdIntervalosIndisponibilidade": len(intervalosUnidos),
        "statusUptime": classificarStatusUptime(uptime)
    }

def filtrarChamadosServidor(chamados, empresa, datacenter, zona, servidor):
    chamadosServidor = []

    for chamado in chamados:
        if (
            str(chamado.get("empresa", "")) == str(empresa)
            and str(chamado.get("datacenter", "")) == str(datacenter)
            and str(chamado.get("zona", "")) == str(zona)
            and str(chamado.get("servidor", "")) == str(servidor)
        ):
            chamadosServidor.append(chamado)

    return chamadosServidor

def converterDatetime(valor):
    if valor is None:
        return None

    if isinstance(valor, datetime):
        return valor.replace(tzinfo=None)

    if isinstance(valor, str):
        return datetime.fromisoformat(valor).replace(tzinfo=None)

    return valor
def carregarChamadosCriticosUptime(inicioPeriodo, fimPeriodo):
    conexao = None

    try:
        conexao = conectarBanco()

        with conexao.cursor() as cursor:
            sql = """
                SELECT DISTINCT
                    e.razaoSocial AS empresa,
                    dc.nome AS datacenter,
                    z.nome AS zona,
                    s.nome AS servidor,
                    ra.severidade,
                    ra.issue_key,
                    ra.aberto_em,
                    ra.resolvido_em
                FROM registros_alertas ra
                JOIN servidor s
                    ON s.idServidor = ra.fkRegistroServidor
                JOIN zona z
                    ON z.idZona = s.fkZona
                JOIN datacenter dc
                    ON dc.idDataCenter = z.fkDataCenter
                JOIN regiao r
                    ON r.fkRegiaoDataCenter = dc.idDataCenter
                JOIN empresa e
                    ON e.idEmpresa = r.fkRegiaoEmpresa
                WHERE ra.severidade = 'critico'
                  AND ra.aberto_em < %s
                  AND (ra.resolvido_em IS NULL OR ra.resolvido_em > %s)
            """

            cursor.execute(sql, (fimPeriodo, inicioPeriodo))
            chamados = cursor.fetchall()

            print(f"✅ Chamados críticos carregados: {len(chamados)}")

            return chamados

    except Exception as e:
        print(f"⚠️ Erro ao buscar chamados críticos: {e}")
        return []

    finally:
        if conexao is not None:
            conexao.close()
#------------------------------------------------------------------------------------------------------------------------------------------------

#alertas semana
def carregarHistoricoAlertas(bucket):
    pathHistorico = "trusted/alertas_historico.json"
    try:
        resp_hist = s3.get_object(
            Bucket=bucket,
            Key=pathHistorico
        )
        historicoAlertas = json.loads(
            resp_hist['Body'].read().decode('utf-8')
        )
        print(f"✅ Histórico carregado: {len(historicoAlertas)} alertas")
        return historicoAlertas
    
    except Exception as e:
        print(f"⚠️ Histórico não encontrado: {e}")
        return []

#def que calcula a quantidade de alertas em cada dia da semana
def calcularAlertaSemana( historicoAlertas, empresa,datacenter):

    diasSemana = {
        0: "Segunda",
        1: "Terça",
        2: "Quarta",
        3: "Quinta",
        4: "Sexta",
        5: "Sábado",
        6: "Domingo"
    }

    alertasPorDia = {
        "Segunda": 0,
        "Terça": 0,
        "Quarta": 0,
        "Quinta": 0,
        "Sexta": 0,
        "Sábado": 0,
        "Domingo": 0
    }

    agora = datetime.now()
    inicioSemana = agora - timedelta(days=agora.weekday())
    fimSemana = inicioSemana + timedelta(days=7)

    for alerta in historicoAlertas:
        if (
            str(alerta.get("empresa", "")) != str(empresa)
            or str(alerta.get("datacenter", "")) != str(datacenter)
        ):
            continue

        ts = str(alerta.get("timestamp", ""))
        try:
            dataAlerta = datetime.fromisoformat(ts).replace(tzinfo=None)
        except:
            continue

        if dataAlerta < inicioSemana:
            continue

        if dataAlerta >= fimSemana:
            continue

        nomeDia = diasSemana[dataAlerta.weekday()]
        alertasPorDia[nomeDia] += 1

    totalAlertas = sum(alertasPorDia.values())

    mediaAlertas = round(totalAlertas / 7, 2)

    alertasPorDia["media"] = mediaAlertas

    return alertasPorDia
#------------------------------------------------------------------------------------------------------------------------------------------------
#Montando kpis!
#kpi de srv saude critica X total srv
def calcularKpiServidoresCriticos(servidores_datacenter):
    totalServidores = len(servidores_datacenter)

    if totalServidores == 0:
        return {
            "qtdCriticos": 0,
            "totalServidores": 0,
            "percentualCriticos": 0,
            "descricao": "Nenhum servidor encontrado."
        }

    qtdCriticos = 0

    for servidor in servidores_datacenter:
        if servidor["status"] == "Crítico":
            qtdCriticos += 1

    percentualCriticos = (qtdCriticos / totalServidores) * 100

    return {
        "qtdCriticos": qtdCriticos,
        "totalServidores": totalServidores,
        "percentualCriticos": round(percentualCriticos, 2),
        "descricao": f"{round(percentualCriticos, 2)}% dos servidores estão com o score abaixo do ideal."
    }

#kpi de percentual de crescimento dos incidentes
def calcularCrescimentoIncidentes(historicoAlertas, empresa, datacenter):
    agora = datetime.now()

    inicioSemanaAtual = agora - timedelta(days=agora.weekday())
    fimSemanaAtual = inicioSemanaAtual + timedelta(days=7)

    inicioSemanaAnterior = inicioSemanaAtual - timedelta(days=7)
    fimSemanaAnterior = inicioSemanaAtual

    alertasSemanaAtual = 0
    alertasSemanaAnterior = 0

    for alerta in historicoAlertas:
        if (
            str(alerta.get("empresa", "")) != str(empresa)
            or str(alerta.get("datacenter", "")) != str(datacenter)
        ):
            continue

        ts = str(alerta.get("timestamp", ""))

        try:
            dataAlerta = datetime.fromisoformat(ts).replace(tzinfo=None)
        except:
            continue

        if inicioSemanaAtual <= dataAlerta < fimSemanaAtual:
            alertasSemanaAtual += 1

        elif inicioSemanaAnterior <= dataAlerta < fimSemanaAnterior:
            alertasSemanaAnterior += 1
    if alertasSemanaAnterior == 0:
        if alertasSemanaAtual > 0:
            crescimentoPercentual = None
            valorFormatado = "Novo"
            tendencia = "aumento"
        else:
            crescimentoPercentual = 0
            valorFormatado = "0%"
            tendencia = "estável"
    else:
        crescimentoPercentual = (
            (alertasSemanaAtual - alertasSemanaAnterior)
            / alertasSemanaAnterior
        ) * 100

        if crescimentoPercentual > 0:
            valorFormatado = f"+{round(crescimentoPercentual, 2)}%"
            tendencia = "aumento"
        elif crescimentoPercentual < 0:
            valorFormatado = f"{round(crescimentoPercentual, 2)}%"
            tendencia = "queda"

        else:
            valorFormatado = "0%"
            tendencia = "estável"


    return {
        "percentual": round(crescimentoPercentual, 2),
        "valorFormatado": valorFormatado,
        "alertasSemanaAtual": alertasSemanaAtual,
        "alertasSemanaAnterior": alertasSemanaAnterior,
        "descricao": f"De {alertasSemanaAnterior} alertas na semana anterior para {alertasSemanaAtual} alertas na semana atual",
        "tendencia": tendencia
    }


#kpi do uptime
def calcularKpiUptime(uptimeServidores, limiteUptimeIdeal=99):
    totalServidores = len(uptimeServidores)

    if totalServidores == 0:
        return {
            "qtdServidoresInstaveis": 0,
            "totalServidores": 0,
            "percentualInstaveis": 0,
            "limiteIdeal": limiteUptimeIdeal,
            "valorFormatado": "0/0",
            "descricao": "Nenhum servidor encontrado."
        }

    qtdServidoresInstaveis = 0

    for servidor in uptimeServidores:
        if servidor["uptime"] < limiteUptimeIdeal:
            qtdServidoresInstaveis += 1

    percentualInstaveis = (qtdServidoresInstaveis / totalServidores) * 100

    return {
        "qtdServidoresInstaveis": qtdServidoresInstaveis,
        "totalServidores": totalServidores,
        "percentualInstaveis": round(percentualInstaveis, 2),
        "limiteIdeal": limiteUptimeIdeal,
        "valorFormatado": f"{qtdServidoresInstaveis}/{totalServidores}",
        "descricao": f"{round(percentualInstaveis, 2)}% dos servidores estão com o uptime abaixo do ideal."
    }
#---------------------------------------------------------------------------------------------------------------------------------

def dashOperacional(dados, geral, bucket):
    print("\n🚀 ENTREI NA DASH OPERACIONAL")

    df = pd.DataFrame(dados)


    if df.empty:
        print("⚠️ DataFrame vazio")
        return {
            "tipo": "gestora",
            "total_dados": 0,
            "datacenters": {}
        }

    df["DATE"] = pd.to_datetime(df["DATE"])
    historicoAlertas = carregarHistoricoAlertas(bucket)

    fimPeriodoUptime = datetime.now()
    inicioPeriodoUptime = fimPeriodoUptime - timedelta(days=7)
    chamadosCriticosUptime = carregarChamadosCriticosUptime(
    inicioPeriodoUptime,
    fimPeriodoUptime
)
    resultado = {}

    for (empresa, datacenter), df_dc in df.groupby(["EMPRESA", "DATACENTER"]):

        print(f"\n🏢 DATACENTER: {datacenter}")

        graficoAlertasSemana = calcularAlertaSemana(
            historicoAlertas,
            empresa,
            datacenter)
        
        zonas = []
        servidores_datacenter = []
        uptimeServidores = []
        for zona, df_zona in df_dc.groupby("ZONA"):

            print(f"\n📍 ZONA: {zona}")

            servidoresZona = []

            for servidor, df_servidor in df_zona.groupby("SERVIDOR"):

                print(f"\n🖥️ SERVIDOR: {servidor}")

                try:
                    info_servidor = geral[empresa][datacenter][zona][servidor]
                    limites = info_servidor.gkpiUptime = calcularKpiUptime(uptimeServidores)et("limites", {})

                    print("✅ Limites encontrados:", limites)

                except (KeyError, TypeError):
                    limites = {}

                    print("⚠️ Limites não encontrados. Usando fallback.")
                chamadosServidor = filtrarChamadosServidor(
                    chamadosCriticosUptime,
                    empresa,
                    datacenter,
                    zona,
                    servidor
                )

                resultadoUptime = calcularUptimeServidor(
                    chamadosServidor,
                    servidor,
                    zona,
                    inicioPeriodoUptime,
                    fimPeriodoUptime
                )
                uptimeServidores.append(resultadoUptime)
                df_servidor = df_servidor.sort_values("DATE")

                coletaServidor = df_servidor.to_dict(orient="records")

                print(f"📦 Quantidade de coletas: {len(coletaServidor)}")

                resultadoScore = calcularScoreServidor(
                    coletaServidor,
                    limites
                )

                print("📊 SCORE SERVIDOR:", resultadoScore)

                servidor_obj = {
                    "servidor": servidor,
                    "zona": zona,
                    "score": resultadoScore["score"],
                    "status": resultadoScore["status"],
                    "scoreParcialAtual": resultadoScore["scoreParcialAtual"],
                    "scoreParcialAnterior": resultadoScore["scoreParcialAnterior"],
                    "queda": resultadoScore["queda"],
                    "penalidadeTendencia": resultadoScore["penalidadeTendencia"],
                    "projecaoSaude": resultadoScore["projecaoSaude"],
                    "uptimeOperacional": resultadoUptime
                }

                servidoresZona.append(servidor_obj)
                servidores_datacenter.append(servidor_obj)

            resultadoZona = calcularScoreZona(servidoresZona)

            print("📊 SCORE ZONA:", resultadoZona)

            zona_obj = {
                "zona": zona,
                "score": resultadoZona["score"],
                "status": resultadoZona["status"],
                "qntServidores": resultadoZona["qntServidores"],
                "qntCriticos": resultadoZona["qntCriticos"],
                "qntAtencao": resultadoZona["qntAtencao"],
                "srvPiorScore": resultadoZona["srvPiorScore"],
                "servidores": servidoresZona
            }

            zonas.append(zona_obj)

            print(f"✅ JSON da zona {zona} criado")

        resultadoDatacenter = calcularScoreDatacenter(zonas)

        print("📊 SCORE DATACENTER:", resultadoDatacenter)

        rankingSrvCriticosTop5 = sorted(
            servidores_datacenter,
            key=lambda servidor: servidor["score"]
        )[:5]

        kpiCrescimentoIncidentes = calcularCrescimentoIncidentes(
            historicoAlertas,
            empresa,
            datacenter
        )

        kpiServidoresCriticos = calcularKpiServidoresCriticos(
            servidores_datacenter
        )

        print("🏆 TOP 5 SERVIDORES CRÍTICOS:")
        for srv in rankingSrvCriticosTop5:
            print(
                f"➡️ {srv['servidor']} | "
                f"Score: {srv['score']} | "
                f"Status: {srv['status']}"
            )

        kpiUptime = calcularKpiUptime(uptimeServidores)

        resultado.setdefault(empresa, {})

        resultado[empresa][datacenter] = {
            "score": resultadoDatacenter["score"],
            "status": resultadoDatacenter["status"],
            "qntZonas": resultadoDatacenter["qntZonas"],
            "qntZonasCriticas": resultadoDatacenter["qntZonasCriticas"],
            "qntZonasAtencao": resultadoDatacenter["qntZonasAtencao"],
            "zonaPiorScore": resultadoDatacenter["zonaPiorScore"],
            "zonas": zonas,
            "rankingSrvCriticosTop5": rankingSrvCriticosTop5,
            "graficoAlertasSemana": graficoAlertasSemana,
            "uptimeServidores": uptimeServidores,
            "kpiUptime": kpiUptime,
            "kpiCrescimentoIncidentes": kpiCrescimentoIncidentes,
            "kpiServidoresCriticos": kpiServidoresCriticos

        }

        print(f"✅ JSON FINAL DO DATACENTER {datacenter} CRIADO")

    print("\n🎉 DASH OPERACIONAL FINALIZADA")

    return {
        "tipo": "gestora",
        "total_dados": len(dados),
        "datacenters": resultado
    }

