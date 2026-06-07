import csv
import json
import boto3
import io  
import pandas as pd 
from urllib.parse import unquote_plus
from datetime import datetime, timedelta
from statistics import median
from zoneinfo import ZoneInfo

s3 = boto3.client('s3')

# Função inicial que chama as demais
def lambda_handler(event, context):
    print("Lambda Op Iniciada! 🪧")
    print(f"Evento recebido: {event}")
    
    try:
        # Desempacota a mensagem do SNS
        mensagem_sns_texto = event["Records"][0]["Sns"]["Message"]
        evento_s3_real = json.loads(mensagem_sns_texto)
        
        if evento_s3_real.get("Event") == "s3:TestEvent":
            print("Evento de teste do S3 ignorado")
            return {"statusCode": 200,"body": "TestEvent ignorado"}
        registro = evento_s3_real["Records"][0]["s3"]
        bucket = registro["bucket"]["name"]
        key = unquote_plus(registro["object"]["key"])
        
        resultado_processamento = ClientGeral(bucket, key)
        
        return {
            "statusCode": 200,
            "body": resultado_processamento
        }

    except Exception as e:
        print(f"❌ Erro fatal na execução da Lambda: {e}")
        return {
            "statusCode": 500,
            "body": f"Erro interno: {str(e)}"
        }
    
def carregarMetricasJson(bucket):
    try:
        resposta = s3.get_object(
            Bucket=bucket,
            Key="raw/metricas.json"
        )

        conteudo = resposta["Body"].read().decode("utf-8")
        metricasJson = json.loads(conteudo)

        print("✅ metricas.json carregado do S3")
        return metricasJson

    except Exception as e:
        print(f"⚠️ Erro ao carregar metricas.json: {e}")
        return {}
    
def padronizarNomeZona(valor):
    if valor is None or pd.isna(valor):
        return valor

    textoNormalizado = normalizarTextoEstrutura(
        valor
    )


    mapeamento = {
        "zonaa": "Zona A",
        "zonab": "Zona B",
        "zonac": "Zona C"
    }

    return mapeamento.get(textoNormalizado,str(valor).strip())


def padronizarNomeServidor(valor):
    if valor is None or pd.isna(valor):
        return valor

    textoNormalizado = normalizarTextoEstrutura(valor)

    if textoNormalizado.startswith("servidor"):
        restante = textoNormalizado.replace("servidor", "",1)

        if len(restante) >= 4:
            uf = restante[:2].upper()
            numero = restante[2:]

            if numero.isdigit():
                return (
                    f"SERVIDOR-{uf}-"
                    f"{numero.zfill(2)}"
                )

    return str(valor).strip()

def verificarDuplicidadeServidores(dataframe):
    associacoes = (
        dataframe[
            [
                "EMPRESA",
                "DATACENTER",
                "ZONA",
                "SERVIDOR"
            ]
        ]
        .drop_duplicates()
    )

    quantidadeZonasPorServidor = (
        associacoes
        .groupby(
            [
                "EMPRESA",
                "DATACENTER",
                "SERVIDOR"
            ]
        )["ZONA"]
        .nunique()
    )

    duplicados = quantidadeZonasPorServidor[
        quantidadeZonasPorServidor > 1
    ]

    if duplicados.empty:
        print("✅ Nenhum servidor associado "
            "a mais de uma zona."
        )
        return

    print(
        "⚠️ Servidores associados a mais de uma zona:"
    )

    print(duplicados)


def prepararDataframesDashboard(caminhoLocal):
    colunasNecessarias = [
        "EMPRESA",
        "REGIAO",
        "DATACENTER",
        "ZONA",
        "SERVIDOR",
        "DATE",
        "CPU_PER",
        "RAM_PER",
        "DISCO_PER",
        "LATENCIA"
    ]

    agora = obterAgoraSaoPaulo()

    inicioSemanaAtual = (
        agora
        - timedelta(days=agora.weekday())
    ).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0
    )

    inicioSemanaAnterior = (
        inicioSemanaAtual
        - timedelta(days=7)
    )

    colunasAgrupamento = [
        "EMPRESA",
        "REGIAO",
        "DATACENTER",
        "ZONA",
        "SERVIDOR"
    ]

    acumuladoScore = pd.DataFrame(
        columns=colunasNecessarias
    )

    partesAlertas = []

    totalLido = 0

    leitorChunks = pd.read_csv(
        caminhoLocal,
        delimiter=";",
        encoding="utf-8-sig",
        usecols=colunasNecessarias,
        low_memory=False,
        chunksize=50_000
    )

    for numeroChunk, chunk in enumerate(
        leitorChunks,
        start=1
    ):
        totalLido += len(chunk)

        chunk["DATE"] = pd.to_datetime(
            chunk["DATE"],
            format="mixed",
            errors="coerce"
        )

        chunk = chunk.dropna(
            subset=[
                "DATE",
                "EMPRESA",
                "REGIAO",
                "DATACENTER",
                "ZONA",
                "SERVIDOR"
            ]
        )

        # Normalização vetorizada, mais leve que apply().
        zonaNormalizada = (
            chunk["ZONA"]
            .astype(str)
            .str.strip()
            .str.lower()
            .str.replace(" ", "", regex=False)
            .str.replace("-", "", regex=False)
            .str.replace("_", "", regex=False)
        )

        mapaZonas = {
            "zonaa": "Zona A",
            "zonab": "Zona B",
            "zonac": "Zona C"
        }

        chunk["ZONA"] = (
            zonaNormalizada
            .map(mapaZonas)
            .fillna(
                chunk["ZONA"]
                .astype(str)
                .str.strip()
            )
        )

        chunk["SERVIDOR"] = (
            chunk["SERVIDOR"]
            .astype(str)
            .str.strip()
            .str.upper()
        )

        chunk = chunk.drop_duplicates(
            subset=[
                "EMPRESA",
                "REGIAO",
                "DATACENTER",
                "ZONA",
                "SERVIDOR",
                "DATE"
            ],
            keep="last"
        )

        chunkAlertas = chunk[
            (chunk["DATE"] >= inicioSemanaAnterior)
            & (chunk["DATE"] <= agora)
        ]

        if not chunkAlertas.empty:
            partesAlertas.append(
                chunkAlertas.copy()
            )

        combinadoScore = pd.concat(
            [
                acumuladoScore,
                chunk
            ],
            ignore_index=True
        )

        acumuladoScore = (
            combinadoScore
            .sort_values("DATE")
            .groupby(
                colunasAgrupamento,
                group_keys=False,
                sort=False
            )
            .tail(60)
            .copy()
        )

        del combinadoScore
        del chunk
        del chunkAlertas

        print(
            f"Chunk {numeroChunk} processado. "
            f"Total lido: {totalLido}. "
            f"Acumulado score: {len(acumuladoScore)}."
        )

    if partesAlertas:
        dfAlertas = pd.concat(
            partesAlertas,
            ignore_index=True
        )

        dfAlertas = dfAlertas.drop_duplicates(
            subset=[
                "EMPRESA",
                "DATACENTER",
                "ZONA",
                "SERVIDOR",
                "DATE"
            ],
            keep="last"
        )
    else:
        dfAlertas = pd.DataFrame(
            columns=colunasNecessarias
        )

    dfScore = acumuladoScore.reset_index(
        drop=True
    )

    print(
        f"Total lido do trusted: {totalLido}"
    )

    print(
        f"Linhas finais para score: {len(dfScore)}"
    )

    print(
        f"Linhas finais para alertas: {len(dfAlertas)}"
    )

    return dfScore, dfAlertas

def ClientGeral(bucket, chave):
    print(f"Lendo arquivo Trusted no S3: {chave}")

    caminhoLocal = "/tmp/dados_tratados.csv"

    s3.download_file(
        bucket,
        chave,
        caminhoLocal
    )

    print("Trusted baixado.")

    geral = carregarMetricasJson(
        bucket
    )

    dfScore, dfAlertas = (
        prepararDataframesDashboard(
            caminhoLocal
        )
    )

    verificarDuplicidadeServidores(
        dfScore
    )

    respGestoraOp = dashOperacional(
        dfScore,
        dfAlertas,
        geral,
        bucket
    )

    conteudoJson = json.dumps(
        respGestoraOp,
        default=str,
        ensure_ascii=False,
        separators=(",", ":")
    )

    s3.put_object(
        Bucket=bucket,
        Key="client/dashOpGestao.json",
        Body=conteudoJson.encode("utf-8"),
        ContentType="application/json",
        CacheControl="no-cache"
    )

    print(
        "Tamanho do JSON:",
        len(conteudoJson.encode("utf-8")),
        "bytes"
    )

    print(
        "Todas as páginas processadas "
        "e atualizadas. 🟩"
    )

    return "Lambda concluída com sucesso! ✅"
#------------------------------------------------------------------------------Scores------------------------------------------------------------
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

    componentesCriticos = []

    if persistenciaCpu > 0:
        componentesCriticos.append({
            "componente": "CPU",
            "persistencia": round(persistenciaCpu * 100, 1)
        })

    if persistenciaRam > 0:
        componentesCriticos.append({
            "componente": "RAM",
            "persistencia": round(persistenciaRam * 100, 1)
        })

    if persistenciaDisco > 0:
        componentesCriticos.append({
            "componente": "DISCO",
            "persistencia": round(persistenciaDisco * 100, 1)
        })

    if persistenciaLatencia > 0:
        componentesCriticos.append({
            "componente": "LATÊNCIA",
            "persistencia": round(persistenciaLatencia * 100, 1)
        })
        
    return {
    "score": score_parcial,
    "componentesCriticos": componentesCriticos
       }

#SCORE DE SAUDE SERVIDOR
def calcularScoreServidor(coletaServidor, limites):
    if not coletaServidor:
        return {
            "score": 100,
            "componentesCriticos": [],
            "status": "Saudável",
            "scoreParcialAtual": 100,
            "scoreParcialAnterior": 100,
            "variacaoScore": 0,
            "tendenciaDegradacao": {
                "possuiRisco": False,
                "nivelRisco": "insuficiente",
                "variacaoScore": 0,
                "motivo": ("Sem dados suficientes " "para análise."),
                "componentesTendencia": []
            }
        }

    coletasOrdenadas = sorted(
        coletaServidor,
        key=lambda coleta: pd.to_datetime(
            coleta.get("DATE"),
            errors="coerce"
        )
    )

    ultimas60Linhas = coletasOrdenadas[-60:]

    if len(ultimas60Linhas) >= 60:
        janelaAnterior = ultimas60Linhas[:30]
        janelaAtual = ultimas60Linhas[30:]

    elif len(ultimas60Linhas) >= 40:
        metade = len(ultimas60Linhas) // 2

        janelaAnterior = ultimas60Linhas[:metade]
        janelaAtual = ultimas60Linhas[metade:]

    else:
        janelaAnterior = []
        janelaAtual = ultimas60Linhas[-30:]

    resultadoAtual = calcularScoreParcial(
        janelaAtual,
        limites
    )

    scoreParcialAtual = resultadoAtual["score"]
    componentesCriticos = resultadoAtual["componentesCriticos"]

    if janelaAnterior:
        resultadoAnterior = calcularScoreParcial(janelaAnterior,limites)

        scoreParcialAnterior = resultadoAnterior["score"]

    else:
        scoreParcialAnterior = scoreParcialAtual

    scoreFinal = max(0,min(100, scoreParcialAtual))

    resultadoTendencia = (
    calcularTendenciaServidor(
        janelaAnterior,
        janelaAtual,
        limites,
        scoreParcialAnterior,
        scoreParcialAtual
    ))

    
    return {
        "score": round(scoreFinal, 2),
        "status": classificarStatusScore(scoreFinal),
        "scoreParcialAtual": round(scoreParcialAtual, 2),
        "scoreParcialAnterior": round( scoreParcialAnterior,2),
        "variacaoScore": round(scoreParcialAtual - scoreParcialAnterior, 2 ),
        "tendenciaDegradacao": ( resultadoTendencia),
        "componentesCriticidade": componentesCriticos,
    }

#SCORE SAUDE ZONA 
def calcularScoreZona(servidoresZona):
    total = len(servidoresZona)

    if total == 0:
        return {
            "score": 100,
            "status": "Saudável",
            "qntServidores": 0,
            "qntCriticos": 0,
            "qntAtencao": 0,
            "srvPiorScore": 100
        }

    qntCriticos = 0
    qntAtencao = 0
    srvPiorScore = 100

    for servidor in servidoresZona:
        score = converter_float(
            servidor.get("score"),
            100
        )

        status = servidor.get(
            "status",
            "Saudável"
        )

        if status == "Crítico":
            qntCriticos += 1
        elif status == "Atenção":
            qntAtencao += 1

        if score < srvPiorScore:
            srvPiorScore = score

    percentualCriticos = qntCriticos / total
    percentualAtencao = qntAtencao / total
    penalidadeCriticos = percentualCriticos * 45
    penalidadeAtencao = percentualAtencao * 20

    if srvPiorScore < 80:
        penalidadePiorServidor = (80 - srvPiorScore) * 0.30
    else:
        penalidadePiorServidor = 0

    penalidadePiorServidor = min(penalidadePiorServidor,20)

    if total == 1:
        scoreZona = srvPiorScore
    else:
        scoreZona = (100- penalidadeCriticos - penalidadeAtencao- penalidadePiorServidor)

    scoreZona = max(0,min(100, scoreZona))

    return {
        "score": round(scoreZona, 2),
        "status": classificarStatusScore(scoreZona),
        "qntServidores": total,
        "qntCriticos": qntCriticos,
        "qntAtencao": qntAtencao,
        "percentualCriticos": round(percentualCriticos * 100,2),
        "percentualAtencao": round( percentualAtencao * 100,2),
        "srvPiorScore": round(srvPiorScore,2)
    }


#SCORE SAUDE DATACENTER 
def calcularScoreDatacenter(zonas):
    total = len(zonas)

    if total == 0:
        return {
            "score": 100,
            "status": "Saudável",
            "qntZonas": 0,
            "qntZonasCriticas": 0,
            "qntZonasAtencao": 0,
            "zonaPiorScore": 100
        }

    qntCriticos = 0
    qntAtencao = 0
    zonaPiorScore = 100

    for zona in zonas:
        score = converter_float(zona.get("score"), 100)
        status = zona.get("status", "Saudável")

        if status == "Crítico":
            qntCriticos += 1
        elif status == "Atenção":
            qntAtencao += 1

        if score < zonaPiorScore:
            zonaPiorScore = score

    percentualCriticos = qntCriticos / total
    percentualAtencao = qntAtencao / total

    penalidadeCriticos = percentualCriticos * 45
    penalidadeAtencao = percentualAtencao * 20

    if zonaPiorScore < 80:
        penalidadePiorZona = (80 - zonaPiorScore) * 0.30
    else:
        penalidadePiorZona = 0

    penalidadePiorZona = min(penalidadePiorZona, 20)

    if total == 1:
        scoreDatacenter = zonaPiorScore
    else:
        scoreDatacenter = (100 - penalidadeCriticos- penalidadeAtencao- penalidadePiorZona)

    scoreDatacenter = max(0, min(100, scoreDatacenter))

    return {
        "score": round(scoreDatacenter, 2),
        "status": classificarStatusScore(scoreDatacenter),
        "qntZonas": total,
        "qntZonasCriticas": qntCriticos,
        "qntZonasAtencao": qntAtencao,
        "percentualZonasCriticas": round(percentualCriticos * 100,2),
        "percentualZonasAtencao": round(percentualAtencao * 100,2),
        "zonaPiorScore": round(zonaPiorScore, 2),
        "penalidades": {
            "zonasCriticas": round(penalidadeCriticos, 2),
            "zonasAtencao": round(penalidadeAtencao, 2),
            "piorZona": round(penalidadePiorZona, 2)
        }
    }

# SCORE SAÚDE REGIÃO
def calcularScoreRegiao(datacentersRegiao):
    total = len(datacentersRegiao)

    if total == 0:
        return {
            "score": 100,
            "status": "Saudável",
            "qntDatacenters": 0,
            "qntDatacentersCriticos": 0,
            "qntDatacentersAtencao": 0,
            "percentualDatacentersCriticos": 0,
            "percentualDatacentersAtencao": 0,
            "datacenterPiorScore": 100
        }

    qntCriticos = 0
    qntAtencao = 0
    datacenterPiorScore = 100

    for datacenter in datacentersRegiao:
        score = converter_float(
            datacenter.get("score"),
            100
        )

        status = datacenter.get(
            "status",
            "Saudável"
        )

        if status == "Crítico":
            qntCriticos += 1
        elif status == "Atenção":
            qntAtencao += 1

        if score < datacenterPiorScore:
            datacenterPiorScore = score

    percentualCriticos = qntCriticos / total
    percentualAtencao = qntAtencao / total
    penalidadeCriticos = percentualCriticos * 45
    penalidadeAtencao = percentualAtencao * 20

    if datacenterPiorScore < 80:
        penalidadePiorDatacenter = (80 - datacenterPiorScore) * 0.20
    else:
        penalidadePiorDatacenter = 0

    penalidadePiorDatacenter = min(penalidadePiorDatacenter,15)

    if total == 1:
        scoreRegiao = datacenterPiorScore
    else:
        scoreRegiao = ( 100- penalidadeCriticos - penalidadeAtencao - penalidadePiorDatacenter)

    scoreRegiao = max(0,min(100, scoreRegiao))

    return {
        "score": round(scoreRegiao, 2),
        "status": classificarStatusScore(scoreRegiao),
        "qntDatacenters": total,
        "qntDatacentersCriticos": qntCriticos,
        "qntDatacentersAtencao": qntAtencao,
        "percentualDatacentersCriticos": round(percentualCriticos * 100,2),
        "percentualDatacentersAtencao": round(percentualAtencao * 100,2),
        "datacenterPiorScore": round(datacenterPiorScore,2),
        "penalidades": {
            "datacentersCriticos": round(penalidadeCriticos,2),
            "datacentersAtencao": round(penalidadeAtencao,2),
            "piorDatacenter": round(penalidadePiorDatacenter,2)
        }
    }
#------------------------------------------------------------------------------------------------------------------------------------------------
# TENDÊNCIA DE DEGRADAÇÃO

MINIMO_COLETAS_TENDENCIA = 20

# O componente precisa aparecer acima do limite
# em pelo menos 20% das coletas atuais.
MINIMA_PERSISTENCIA_ATUAL = 0.10

# A persistência precisa ter aumentado pelo menos
# 10 pontos percentuais entre as janelas.
MINIMO_AUMENTO_PERSISTENCIA = 0.05


def calcularPersistenciaComponente(janela,campo, limite):
    if not janela:
        return 0

    quantidadeValidas = 0
    quantidadeAcimaLimite = 0

    for coleta in janela:
        valor = converter_float(coleta.get(campo),padrao=None)

        if valor is None:
            continue

        quantidadeValidas += 1

        if valor > limite:
            quantidadeAcimaLimite += 1

    if quantidadeValidas == 0:
        return 0

    return (quantidadeAcimaLimite / quantidadeValidas)


def obterConfigComponentes(limites):
    return [
        {
            "nome": "CPU",
            "campo": "CPU_PER",
            "limite": converter_float(
                limites.get("CPU"),
                LIMITE_CPU
            )
        },
        {
            "nome": "RAM",
            "campo": "RAM_PER",
            "limite": converter_float(
                limites.get("RAM"),
                LIMITE_RAM
            )
        },
        {
            "nome": "Disco",
            "campo": "DISCO_PER",
            "limite": converter_float(
                limites.get("DISCO"),
                LIMITE_DISCO
            )
        },
        {
            "nome": "Latência",
            "campo": "LATENCIA",
            "limite": converter_float(
                limites.get("REDE"),
                LIMITE_LATENCIA
            )
        }
       
    ]
       
    print("LIMITES:", limites)


def calcularTendenciaComponente(janelaAnterior, janelaAtual, configuracao):
    nome = configuracao["nome"]
    campo = configuracao["campo"]
    limite = configuracao["limite"]

    persistenciaAnterior = (
        calcularPersistenciaComponente(janelaAnterior, campo,limite)
    )

    persistenciaAtual = (
        calcularPersistenciaComponente(janelaAtual, campo,limite)
    )

    aumentoPersistencia = ( persistenciaAtual - persistenciaAnterior )
    possuiTendencia = ( persistenciaAtual >= MINIMA_PERSISTENCIA_ATUAL and aumentoPersistencia >= MINIMO_AUMENTO_PERSISTENCIA)

    return {
        "componente": nome,
        "possuiTendencia": possuiTendencia,
        "persistenciaAnterior": round(persistenciaAnterior * 100,2),
        "persistenciaAtual": round( persistenciaAtual * 100, 2),
        "aumentoPersistencia": round(aumentoPersistencia * 100,2),
        "limite": round(limite, 2)
    }


def calcularTendenciaServidor(janelaAnterior,janelaAtual,limites,scoreAnterior,scoreAtual):
    print(
    f"Janela anterior: {len(janelaAnterior)} | "
    f"Janela atual: {len(janelaAtual)}")
    if ( len(janelaAnterior) < MINIMO_COLETAS_TENDENCIA or len(janelaAtual) < MINIMO_COLETAS_TENDENCIA):
        return {
            "possuiRisco": False,
            "nivelRisco": "insuficiente",
            "motivo": (
                "Sem dados suficientes para "
                "comparar os períodos."
            ),
            "componentesTendencia": []
        }

    componentesTendencia = []

    configuracoes = obterConfigComponentes(limites)

    for configuracao in configuracoes:
        resultado = calcularTendenciaComponente(janelaAnterior,janelaAtual,configuracao)

        if resultado["possuiTendencia"]:
            componentesTendencia.append(resultado)

    componentesTendencia.sort(
        key=lambda componente: (
            componente["aumentoPersistencia"],
            componente["persistenciaAtual"]
        ),
        reverse=True
    )

    variacaoScore = scoreAtual - scoreAnterior

    possuiRisco = (len(componentesTendencia) > 0)

    if not possuiRisco:
        return {
            "possuiRisco": False,
            "nivelRisco": "sem risco",
            "variacaoScore": round(
                variacaoScore,
                2
            ),
            "motivo": (
                "Sem aumento persistente de "
                "componentes críticos."
            ),
            "componentesTendencia": []
        }

    maiorAumento = componentesTendencia[
        0
    ]["aumentoPersistencia"]

    quantidadeComponentes = len(
        componentesTendencia
    )

    if (
        maiorAumento >= 30
        or quantidadeComponentes >= 2
    ):
        nivelRisco = "alto"

    elif maiorAumento >= 20:
        nivelRisco = "moderado"

    else:
        nivelRisco = "baixo"

    nomesComponentes = [
        componente["componente"]
        for componente
        in componentesTendencia[:2]
    ]

    if len(nomesComponentes) == 1:
        motivo = (
            f"{nomesComponentes[0]} apresentou "
            f"aumento de persistência acima do limite."
        )

    else:
        motivo = (
            f"{nomesComponentes[0]} e "
            f"{nomesComponentes[1]} apresentaram "
            f"aumento de persistência acima do limite."
        )

    return {
        "possuiRisco": True,
        "nivelRisco": nivelRisco,
        "variacaoScore": round(variacaoScore,2),
        "motivo": motivo,
        "componentesTendencia": (componentesTendencia)
    }


def servidorPossuiRiscoDegradacao(servidor):
    tendencia = servidor.get(
        "tendenciaDegradacao",
        {}
    )

    return tendencia.get("possuiRisco",False)

def obterMaiorAumentoPersistencia(servidor):
    tendencia = servidor.get(
        "tendenciaDegradacao",
        {}
    )

    componentes = tendencia.get(
        "componentesTendencia",
        []
    )

    if not componentes:
        return 0

    return max(
        componente.get( "aumentoPersistencia",0)
        for componente in componentes
    )
#-----------------------------------------------Arrumando o campo Zona do trusted para que eu consiga peg=a-lo------------------------------------------------------------------
import unicodedata


def normalizarTextoEstrutura(valor):
    if valor is None or pd.isna(valor):
        return ""

    texto = str(valor).strip().lower()
    texto = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("ascii")
    return (texto.replace(" ", "").replace("-", "").replace("_", "").replace("\u00a0", ""))

def buscarChaveNormalizada(dicionario, valorProcurado):
    if not isinstance(dicionario, dict):
        return None

    valorNormalizado = normalizarTextoEstrutura(valorProcurado)

    for chave in dicionario.keys():
        if (normalizarTextoEstrutura(chave)== valorNormalizado):
            return chave

    return None

def buscarServidorNaEstrutura(estruturaJson, empresa, datacenter,zona,servidor):
    if not isinstance(estruturaJson, dict):
        return None

    chaveEmpresa = buscarChaveNormalizada( estruturaJson, empresa)

    if chaveEmpresa is None:
        print(f"Empresa não encontrada: {empresa}")
        return None

    dadosEmpresa = estruturaJson[chaveEmpresa]
    chaveDatacenter = buscarChaveNormalizada(dadosEmpresa,datacenter)

    if chaveDatacenter is None:
        print(
            f"Datacenter não encontrado: "
            f"{empresa}/{datacenter}"
        )
        return None

    dadosDatacenter = dadosEmpresa[chaveDatacenter]

    chaveZona = buscarChaveNormalizada( dadosDatacenter,zona)

    if chaveZona is None:
        print(
            f"Zona não encontrada: "
            f"{empresa}/{datacenter}/{zona}"
        )
        print(
            "Zonas disponíveis:",
            list(dadosDatacenter.keys())
        )
        return None

    dadosZona = dadosDatacenter[chaveZona]
    chaveServidor = buscarChaveNormalizada(dadosZona,servidor)

    if chaveServidor is None:
        print(
            f"Servidor não encontrado: "
            f"{empresa}/{datacenter}/{zona}/{servidor}"
        )
        print("Servidores disponíveis:", list(dadosZona.keys())) 
        return None

    return dadosZona[chaveServidor]


#---------------------------------- Arrumando a duplicidade dos dados --------------------------------------------------------------------------
def construirMapaCadastroServidores(metricasJson):
    mapa = {}

    if not isinstance(metricasJson, dict):
        return mapa

    for empresa, dadosEmpresa in metricasJson.items():
        if not isinstance(dadosEmpresa, dict):
            continue

        for datacenter, dadosDatacenter in dadosEmpresa.items():
            if not isinstance(dadosDatacenter, dict):
                continue

            for zona, dadosZona in dadosDatacenter.items():
                if not isinstance(dadosZona, dict):
                    continue

                zonaPadronizada = padronizarNomeZona(zona)

                for servidor in dadosZona.keys():
                    servidorPadronizado = (padronizarNomeServidor(servidor))
                    chave = (
                        normalizarTextoEstrutura(empresa),
                        normalizarTextoEstrutura(datacenter),
                        normalizarTextoEstrutura(servidorPadronizado)
                    )

                    mapa[chave] = {
                        "empresa": empresa,
                        "datacenter": datacenter,
                        "zona": zonaPadronizada,
                        "servidor": servidorPadronizado
                    }

    return mapa

#Aplicando essas validacoes e tratamentos em cada linha
def corrigirCadastroLinha(linha,mapaCadastro):
    empresa = linha["EMPRESA"]
    datacenter = linha["DATACENTER"]
    servidor = linha["SERVIDOR"]

    chave = (
        normalizarTextoEstrutura(empresa),
        normalizarTextoEstrutura(datacenter),
        normalizarTextoEstrutura(servidor)
    )

    cadastro = mapaCadastro.get(chave)

    if cadastro is None:
        return linha

    linha["EMPRESA"] = cadastro["empresa"]
    linha["DATACENTER"] = cadastro["datacenter"]
    linha["ZONA"] = cadastro["zona"]
    linha["SERVIDOR"] = cadastro["servidor"]

    return linha
#-----------------------------------------------------------------------------------------------------------------------------------------------
#gerando o uptime de cada servidor

PERIODO_UPTIME_DIAS = 30
LIMITE_UPTIME_IDEAL = 99

def classificarStatusUptime(uptime):
    if uptime >= 99:
        return "Saudável"
    elif uptime >= 95:
        return "Atenção"
    return "Crítico"

#df que lê o json de chamados
def carregarChamadosJson(bucket):
    caminhoJson = "dados_alertas/ultimos_alertas.json"

    try:
        resposta = s3.get_object(Bucket=bucket,Key=caminhoJson)

        conteudo = resposta["Body"].read().decode("utf-8-sig")
        chamadosJson = json.loads(conteudo)

        print(
            f" ultimos_alertas.json carregado do S3: "
            f"{caminhoJson}"
        )
        return chamadosJson
    except Exception as erro:
        print(
            f" Erro ao carregar ultimos_alertas.json: "
            f"{erro}"
        )

        return {}
    
#df que pega os chamadso de cada srv individualmente
def obterChamadosServidor(chamadosJson,empresa,datacenter,zona,servidor):
    
    dadosServidor = buscarServidorNaEstrutura(
    chamadosJson,
    empresa,
    datacenter,
    zona,
    servidor
    )

    if dadosServidor is None:
        print(
            f"Chamados não encontrados para: "
            f"{empresa}/{datacenter}/{zona}/{servidor}"
        )
        return []

    chamados = dadosServidor.get("chamados",{})

    if isinstance(chamados, dict):
        return list(chamados.values())

    if isinstance(chamados, list):
        return chamados


#df filtrando apenas os chamados criticos que afetam a disponibilidade do srv

import unicodedata

def normalizarTexto(valor):
    if valor is None:
        return ""

    texto = str(valor).strip().lower()
    texto = str(valor).strip().lower()
    texto = unicodedata.normalize("NFKD",texto).encode("ascii","ignore").decode("ascii")

    return (
        texto
        .replace(" ", "")
        .replace("-", "")
        .replace("_", "")
    )


def chamadoContaComoIndisponibilidade(chamado):
    if not isinstance(chamado, dict):
        return False

    severidade = normalizarTexto(
        chamado.get("severidade")
    )

    return severidade == "critico"


# def que faz a conversao da data e hora
def converterData(valor):
    if valor is None:
        return None

    if isinstance(valor, datetime):
        return valor.replace(tzinfo=None)

    if isinstance(valor, pd.Timestamp):
        return valor.to_pydatetime().replace(
            tzinfo=None
        )

    if not isinstance(valor, str):
        return None

    valor = valor.strip()

    if valor == "":
        return None

    if valor.lower() in [
        "none",
        "null",
        "em_aberto"
    ]:
        return None

    try:
        data = datetime.fromisoformat(valor)

        if data.tzinfo is not None:
            data = data.replace(tzinfo=None)

        return data

    except ValueError:
        pass

    formatosAlternativos = [
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y %H:%M:%S"
    ]

    for formato in formatosAlternativos:
        try:
            return datetime.strptime(valor,formato)

        except ValueError:
            continue

    print(f" Data inválida ignorada: {valor}")
    return None

#def que junta o intervalo entre cada chamada aberto
def unificarIntervalosIndisponibilidade(intervalos):
    if not intervalos:
        return []

    intervalosOrdenados = sorted(
        intervalos,
        key=lambda intervalo: intervalo[0]
    )

    intervalosUnificados = [
        [
            intervalosOrdenados[0][0],
            intervalosOrdenados[0][1]
        ]
    ]

    for inicioAtual, fimAtual in intervalosOrdenados[1:]:
        ultimoIntervalo = intervalosUnificados[-1]

        ultimoFim = ultimoIntervalo[1]

        if inicioAtual <= ultimoFim:
            ultimoIntervalo[1] = max(
                ultimoFim,
                fimAtual
            )

        else:
            intervalosUnificados.append(
                [inicioAtual, fimAtual]
            )

    return [
        (inicio, fim)
        for inicio, fim in intervalosUnificados
    ]

#def que de acordo com os seguntos em que o chamado ficou aberto( rodando a cada 5 minuots para a atualização) considera isso como indisponibilidade e faz o calculo do uptime do servidor
def calcularUptimeServidor(chamadosServidor, servidor, zona, inicioPeriodo, fimPeriodo):
    tempoTotalSegundos = (
        fimPeriodo - inicioPeriodo
    ).total_seconds()

    if tempoTotalSegundos <= 0:
        return {
            "servidor": servidor,
            "zona": zona,
            "periodoInicio": inicioPeriodo.isoformat(),
            "periodoFim": fimPeriodo.isoformat(),
            "periodoDias": 0,
            "uptime": 100,
            "tempoDisponivelSegundos": 0,
            "tempoIndisponivelSegundos": 0,
            "tempoIndisponivelMinutos": 0,
            "tempoIndisponivelHoras": 0,
            "qtdChamadosCriticos": 0,
            "qtdPeriodosIndisponibilidade": 0,
            "qtdChamadosInvalidos": 0,
            "statusUptime": "Saudável",
            "possuiChamadosNoPeriodo": False,
            "descricao": "Período de análise inválido."
        }

    intervalosIndisponibilidade = []

    qtdChamadosCriticos = 0
    qtdChamadosInvalidos = 0

    for chamado in chamadosServidor:
        if not isinstance(chamado, dict):
            qtdChamadosInvalidos += 1
            continue

        if not chamadoContaComoIndisponibilidade(
            chamado
        ):
            continue

        abertura = converterData(
            chamado.get("abertura")
            or chamado.get("aberto_em")
            or chamado.get(
                "inicioIndisponibilidade"
            )
        )

        fechamento = converterData(
            chamado.get("fechamento")
            or chamado.get("resolvido_em")
            or chamado.get(
                "fimIndisponibilidade"
            )
        )

        if abertura is None:
            print(
                f" Chamado crítico sem abertura válida "
                f"no servidor {servidor}: "
                f"{chamado.get('id_alerta')}"
            )

            qtdChamadosInvalidos += 1
            continue

        if fechamento is None:
            fechamento = fimPeriodo

        if fechamento < abertura:
            print(
                f"Chamado com fechamento anterior à "
                f"abertura no servidor {servidor}: "
                f"{chamado.get('id_alerta')}"
            )

            qtdChamadosInvalidos += 1
            continue

        inicioConsiderado = max(abertura,inicioPeriodo)
        fimConsiderado = min(fechamento,fimPeriodo)

        if fimConsiderado <= inicioConsiderado:
            continue

        intervalosIndisponibilidade.append((inicioConsiderado,fimConsiderado))

        qtdChamadosCriticos += 1

    intervalosUnificados = (unificarIntervalosIndisponibilidade(intervalosIndisponibilidade))

    tempoIndisponivelSegundos = sum(
        (
            fimIntervalo - inicioIntervalo
        ).total_seconds()
        for inicioIntervalo, fimIntervalo
        in intervalosUnificados
)

    tempoIndisponivelSegundos = max(0,min(tempoIndisponivelSegundos,tempoTotalSegundos))

    tempoDisponivelSegundos = (tempoTotalSegundos- tempoIndisponivelSegundos)
    uptime = (tempoDisponivelSegundos/ tempoTotalSegundos) * 100
    uptime = max(0,min(100, uptime))
    tempoIndisponivelMinutos = (tempoIndisponivelSegundos / 60)
    tempoIndisponivelHoras = (tempoIndisponivelSegundos / 3600)

    possuiChamadosNoPeriodo = (qtdChamadosCriticos > 0)

    if possuiChamadosNoPeriodo:
        descricao = (
            f"{round(tempoIndisponivelHoras, 2)} horas "
            f"de indisponibilidade registradas nos últimos "
            f"{PERIODO_UPTIME_DIAS} dias."
        )

    else:
        descricao = (
            f"Nenhuma indisponibilidade crítica registrada "
            f"nos últimos {PERIODO_UPTIME_DIAS} dias."
        )

    return {
        "servidor": servidor,
        "zona": zona,
        "periodoInicio": inicioPeriodo.isoformat(),
        "periodoFim": fimPeriodo.isoformat(),
        "periodoDias": PERIODO_UPTIME_DIAS,
        "uptime": round(uptime, 4),
        "tempoDisponivelSegundos": round(tempoDisponivelSegundos,2),
        "tempoIndisponivelSegundos": round(tempoIndisponivelSegundos,2),
        "tempoIndisponivelMinutos": round(tempoIndisponivelMinutos,2),
        "tempoIndisponivelHoras": round(tempoIndisponivelHoras,2),
        "qtdChamadosCriticos": qtdChamadosCriticos,
        "qtdPeriodosIndisponibilidade": len(intervalosUnificados),
        "qtdChamadosInvalidos": qtdChamadosInvalidos,
        "statusUptime": classificarStatusUptime(uptime),
        "possuiChamadosNoPeriodo": possuiChamadosNoPeriodo,
        "descricao": descricao
    }

#------------------------------------------------------------------ Grafico de alertaas -----------------------------------------------------------------------
FUSO_HORARIO = ZoneInfo("America/Sao_Paulo")

DIAS_SEMANA = [
    "Segunda",
    "Terça",
    "Quarta",
    "Quinta",
    "Sexta",
    "Sábado",
    "Domingo"
]


def obterAgoraSaoPaulo():
    return datetime.now(FUSO_HORARIO).replace(tzinfo=None)


INTERVALO_COMPARACAO_MINUTOS = 30

def gerarHistoricoAlertasComponentes(dataframe,metricasJson):
    if dataframe is None or dataframe.empty:
        print("DataFrame de alertas vazio.")
        return []

    dfAlertas = dataframe.copy()
    dfAlertas["DATE"] = pd.to_datetime(dfAlertas["DATE"],errors="coerce")

    dfAlertas = dfAlertas.dropna(
        subset=[
            "DATE",
            "EMPRESA",
            "DATACENTER",
            "ZONA",
            "SERVIDOR"
        ]
    )

    dfAlertas = dfAlertas.drop_duplicates(
        subset=[
            "EMPRESA",
            "DATACENTER",
            "ZONA",
            "SERVIDOR",
            "DATE"
        ],
        keep="last"
    )

    historicoAlertas = []
    estadoAnterior = {}
    totalColetasAnalisadas = 0
    totalColetasComAlerta = 0
    totalSemLimites = 0

    contagemExcedentes = {
    "CPU": 0,
    "RAM": 0,
    "Disco": 0,
    "Latência": 0
    }

    maioresValores = {
        "CPU": None,
        "RAM": None,
        "Disco": None,
        "Latência": None
    }

    limitesEncontrados = {
        "CPU": set(),
        "RAM": set(),
        "Disco": set(),
        "Latência": set()
    }

    dfAlertas = dfAlertas.sort_values(["EMPRESA", "DATACENTER", "ZONA", "SERVIDOR", "DATE"])

    for coleta in dfAlertas.to_dict(orient="records"):

        empresa = coleta.get("EMPRESA")
        datacenter = coleta.get("DATACENTER")
        zona = coleta.get("ZONA")
        servidor = coleta.get("SERVIDOR")
        dataHora = coleta.get("DATE")

        if(empresa is None or datacenter is None or zona is None or servidor is None or pd.isna(dataHora)):
            continue

        totalColetasAnalisadas += 1

        infoServidor = buscarServidorNaEstrutura(
            metricasJson,
            empresa,
            datacenter,
            zona,
            servidor
        )

        if infoServidor is None:
            totalSemLimites += 1
            print(
                f"Limites não encontrados para alerta: "
                f"{empresa}/{datacenter}/{zona}/{servidor}"
            )
            continue

        limites = infoServidor.get(
            "limites",
            {}
        )

        componentes = obterConfigComponentes(limites)
        componentesAcimaLimite = []

        for componente in componentes:
            nomeComponente = componente["nome"]
            campo = componente["campo"]
            limite = componente["limite"]

            valor = converter_float(
                coleta.get(campo),
                None
            )

            if valor is None:
                continue

            limitesEncontrados[nomeComponente].add(
                round(limite, 2)
            )

            if (
                maioresValores[nomeComponente] is None
                or valor > maioresValores[nomeComponente]
            ):
                maioresValores[nomeComponente] = valor

            if valor > limite:
                contagemExcedentes[nomeComponente] += 1

                componentesAcimaLimite.append({
                    "componente": nomeComponente,
                    "valor": round(valor, 2),
                    "limite": round(limite, 2),
                    "excessoPercentual": round(
                        ((valor - limite) / limite) * 100,
                        2
                    )})
                
        
        possuiAlertaAtual = (len(componentesAcimaLimite) > 0)

        chaveServidor = (
            empresa,
            datacenter,
            zona,
            servidor
        )

        alertaAnterior = estadoAnterior.get(
            chaveServidor,
            False
        )

        if possuiAlertaAtual and not alertaAnterior:

            totalColetasComAlerta += 1

            historicoAlertas.append({
                "empresa": str(empresa).strip(),
                "datacenter": str(datacenter).strip(),
                "zona": str(zona).strip(),
                "servidor": str(servidor).strip(),
                "timestamp": dataHora.isoformat(),
                "qtdComponentesAcimaLimite": len(
                    componentesAcimaLimite
                ),
                "componentesAcimaLimite": (
                    componentesAcimaLimite
                )
            })

        estadoAnterior[chaveServidor] = (possuiAlertaAtual)
    print("Coletas analisadas para alertas:",totalColetasAnalisadas)

    print("Coletas com pelo menos um alerta:",totalColetasComAlerta)
    print("Coletas sem limites encontrados:",totalSemLimites)
    print("Total final no histórico:",len(historicoAlertas))


    print(
        "Excedentes por componente:",
        contagemExcedentes
    )

    print(
        "Maiores valores encontrados:",
        maioresValores
    )

    print(
        "Limites utilizados:",
        {
            componente: sorted(list(valores))
            for componente, valores
            in limitesEncontrados.items()
        }
    )

    
    if historicoAlertas:
        print("Exemplo de alerta criado:",historicoAlertas[0])

    return historicoAlertas


def calcularAlertaSemana(
    historicoAlertas,
    empresa,
    datacenter
):
    agora = obterAgoraSaoPaulo()

    inicioSemana = (
        agora
        - timedelta(days=agora.weekday())
    ).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0
    )

    fimSemana = inicioSemana + timedelta(
        days=7
    )

    alertasPorDia = {
        dia: 0
        for dia in DIAS_SEMANA
    }

    empresaComparacao = normalizarTextoEstrutura(
        empresa
    )

    datacenterComparacao = normalizarTextoEstrutura(
        datacenter
    )

    alertasEncontradosDatacenter = 0
    alertasEncontradosSemana = 0

    for alerta in historicoAlertas:
        empresaAlerta = normalizarTextoEstrutura(
            alerta.get("empresa")
        )

        datacenterAlerta = normalizarTextoEstrutura(
            alerta.get("datacenter")
        )

        if empresaAlerta != empresaComparacao:
            continue

        if (
            datacenterAlerta
            != datacenterComparacao
        ):
            continue

        alertasEncontradosDatacenter += 1

        dataAlerta = converterData(
            alerta.get("timestamp")
        )

        if dataAlerta is None:
            continue

        if not (
            inicioSemana
            <= dataAlerta
            < fimSemana
        ):
            continue

        alertasEncontradosSemana += 1

        nomeDia = DIAS_SEMANA[
            dataAlerta.weekday()
        ]

        alertasPorDia[nomeDia] += 1

    totalAlertasSemana = sum(alertasPorDia.values())

    diasDecorridosSemana = (agora.weekday() + 1)

    mediaDiariaSemana = (totalAlertasSemana/ diasDecorridosSemana
        if diasDecorridosSemana > 0
        else 0
    )

    print(
        f"Alertas encontrados para {datacenter}:", alertasEncontradosDatacenter
    )

    print(
        f"Alertas da semana para {datacenter}:",alertasEncontradosSemana
    )

    print(
        f"Alertas por dia de {datacenter}:", alertasPorDia
    )

    return {
        "periodoInicio": (inicioSemana.isoformat()),
        "periodoFim": fimSemana.isoformat(),
        "alertasPorDia": alertasPorDia,
        "totalAlertasSemana": (totalAlertasSemana),
        "diasConsiderados": (diasDecorridosSemana),
        "mediaDiariaAlertas": round(mediaDiariaSemana,2)
    }

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
def calcularCrescimentoAlertas(historicoAlertas,empresa, datacenter,intervaloMinutos = INTERVALO_COMPARACAO_MINUTOS):
    agora = obterAgoraSaoPaulo()

    inicioIntervaloAtual = (agora- timedelta(minutes=intervaloMinutos) )
    fimIntervaloAnterior = (inicioIntervaloAtual)
    inicioIntervaloAnterior = (fimIntervaloAnterior - timedelta(minutes=intervaloMinutos))
    alertasIntervaloAtual = 0
    alertasIntervaloAnterior = 0

    for alerta in historicoAlertas:
        if (str(alerta.get("empresa", "")) != str(empresa)or str(alerta.get("datacenter", "")) != str(datacenter)):
            continue

        dataAlerta = converterData(alerta.get("timestamp"))

        if dataAlerta is None:
            continue

        if inicioIntervaloAtual <= dataAlerta <= agora:
            alertasIntervaloAtual += 1

        elif (
            inicioIntervaloAnterior <= dataAlerta<= fimIntervaloAnterior):
            alertasIntervaloAnterior += 1

    if alertasIntervaloAnterior == 0:
        if alertasIntervaloAtual > 0:
            percentual = None
            valorFormatado = "Novo"
            tendencia = "aumento"
        else:
            percentual = 0
            valorFormatado = "0%"
            tendencia = "estável"
    else:
        percentualCalculado = (( alertasIntervaloAtual- alertasIntervaloAnterior)/ alertasIntervaloAnterior) * 100
        percentual = round(percentualCalculado,2)

        if percentual > 0:
            valorFormatado = (
                f"+{percentual}%"
            )
            tendencia = "aumento"

        elif percentual < 0:
            valorFormatado = (
                f"{percentual}%"
            )
            tendencia = "queda"

        else:
            valorFormatado = "0%"
            tendencia = "estável"

    return {
        "percentual": percentual,
        "valorFormatado": valorFormatado,
        "alertasIntervaloAtual": alertasIntervaloAtual,
        "alertasIntervaloAnterior": alertasIntervaloAnterior,
        "intervaloMinutos": intervaloMinutos,
        "tendencia": tendencia,
         "descricao": (
            f"De {alertasIntervaloAnterior} alertas "
            f"nos {intervaloMinutos} minutos anteriores "
            f"para {alertasIntervaloAtual} alertas "
            f"nos últimos {intervaloMinutos} minutos."
        ),
        "periodoAtual": {
            "inicio": inicioIntervaloAtual.isoformat(),
            "fim": agora.isoformat()
        },
        "periodoAnteriorComparado": {
            "inicio": inicioIntervaloAnterior.isoformat(),
            "fim": fimIntervaloAnterior.isoformat()
        }
    }


#kpi do uptime
def calcularKpiUptime(uptimeServidores, limiteUptimeIdeal=LIMITE_UPTIME_IDEAL):
    totalServidores = len(uptimeServidores)

    if totalServidores == 0:
        return {
            "servidoresAbaixoIdeal": 0,
            "totalServidores": 0,
            "percentualAbaixoIdeal": 0,
            "limiteIdeal": limiteUptimeIdeal,
            "periodoDias": PERIODO_UPTIME_DIAS,
            "valorFormatado": "0/0",
            "descricao": "Nenhum servidor encontrado."
        }

    servidoresAbaixoIdeal = 0
    servidoresSemIndisponibilidadeRegistrada = 0

    for servidor in uptimeServidores:
        uptime = converter_float(
            servidor.get("uptime"),
            100
        )

        if uptime < limiteUptimeIdeal:
            servidoresAbaixoIdeal += 1

        if not servidor.get(
            "possuiChamadosNoPeriodo",
            False
        ):
            servidoresSemIndisponibilidadeRegistrada += 1

    percentualAbaixoIdeal = (servidoresAbaixoIdeal/ totalServidores) * 100

    return {
        "servidoresAbaixoIdeal": servidoresAbaixoIdeal,
        "totalServidores": totalServidores,
        "percentualAbaixoIdeal": round(percentualAbaixoIdeal,2),
        "servidoresSemIndisponibilidadeRegistrada": (servidoresSemIndisponibilidadeRegistrada),
        "limiteIdeal": limiteUptimeIdeal,
        "periodoDias": PERIODO_UPTIME_DIAS,
        "valorFormatado": (
            f"{servidoresAbaixoIdeal}/"
            f"{totalServidores}"
        ),
        "descricao": (
            f"{servidoresAbaixoIdeal} de "
            f"{totalServidores} servidores ficaram "
            f"abaixo de {limiteUptimeIdeal}% de uptime "
            f"nos últimos {PERIODO_UPTIME_DIAS} dias."
        )
    }

#---------------------------------------------------------------------------------------------------------------------------------
def dashOperacional( dfScore,dfAlertas, geral, bucket):
    print("\n ENTREI NA DASH OPERACIONAL")

    df = dfScore.copy()

    if df.empty:
        print(" DataFrame vazio")
        return {
            "tipo": "gestora",
            "total_dados": 0,
            "empresas": {}
        }

    df["DATE"] = pd.to_datetime(df["DATE"],format="mixed",errors="coerce")
    df = df.dropna(subset=["DATE"])

    historicoAlertas = gerarHistoricoAlertasComponentes(dfAlertas,geral)
    fimPeriodoUptime = datetime.now()
    inicioPeriodoUptime =   (fimPeriodoUptime- timedelta(days=PERIODO_UPTIME_DIAS))

    print("Total de alertas no histórico:", len(historicoAlertas))

    if historicoAlertas:
        print("Primeiro alerta encontrado:", historicoAlertas[0])

    chamadosJson = carregarChamadosJson(bucket)

    resultado = {}

    for empresa, df_empresa in df.groupby("EMPRESA"):

        print(f"\nEMPRESA: {empresa}")

        resultado.setdefault(empresa, {
            "regioes": {},
            "datacenters": {}
        })

        for regiao, df_regiao in df_empresa.groupby("REGIAO"):

            print(f"\n REGIÃO: {regiao}")

            datacentersRegiao = []

            for datacenter, df_dc in df_regiao.groupby("DATACENTER"):

                print(f"\n DATACENTER: {datacenter}")

                graficoAlertasSemana = calcularAlertaSemana(
                    historicoAlertas,
                    empresa,
                    datacenter
                )

                zonas = []
                servidores_datacenter = []
                uptimeServidores = []

                for zona, df_zona in df_dc.groupby("ZONA"):

                    print(f"\n ZONA: {zona}")

                    servidoresZona = []

                    for servidor, df_servidor in df_zona.groupby("SERVIDOR"):

                        print(f"\n SERVIDOR: {servidor}")

                        info_servidor = buscarServidorNaEstrutura(
                            geral,
                            empresa,
                            datacenter,
                            zona,
                            servidor
                        )

                        if info_servidor is None:
                            limites = {}

                            print(
                                f"Limites não encontrados para "
                                f"{empresa}/{datacenter}/{zona}/{servidor}. "
                                f"Usando fallback."
                            )

                        else:
                            limites = info_servidor.get(
                                "limites",
                                {}
                            )

                            print(
                                f"Limites encontrados para "
                                f"{empresa}/{datacenter}/{zona}/{servidor}:",
                                limites
                            )

                            

                        
                        chamadosServidor = obterChamadosServidor(
                            chamadosJson,
                            empresa,
                            datacenter,
                            zona,
                            servidor
                        )

                        resultadoUptime = calcularUptimeServidor(chamadosServidor, servidor,zona,inicioPeriodoUptime,fimPeriodoUptime)

                        uptimeServidores.append(resultadoUptime)

                        df_servidor = df_servidor.sort_values("DATE")
                        coletaServidor = df_servidor.to_dict(orient="records")

                        print(f" Quantidade de coletas: {len(coletaServidor)}")

                        resultadoScore = calcularScoreServidor(
                            coletaServidor,
                            limites
                        )

                        print(" SCORE SERVIDOR:", resultadoScore)

                        servidor_obj = {
                            "servidor": servidor,
                            "zona": zona,
                            "score": resultadoScore["score"],
                            "status": resultadoScore["status"],
                            "scoreParcialAtual": resultadoScore["scoreParcialAtual"],
                            "scoreParcialAnterior": resultadoScore["scoreParcialAnterior"],
                            "variacaoScore": resultadoScore[ "variacaoScore"],
                            "tendenciaDegradacao": resultadoScore["tendenciaDegradacao"],
                            "uptimeOperacional": resultadoUptime
                        }

                        servidoresZona.append(servidor_obj)
                        servidores_datacenter.append(servidor_obj)

                    resultadoZona = calcularScoreZona(servidoresZona)

                    print(" SCORE ZONA:", resultadoZona)

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

                    print(f" JSON da zona {zona} criado")

                uptimeServidores = sorted(
                uptimeServidores,
                key=lambda servidor: (
                    converter_float(servidor.get("uptime"),100),servidor.get("servidor", "")))
                
                resultadoDatacenter = calcularScoreDatacenter(zonas)

                print("SCORE DATACENTER:", resultadoDatacenter)

                rankingSrv = sorted(
                    servidores_datacenter,
                    key=lambda servidor: servidor["score"]
                )

                servidoresComRisco = [
                        servidor
                        for servidor in servidores_datacenter
                        if servidorPossuiRiscoDegradacao(
                            servidor
                        )
                    ]

                rankingTendenciaServidores = sorted(
                    servidoresComRisco,
                    key=lambda servidor: (-obterMaiorAumentoPersistencia( servidor), servidor.get("score", 100)
                    )
                )
                kpiCrescimentoAlertas = calcularCrescimentoAlertas(
                    historicoAlertas,
                    empresa,
                    datacenter
                )

                kpiServidoresCriticos = calcularKpiServidoresCriticos(
                    servidores_datacenter
                )

                kpiUptime = calcularKpiUptime(uptimeServidores)

                datacenter_obj_completo = {
                    "regiao": regiao,
                    "score": resultadoDatacenter["score"],
                    "status": resultadoDatacenter["status"],
                    "qntZonas": resultadoDatacenter["qntZonas"],
                    "qntZonasCriticas": resultadoDatacenter["qntZonasCriticas"],
                    "qntZonasAtencao": resultadoDatacenter["qntZonasAtencao"],
                    "zonaPiorScore": resultadoDatacenter["zonaPiorScore"],
                    "zonas": zonas,
                    "rankingSrv": rankingSrv,
                    "rankingTendenciaServidores": rankingTendenciaServidores,
                    "graficoAlertasSemana": graficoAlertasSemana,
                    "uptimeServidores": uptimeServidores,
                    "kpiUptime": kpiUptime,
                    "kpiCrescimentoAlertas": kpiCrescimentoAlertas,
                    "kpiServidoresCriticos": kpiServidoresCriticos
                }

                resultado[empresa]["datacenters"][datacenter] = datacenter_obj_completo

                datacenter_obj_regiao = {
                    "datacenter": datacenter,
                    "score": resultadoDatacenter["score"],
                    "status": resultadoDatacenter["status"],
                    "qntZonas": resultadoDatacenter["qntZonas"],
                    "qntZonasCriticas": resultadoDatacenter["qntZonasCriticas"],
                    "qntZonasAtencao": resultadoDatacenter["qntZonasAtencao"],
                    "zonaPiorScore": resultadoDatacenter["zonaPiorScore"]
                }

                datacentersRegiao.append(datacenter_obj_regiao)
               

                print(
                    f"Gráfico de alertas de {datacenter}:",
                    graficoAlertasSemana
                )
                print(f"JSON FINAL DO DATACENTER {datacenter} CRIADO")

            resultadoRegiao = calcularScoreRegiao(datacentersRegiao)

            print(" SCORE REGIÃO:", resultadoRegiao)

            resultado[empresa]["regioes"][regiao] = {
                "score": resultadoRegiao["score"],
                "status": resultadoRegiao["status"],
                "qntDatacenters": resultadoRegiao["qntDatacenters"],
                "qntDatacentersCriticos": resultadoRegiao["qntDatacentersCriticos"],
                "qntDatacentersAtencao": resultadoRegiao["qntDatacentersAtencao"],
                "datacenterPiorScore": resultadoRegiao["datacenterPiorScore"],
                "datacenters": datacentersRegiao
            }

            print(f" JSON FINAL DA REGIÃO {regiao} CRIADO")

    print("\n DASH OPERACIONAL FINALIZADA")

    return {
        "tipo": "gestora",
        "total_dados_score": len(dfScore),
        "total_dados_alertas": len(dfAlertas),
        "empresas": resultado
    }   