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
    
def ClientGeral(bucket, chave):
    print(f"Lendo arquivo Trusted no S3: {chave}")
    
    resposta = s3.get_object(Bucket=bucket, Key=chave)
    conteudo_texto = resposta["Body"].read().decode("utf-8")

    geral = carregarMetricasJson(bucket)

    df = pd.read_csv(
    io.StringIO(conteudo_texto),
    delimiter=";"
    )

    df["DATE"] = pd.to_datetime(df["DATE"],format="mixed",errors="coerce")

    df = df.dropna(subset=["DATE"])
    df = df.drop_duplicates(
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

    agora = obterAgoraSaoPaulo()

    inicioSemanaAtual = (agora - timedelta(days=agora.weekday())).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0
    )

    inicioSemanaAnterior = (inicioSemanaAtual - timedelta(days=7))

    dfAlertas = df[(df["DATE"] >= inicioSemanaAnterior)& (df["DATE"] <= agora)].copy()

   
    colunasAgrupamento = [
        "EMPRESA",
        "REGIAO",
        "DATACENTER",
        "ZONA",
        "SERVIDOR"
    ]

    dfScore = (
        df.sort_values("DATE").groupby(colunasAgrupamento,group_keys=False).tail(60).copy())

    print(
        f"Total recebido do trusted: {len(df)}"
    )

    print(
        f"Linhas usadas nos alertas: {len(dfAlertas)}"
    )

    print(
        f"Linhas usadas no score: {len(dfScore)}"
    )

    respGestoraOp = dashOperacional(dfScore,dfAlertas,geral,bucket)
    s3.put_object(
        Bucket=bucket,
        Key="client/dashOpGestao.json",
        Body=json.dumps(respGestoraOp, default=str, indent=4),
        ContentType="application/json"
    )

    print("Todas as paginas processadas e atualizadas. 🟩")
    return "Lambda concluida com sucesso! ✅"

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

    return max(0, min(100, score_parcial))

#SCORE DE SAUDE SERVIDOR
def calcularScoreServidor(coletaServidor, limites):
    if not coletaServidor:
        return {
            "score": 100,
            "status": "Saudável",
            "scoreParcialAtual": 100,
            "scoreParcialAnterior": 100,
            "queda": 0,
            "projecaoSaude": {
                "scoreAtual": 100,
                "scoreProjetado": 100,
                "scoreParcialProjetado": 100,
                "degradacaoProjetada": 0,
                "risco": "Saudável",
                "horizonteColetas": 0,
                "horizonteMinutos": 0,
                "confiabilidade": "insuficiente",
                "motivo": "Sem dados suficientes para projeção",
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

    scoreParcialAtual = calcularScoreParcial(
        janelaAtual,
        limites
    )

    if janelaAnterior:
        scoreParcialAnterior = calcularScoreParcial(janelaAnterior,limites)
    else:
        scoreParcialAnterior = scoreParcialAtual

    queda = scoreParcialAnterior - scoreParcialAtual
    scoreFinal = max(0,min(100, scoreParcialAtual))

    resultadoProjecao = construirJanelaProjetada(janelaAnterior,janelaAtual,limites,horizonteColetas=QUANTIDADE_COLETAS_PROJETADAS)
    janelaProjetada = resultadoProjecao["janelaProjetada"]
    componentesTendencia = resultadoProjecao["componentesTendencia"]

    if componentesTendencia and len(janelaProjetada) >= 20:
        scoreParcialProjetado = calcularScoreParcial(janelaProjetada,limites)

  
        scoreProjetado = min(scoreFinal,scoreParcialProjetado)

    else:
        scoreParcialProjetado = scoreParcialAtual
        scoreProjetado = scoreFinal

    scoreProjetado = max(0,min(100, scoreProjetado))

    degradacaoProjetada = max(0,scoreFinal - scoreProjetado)
    motivoProjecao = gerarMotivoProjecao(componentesTendencia)

    return {
        "score": round(scoreFinal, 2),
        "status": classificarStatusScore(scoreFinal),
        "scoreParcialAtual": round(scoreParcialAtual, 2),
        "scoreParcialAnterior": round(scoreParcialAnterior, 2),
        "queda": round(queda, 2),
        "projecaoSaude": {
            "scoreAtual": round(scoreFinal, 2),
            "scoreProjetado": round(scoreProjetado, 2),
            "scoreParcialProjetado": round(scoreParcialProjetado,2),
            "degradacaoProjetada": round(degradacaoProjetada,2),
            "risco": classificarStatusScore(scoreProjetado),
            "horizonteColetas": resultadoProjecao[
                "horizonteColetas"
            ],
            "horizonteMinutos": resultadoProjecao[
                "horizonteMinutos"
            ],
            "confiabilidade": resultadoProjecao[
                "confiabilidade"
            ],
            "motivo": motivoProjecao,
            "componentesTendencia": componentesTendencia
        }
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
#Projecao de score e tendencia de componentesque vão aumentar criticamente

QUANTIDADE_COLETAS_PROJETADAS = 10
ALPHA_EWMA = 0.30 #
MINIMO_COLETAS_TENDENCIA = 20
MINIMO_CONCORDANCIA_TENDENCIA = 0.65
MINIMA_EVOLUCAO_RECENTE = 0.03
MINIMA_MUDANCA_PROJETADA = 0.05


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


# def que armazena os valores do componente e os seus limites para faciltar os calculos que os usam
def obterConfigComponentes(limites):
    return [
        {
            "nome": "CPU",
            "campo": "CPU_PER",
            "limite": converter_float(limites.get("CPU"),LIMITE_CPU),
            "minimo": 0,
            "maximo": 100
        },
        {
            "nome": "RAM",
            "campo": "RAM_PER",
            "limite": converter_float(limites.get("RAM"),LIMITE_RAM),
            "minimo": 0,
            "maximo": 100
        },
        {
            "nome": "Disco",
            "campo": "DISCO_PER",
            "limite": converter_float(limites.get("DISCO"),LIMITE_DISCO),
            "minimo": 0,
            "maximo": 100
        },
        {
            "nome": "Latência",
            "campo": "LATENCIA",
            "limite": converter_float(limites.get("REDE"),LIMITE_LATENCIA),
            "minimo": 0,
            "maximo": None
        }
    ]

# def que permite que os compoenntes sejam analisados na mesma métrica, normalizando eles dividindo o valor coletado pelo limite do componente
def extrairSerieComponente(janela, campo, limite):
    if not janela or limite <= 0:
        return [], [], []

    valoresReais = []
    valoresNormalizados = []
    datas = []

    for coleta in janela:
        valor = converter_float(coleta.get(campo),padrao=None)
        data = coleta.get("DATE")

        if valor is None or data is None:
            continue

        try:
            dataConvertida = pd.to_datetime(
                data,
                errors="raise"
            )

            if isinstance(dataConvertida, pd.Timestamp):
                dataConvertida = dataConvertida.to_pydatetime()

            if dataConvertida.tzinfo is not None:
                dataConvertida = dataConvertida.replace(tzinfo=None)

        except Exception:
            continue

        valoresReais.append(valor)
        valoresNormalizados.append(valor / limite)
        datas.append(dataConvertida)

    if len(datas) < 2:
        return [], [], []

    primeiraData = datas[0]

    temposMinutos = [
        (data - primeiraData).total_seconds() / 60
        for data in datas
    ]

    return (valoresReais,valoresNormalizados,temposMinutos)

# def que coloca o peso de 0.30 na criação da coleta projetada para que ela suavize o valor atual para que não reduza de forma significativa o score no final, por ser um valor projetado em uma pequena janela
def suavizarSerieEwma(valores, alpha=ALPHA_EWMA):
    if not valores:
        return []

    valoresSuavizados = [valores[0]]

    for valor in valores[1:]:
        valorAnterior = valoresSuavizados[-1]

        valorSuavizado = (alpha * valor+ (1 - alpha) * valorAnterior)
        valoresSuavizados.append(valorSuavizado)

    return valoresSuavizados

# def que calculo em pares a medina do crescimento dos valores, para que caso aja um pico absurdo de uso ele não afete grandemente o score projetado, mas sim algo mais realista
def calcularInclinacaoTheilSen(valores, tempos):
    if len(valores) < 2:
        return 0

    if len(valores) != len(tempos):
        return 0

    inclinacoes = []

    for i in range(len(valores)):
        for j in range(i + 1, len(valores)):
            diferencaTempo = tempos[j] - tempos[i]

            if diferencaTempo <= 0:
                continue

            inclinacao = (valores[j] - valores[i]) / diferencaTempo
            inclinacoes.append(inclinacao)

    if not inclinacoes:
        return 0

    return median(inclinacoes)

# def que calcula o intervalo mediano entre uma coleta e outra, porque dados como os da API da steam no script de captura podem fazer com que as linhas tenham intervalos diferentes de coletas, o que resulta em linhas coletadas com intervalos diferentes
def calcularIntervaloMedianoMinutos(tempos):
    if len(tempos) < 2:
        return 0

    intervalos = []

    for indice in range(1, len(tempos)):
        intervalo = tempos[indice] - tempos[indice - 1]

        if intervalo > 0:
            intervalos.append(intervalo)

    if not intervalos:
        return 0

    return median(intervalos)

#def que verifica a proporção de pares de pontos cuja inclinação é positiva
def calcularConcordanciaTendencia(valores,tempos,tolerancia=0.0001):
    if len(valores) < 2:
        return 0

    inclinacoesPositivas = 0
    inclinacoesValidas = 0

    for i in range(len(valores)):
        for j in range(i + 1, len(valores)):
            diferencaTempo = tempos[j] - tempos[i]

            if diferencaTempo <= 0:
                continue

            inclinacao = (valores[j] - valores[i]) / diferencaTempo

            if abs(inclinacao) <= tolerancia:
                continue

            inclinacoesValidas += 1

            if inclinacao > 0:
                inclinacoesPositivas += 1

    if inclinacoesValidas == 0:
        return 0

    return (inclinacoesPositivas/ inclinacoesValidas)

# def que retorna a comparacao entre  a mediana nos valores do inicio e as do fim das 30 coletas, para verificar sem ser afetada por  outiliners a evolução da coleta se aumentou, diminuiu etc
def calcularEvolucaoRecente(valores):
    if len(valores) < 12:
        return 0

    tamanhoTrecho = max(4,len(valores) // 3)

    valoresInicio = valores[:tamanhoTrecho]
    valoresFim = valores[-tamanhoTrecho:]

    medianaInicio = median(valoresInicio)
    medianaFim = median(valoresFim)

    return medianaFim - medianaInicio

#def que calcula a ocilação da credibilidade dessa projeção
def calcularRuidoSerie(valores):
    if len(valores) < 3:
        return 0

    diferencas = []

    for indice in range(1, len(valores)):
        diferencas.append(valores[indice] - valores[indice - 1])

    if not diferencas:
        return 0

    medianaDiferencas = median(diferencas)
    desviosAbsolutos = [abs(diferenca - medianaDiferencas) for diferenca in diferencas]

    return median(desviosAbsolutos)

# def que calcula a confiabilidade do componente de acordo com tudo o que calculamos antes, concordanci, evolucao, etc etc
def calcularConfiabilidadeComponente(concordancia,mudancaProjetada,limiarSinal,evolucaoRecente):
    pontuacaoConcordancia = min(1,max(0, concordancia)) * 50

    if limiarSinal > 0:
        relacaoSinal = mudancaProjetada / limiarSinal
        pontuacaoSinal = min(30, max(0, relacaoSinal - 0.5) * 20)
    else:
        pontuacaoSinal = 0

    pontuacaoEvolucao = min(1,max(0, evolucaoRecente) / 0.10) * 20

    confiabilidadeNumerica = (pontuacaoConcordancia+ pontuacaoSinal+ pontuacaoEvolucao)

    if confiabilidadeNumerica >= 80:
        nivel = "alta"
    elif confiabilidadeNumerica >= 65:
        nivel = "moderada"
    else:
        nivel = "baixa"

    return {
        "nivel": nivel,
        "percentual": round(confiabilidadeNumerica,2)
    }

# def que quando há uma consistencia de dados na coleta, ex: ram = 90% várias vezes seguidas, não deveria indicar um crescimento, já que permaneceu estável e não subiu, ess função serve para isso
def obterValorRecenteConservador(janelaAtual,campo,valorMinimo=0,valorMaximo=None):
    valoresRecentes = []

    for coleta in janelaAtual[-5:]:
        valor = converter_float(coleta.get(campo),padrao=None)

        if valor is not None:
            valoresRecentes.append(valor)

    if not valoresRecentes:
        return valorMinimo

    valor = median(valoresRecentes)
    valor = max(valorMinimo,valor)

    if valorMaximo is not None:
        valor = min( valorMaximo,valor)

    return valor

# def que constroi de fato as 10 coletas projetadas, com base em tudo que calculei até o momento para ser uma prjeção mais realista possivel
def construirJanelaProjetada(janelaAnterior,janelaAtual,limites,horizonteColetas=QUANTIDADE_COLETAS_PROJETADAS):
    if len(janelaAtual) < MINIMO_COLETAS_TENDENCIA:
        return {
            "janelaProjetada": janelaAtual.copy(),
            "componentesTendencia": [],
            "todasProjecoes": [],
            "horizonteColetas": 0,
            "horizonteMinutos": 0,
            "confiabilidade": "insuficiente"
        }

    configuracoes = obterConfigComponentes(limites)

    projecoes = {}

    todasProjecoes = []

    componentesTendencia = []

    for configuracao in configuracoes:
        resultadoProjecao = projetarComponente(janelaAtual,janelaAnterior,configuracao,horizonteColetas)

        projecoes[configuracao["campo"]] = (resultadoProjecao)

        todasProjecoes.append(resultadoProjecao)

        if resultadoProjecao["possuiTendencia"]:
            componentesTendencia.append({
                chave: valor
                for chave, valor in resultadoProjecao.items()
                if chave not in [
                    "campo",
                    "valoresProjetados"
                ]
            })

    componentesTendencia.sort(
        key=lambda componente: (
            componente["confiabilidadePercentual"],
            componente["mudancaProjetada"]
        ),
        reverse=True
    )

    quantidadeMantida = max(0,len(janelaAtual) - horizonteColetas)

    if quantidadeMantida > 0:
        janelaProjetada = [
            coleta.copy()
            for coleta in janelaAtual[-quantidadeMantida:]
        ]
    else:
        janelaProjetada = []

    ultimaColeta = janelaAtual[-1]

    for indiceProjecao in range(horizonteColetas):
        coletaProjetada = ultimaColeta.copy()

        for configuracao in configuracoes:
            campo = configuracao["campo"]

            resultado = projecoes[campo]

            if (resultado["possuiTendencia"]and indiceProjecao< len(resultado["valoresProjetados"])):
                valorFuturo = resultado[
                    "valoresProjetados"
                ][indiceProjecao]

            else:
                valorFuturo = obterValorRecenteConservador(janelaAtual,campo,configuracao["minimo"],configuracao["maximo"])

            coletaProjetada[campo] = valorFuturo

        coletaProjetada["TIPO_REGISTRO"] = "PROJETADO"

        janelaProjetada.append(coletaProjetada)

    horizontes = [
        item.get("horizonteMinutos", 0)
        for item in todasProjecoes
        if item.get("horizonteMinutos", 0) > 0
    ]

    horizonteMinutos = (
        median(horizontes)
        if horizontes
        else 0
    )

    if componentesTendencia:
        confiabilidades = [
            componente["confiabilidadePercentual"]
            for componente in componentesTendencia
        ]

        confiabilidadeMedia = (sum(confiabilidades)/ len(confiabilidades))

        if confiabilidadeMedia >= 80:
            confiabilidadeGeral = "alta"
        elif confiabilidadeMedia >= 65:
            confiabilidadeGeral = "moderada"
        else:
            confiabilidadeGeral = "baixa"

    else:
        confiabilidadeGeral = "sem tendência"

    return {
        "janelaProjetada": janelaProjetada,
        "componentesTendencia": componentesTendencia,
        "todasProjecoes": todasProjecoes,
        "horizonteColetas": horizonteColetas,
        "horizonteMinutos": round(horizonteMinutos,2),
        "confiabilidade": confiabilidadeGeral
    }

# def que indica o  motivo da projeção ter sido calculada daquela forma, qual ou quais componentes que a afetaram mais 
def gerarMotivoProjecao(componentesTendencia):
    if not componentesTendencia:
        return "Sem tendência consistente de degradação"

    principais = componentesTendencia[:2]

    nomes = [
        componente["componente"]
        for componente in principais
    ]

    if len(nomes) == 1:
        return (
            f"{nomes[0]} apresenta crescimento "
            f"consistente no período recente"
        )

    return (
        f"{nomes[0]} e {nomes[1]} apresentam "
        f"crescimento consistente no período recente"
    )


#------------------------------------------------------------------------------------------------------------------------------------------------
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

        conteudo = resposta["Body"].read().decode("utf-8")
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
    try:
        dadosServidor = (
            chamadosJson[empresa]
            [datacenter]
            [zona]
            [servidor]
        )

        chamados = dadosServidor.get("chamados", {})

        if isinstance(chamados, dict):
            return list(chamados.values())

        if isinstance(chamados, list):
            return chamados

        return []

    except (KeyError, TypeError):
        print(
            f"Chamados não encontrados para: "
            f"{empresa}/{datacenter}/{zona}/{servidor}"
        )
        return []
#df filtrando apenas os chamados criticos que afetam a disponibilidade do srv
def normalizarTexto(valor):
    if valor is None:
        return ""

    texto = str(valor).strip().lower()

    substituicoes = {
        "á": "a",
        "à": "a",
        "ã": "a",
        "â": "a",
        "é": "e",
        "ê": "e",
        "í": "i",
        "ó": "o",
        "ô": "o",
        "õ": "o",
        "ú": "u",
        "ç": "c"
    }

    for caractere, substituto in substituicoes.items():
        texto = texto.replace(caractere,substituto)
    return texto


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


def classificarSeveridadeAlerta(valor, limite):
    valor = converter_float(valor, None)
    limite = converter_float(limite, None)

    if valor is None or limite is None:
        return None

    if limite <= 0:
        return None

    if valor <= limite:
        return None

    percentualExcesso = (
        (valor - limite)
        / limite
    ) * 100

    if percentualExcesso <= 10:
        severidade = "baixo"

    elif percentualExcesso < 30:
        severidade = "medio"

    else:
        severidade = "critico"

    return {
        "severidade": severidade,
        "percentualExcesso": round(
            percentualExcesso,
            2
        )
    }




def gerarHistoricoAlertasComponentes(dataframe,metricasJson):
    if dataframe is None or dataframe.empty:
        return []

    dfAlertas = dataframe.copy()

    dfAlertas["DATE"] = pd.to_datetime(
        dfAlertas["DATE"],
        errors="coerce"
    )

    dfAlertas = dfAlertas.dropna(
        subset=["DATE"]
    )


    colunasDuplicidade = [
        "EMPRESA",
        "DATACENTER",
        "ZONA",
        "SERVIDOR",
        "DATE"
    ]

    colunasExistentes = [
        coluna
        for coluna in colunasDuplicidade
        if coluna in dfAlertas.columns
    ]

    if colunasExistentes:
        dfAlertas = dfAlertas.drop_duplicates(
            subset=colunasExistentes
        )

    agora = obterAgoraSaoPaulo()

    inicioSemanaAtual = (agora- timedelta(days=agora.weekday())).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0
    )

    inicioSemanaAnterior = (inicioSemanaAtual- timedelta(days=7))

    dfAlertas = dfAlertas[
        dfAlertas["DATE"]
        >= inicioSemanaAnterior
    ]

    historicoAlertas = []

    for coleta in dfAlertas.to_dict(orient="records"):
        empresa = coleta.get("EMPRESA")
        datacenter = coleta.get("DATACENTER")
        zona = coleta.get("ZONA")
        servidor = coleta.get("SERVIDOR")
        dataHora = coleta.get("DATE")

        if (empresa is None or datacenter is None or zona is None or servidor is None or pd.isna(dataHora)):
            continue

        try:
            infoServidor = (
                metricasJson[empresa]
                [datacenter]
                [zona]
                [servidor]
            )

            limites = infoServidor.get(
                "limites",
                {}
            )

        except (KeyError, TypeError):
            print(
                f"Limites não encontrados para alertas: "
                f"{empresa}/{datacenter}/{zona}/{servidor}"
            )
            continue

        componentes = obterConfigComponentes(limites)

        for componente in componentes:
            nomeComponente = componente["nome"]
            campo = componente["campo"]
            limite = componente["limite"]

            valor = converter_float(
                coleta.get(campo),
                None
            )

            resultadoSeveridade = classificarSeveridadeAlerta(
                valor,
                limite
            )

            if resultadoSeveridade is None:
                continue

            historicoAlertas.append({
                "empresa": empresa,
                "datacenter": datacenter,
                "zona": zona,
                "servidor": servidor,
                "componente": nomeComponente,
                "campo": campo,
                "valor": round(valor, 2),
                "limite": round(limite, 2),
                "percentualExcesso": resultadoSeveridade[
                    "percentualExcesso"
                ],
                "severidade": resultadoSeveridade[
                    "severidade"
                ],
                "timestamp": dataHora.isoformat()
            })

    print(
        f"Alertas calculados a partir das coletas: "
        f"{len(historicoAlertas)}"
    )

    return historicoAlertas


def criarEstruturaDiaAlertas():
    return {
        "baixo": 0,
        "medio": 0,
        "critico": 0,
        "total": 0
    }


def calcularAlertaSemana(historicoAlertas,empresa,datacenter):
    agora = obterAgoraSaoPaulo()

    inicioSemana = (agora- timedelta(days=agora.weekday())).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0
    )

    fimSemana = (inicioSemana+ timedelta(days=7))

    alertasPorDia = {
        dia: criarEstruturaDiaAlertas()
        for dia in DIAS_SEMANA
    }

    totaisSemana = criarEstruturaDiaAlertas()

    for alerta in historicoAlertas:
        if (str(alerta.get("empresa", ""))!= str(empresa)):
            continue

        if (str(alerta.get("datacenter", ""))!= str(datacenter)):
            continue

        dataAlerta = converterData(alerta.get("timestamp"))

        if dataAlerta is None:
            continue

        if not (inicioSemana<= dataAlerta< fimSemana):
            continue

        severidade = normalizarTexto(
            alerta.get("severidade")
        )

        if severidade not in ["baixo","medio","critico"]:
            continue

        nomeDia = DIAS_SEMANA[
            dataAlerta.weekday()
        ]

        alertasPorDia[nomeDia][
            severidade
        ] += 1

        alertasPorDia[nomeDia][
            "total"
        ] += 1

        totaisSemana[severidade] += 1
        totaisSemana["total"] += 1

    totaisPorDia = {
        dia: dados["total"]
        for dia, dados in alertasPorDia.items()
    }

    return {
        "periodoInicio": inicioSemana.isoformat(),
        "periodoFim": fimSemana.isoformat(),
        "totaisPorDia": totaisPorDia,
        "porSeveridade": alertasPorDia,
        "totaisSemana": totaisSemana
    }


def calcularVariacaoAlertas(quantidadeAtual,quantidadeAnterior):
    if quantidadeAnterior == 0:
        if quantidadeAtual > 0:
            return {
                "percentual": None,
                "valorFormatado": "Novo",
                "tendencia": "aumento"
            }

        return {
            "percentual": 0,
            "valorFormatado": "0%",
            "tendencia": "estável"
        }

    percentual = ((quantidadeAtual - quantidadeAnterior)/ quantidadeAnterior) * 100

    if percentual > 0:
        tendencia = "aumento"
        valorFormatado = (
            f"+{round(percentual, 2)}%"
        )

    elif percentual < 0:
        tendencia = "queda"
        valorFormatado = (
            f"{round(percentual, 2)}%"
        )

    else:
        tendencia = "estável"
        valorFormatado = "0%"

    return {
        "percentual": round(percentual,2),
        "valorFormatado": valorFormatado,
        "tendencia": tendencia
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
def calcularCrescimentoAlertas(
    historicoAlertas,
    empresa,
    datacenter
):
    agora = obterAgoraSaoPaulo()

    inicioSemanaAtual = (agora - timedelta(days=agora.weekday())).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0
    )

    tempoDecorridoSemana = (agora - inicioSemanaAtual)
    inicioSemanaAnterior = (inicioSemanaAtual - timedelta(days=7))
    fimPeriodoSemanaAnterior = (inicioSemanaAnterior+ tempoDecorridoSemana )

    alertasSemanaAtual = 0
    alertasSemanaAnterior = 0

    for alerta in historicoAlertas:
        if (
            str(alerta.get("empresa", "")) != str(empresa)
            or str(alerta.get("datacenter", "")) != str(datacenter)
        ):
            continue

        dataAlerta = converterData(
            alerta.get("timestamp")
        )

        if dataAlerta is None:
            continue

        if inicioSemanaAtual <= dataAlerta <= agora:
            alertasSemanaAtual += 1

        elif (
            inicioSemanaAnterior
            <= dataAlerta
            <= fimPeriodoSemanaAnterior
        ):
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
            (
                alertasSemanaAtual- alertasSemanaAnterior)/ alertasSemanaAnterior) * 100

        if crescimentoPercentual > 0:
            valorFormatado = (
                f"+{round(crescimentoPercentual, 2)}%"
            )
            tendencia = "aumento"

        elif crescimentoPercentual < 0:
            valorFormatado = (
                f"{round(crescimentoPercentual, 2)}%"
            )
            tendencia = "queda"

        else:
            valorFormatado = "0%"
            tendencia = "estável"

    percentualRetorno = (
        None
        if crescimentoPercentual is None
        else round(crescimentoPercentual, 2)
    )

    return {
        "percentual": percentualRetorno,
        "valorFormatado": valorFormatado,
        "alertasSemanaAtual": alertasSemanaAtual,
        "alertasSemanaAnterior": alertasSemanaAnterior,
        "descricao": (
            f"De {alertasSemanaAnterior} alertas "
            f"no mesmo período da semana anterior para "
            f"{alertasSemanaAtual} alertas na semana atual."
        ),
        "tendencia": tendencia,
        "periodoAtual": {
            "inicio": inicioSemanaAtual.isoformat(),
            "fim": agora.isoformat()
        },
        "periodoAnteriorComparado": {
            "inicio": inicioSemanaAnterior.isoformat(),
            "fim": fimPeriodoSemanaAnterior.isoformat()
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

                        try:
                            info_servidor = geral[empresa][datacenter][zona][servidor]
                            limites = info_servidor.get("limites", {})

                            print(" Limites encontrados:", limites)

                        except (KeyError, TypeError):
                            limites = {}
                            print(" Limites não encontrados. Usando fallback.")

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
                            "queda": resultadoScore["queda"],
                            "projecaoSaude": resultadoScore["projecaoSaude"],
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

                resultadoDatacenter = calcularScoreDatacenter(zonas)

                print("SCORE DATACENTER:", resultadoDatacenter)

                rankingSrvCriticosTop5 = sorted(
                    servidores_datacenter,
                    key=lambda servidor: servidor["score"]
                )[:5]

                rankingTendenciaServidores = sorted(
                    servidores_datacenter,
                    key=lambda servidor: (
                        servidor.get("projecaoSaude", {}).get("scoreProjetado",servidor.get("score", 100)),
                        -servidor.get("projecaoSaude", {}).get("degradacaoProjetada",0),
                        servidor.get("score", 100)
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
                    "rankingSrvCriticosTop5": rankingSrvCriticosTop5,
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