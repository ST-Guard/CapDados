import csv
import json
import boto3
import io  
import pandas as pd 
from urllib.parse import unquote_plus
from datetime import datetime, timedelta
from statistics import median

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

    df = pd.read_csv(io.StringIO(conteudo_texto), delimiter=";")
    dados_dicionario = df.to_dict(orient="records")

    respGestoraOp = dashOperacional(dados_dicionario, geral, bucket)

    s3.put_object(
        Bucket=bucket,
        Key="client/dashOpGestao.json",
        Body=json.dumps(respGestoraOp, default=str, indent=4),
        ContentType="application/json"
    )

    print("Todas as paginas processadas e atualizadas. 🟩")
    return "Lambda concluida com sucesso! ✅"

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

    janelaProjetada = [
        coleta.copy()
        for coleta in janelaAtual[-quantidadeMantida:]
    ]

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

def classificarStatusUptime(uptime):
    if uptime >= 99:
        return "Saudável"
    elif uptime >= 95:
        return "Atenção"
    return "Crítico"

#df que lê o json de chamados
def carregarChamadosJson(bucket):
    try:
        s3 = boto3.client("s3")

        resposta = s3.get_object(
            Bucket=bucket,
            Key="dados_alertas/ultimos_alertas.json"
        )

        conteudo = resposta["Body"].read().decode("utf-8")
        chamadosJson = json.loads(conteudo)

        print("✅ chamados.json carregado do S3")
        return chamadosJson

    except Exception as e:
        print(f"⚠️ Erro ao carregar chamados.json: {e}")
        return {}

#df que pega os chamadso de cada srv individualmente
def obterChamadosServidor(chamadosJson, empresa, datacenter, zona, servidor):
    try:
        dadosServidor = chamadosJson[empresa][datacenter][zona][servidor]
        return dadosServidor.get("chamados", [])

    except (KeyError, TypeError):
        return []

#df filtrando apenas os chamados criticos que afetam a disponibilidade do srv
def chamadoContaComoIndisponibilidade(chamado):
    severidade = chamado.get("severidade")

    if severidade is None or str(severidade).strip() == "":
        return True

    return str(severidade).lower() in ["critico", "crítico"]

def converterData(valor):
    if valor is None:
        return None

    if isinstance(valor, datetime):
        return valor.replace(tzinfo=None)

    if isinstance(valor, str):
        valor = valor.strip()

        if valor == "":
            return None

        try:
            return datetime.fromisoformat(valor).replace(tzinfo=None)
        except ValueError:
            try:
                return datetime.strptime(valor, "%Y-%m-%d %H:%M:%S").replace(tzinfo=None)
            except ValueError:
                print(f"⚠️ Data inválida: {valor}")
                return None

    return None

#def que de acordo com os seguntos em que o chamado ficou aberto( rodando a cada 5 minuots para a atualização) considera isso como indisponibilidade e faz o calculo do uptime do servidor
def calcularUptimeServidor(chamadosServidor, servidor, zona, inicioPeriodo, fimPeriodo):
    tempoTotalSegundos = (fimPeriodo - inicioPeriodo).total_seconds()

    tempoIndisponivelSegundos = 0
    qtdChamadosIndisponibilidade = 0

    for chamado in chamadosServidor:
        if not chamadoContaComoIndisponibilidade(chamado):
            continue

        duracaoSegundos = chamado.get("duracaoSegundos")

        if duracaoSegundos is not None:
            try:
                duracao = float(duracaoSegundos)

                if duracao < 0:
                    print(f"⚠️ Duração negativa ignorada no servidor {servidor}: {duracao}")
                    continue

                duracao = min(duracao, tempoTotalSegundos)

                tempoIndisponivelSegundos += duracao
                qtdChamadosIndisponibilidade += 1
                continue

            except (ValueError, TypeError):
                print(f"⚠️ Duração inválida no chamado do servidor {servidor}: {duracaoSegundos}")

        abertura = (
            chamado.get("abertura")
            or chamado.get("aberto_em")
            or chamado.get("inicioIndisponibilidade")
        )

        abertura = converterData(abertura)

        if abertura is None:
            continue

        inicioIndisponibilidade = max(abertura, inicioPeriodo)
        fimIndisponibilidade = fimPeriodo

        if fimIndisponibilidade <= inicioIndisponibilidade:
            continue

        duracaoCalculada = (fimIndisponibilidade - inicioIndisponibilidade).total_seconds()

        if duracaoCalculada < 0:
            continue

        duracaoCalculada = min(duracaoCalculada, tempoTotalSegundos)

        tempoIndisponivelSegundos += duracaoCalculada
        qtdChamadosIndisponibilidade += 1

    tempoIndisponivelSegundos = min(
        tempoIndisponivelSegundos,
        tempoTotalSegundos
    )

    if tempoTotalSegundos <= 0:
        uptime = 100
    else:
        uptime = ((tempoTotalSegundos - tempoIndisponivelSegundos) / tempoTotalSegundos) * 100

    uptime = max(0, min(100, uptime))

    tempoIndisponivelHoras = tempoIndisponivelSegundos / 3600

    return {
        "servidor": servidor,
        "zona": zona,
        "uptime": round(uptime, 2),
        "tempoIndisponivelSegundos": round(tempoIndisponivelSegundos, 2),
        "tempoIndisponivelHoras": round(tempoIndisponivelHoras, 2),
        "qtdChamadosIndisponibilidade": qtdChamadosIndisponibilidade,
        "statusUptime": classificarStatusUptime(uptime)
    }
#------------------------------------------------------------------------------------------------------------------------------------------------

#alertas semana
def carregarHistoricoAlertas(bucket):
   pathHistorico = "da/trustedalertas_historico.json"

   try:
        s3 = boto3.client("s3")

        resp_hist = s3.get_object(
            Bucket=bucket,
            Key=pathHistorico
        )

        historicoAlertas = json.loads(
            resp_hist["Body"].read().decode("utf-8")
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
    inicioSemana = (agora - timedelta(days=agora.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
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

    inicioSemanaAtual = (agora - timedelta(days=agora.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
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
        except Exception:
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

    percentualRetorno = (
        None if crescimentoPercentual is None
        else round(crescimentoPercentual, 2)
    )

    return {
        "percentual": percentualRetorno,
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
            "empresas": {}
        }

    df["DATE"] = pd.to_datetime(df["DATE"], format='mixed')

    historicoAlertas = carregarHistoricoAlertas(bucket)

    fimPeriodoUptime = datetime.now()
    inicioPeriodoUptime = fimPeriodoUptime - timedelta(days=7)

    chamadosJson = carregarChamadosJson(bucket)

    resultado = {}

    for empresa, df_empresa in df.groupby("EMPRESA"):

        print(f"\n🏢 EMPRESA: {empresa}")

        resultado.setdefault(empresa, {
            "regioes": {},
            "datacenters": {}
        })

        for regiao, df_regiao in df_empresa.groupby("REGIAO"):

            print(f"\n🌎 REGIÃO: {regiao}")

            datacentersRegiao = []

            for datacenter, df_dc in df_regiao.groupby("DATACENTER"):

                print(f"\n🏢 DATACENTER: {datacenter}")

                graficoAlertasSemana = calcularAlertaSemana(
                    historicoAlertas,
                    empresa,
                    datacenter
                )

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
                            limites = info_servidor.get("limites", {})

                            print("✅ Limites encontrados:", limites)

                        except (KeyError, TypeError):
                            limites = {}
                            print("⚠️ Limites não encontrados. Usando fallback.")

                        chamadosServidor = obterChamadosServidor(
                            chamadosJson,
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
                    "graficoAlertasSemana": graficoAlertasSemana,
                    "uptimeServidores": uptimeServidores,
                    "kpiUptime": kpiUptime,
                    "kpiCrescimentoIncidentes": kpiCrescimentoIncidentes,
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

                print(f"✅ JSON FINAL DO DATACENTER {datacenter} CRIADO")

            resultadoRegiao = calcularScoreRegiao(datacentersRegiao)

            print("📊 SCORE REGIÃO:", resultadoRegiao)

            resultado[empresa]["regioes"][regiao] = {
                "score": resultadoRegiao["score"],
                "status": resultadoRegiao["status"],
                "qntDatacenters": resultadoRegiao["qntDatacenters"],
                "qntDatacentersCriticos": resultadoRegiao["qntDatacentersCriticos"],
                "qntDatacentersAtencao": resultadoRegiao["qntDatacentersAtencao"],
                "datacenterPiorScore": resultadoRegiao["datacenterPiorScore"],
                "datacenters": datacentersRegiao
            }

            print(f"✅ JSON FINAL DA REGIÃO {regiao} CRIADO")

    print("\n🎉 DASH OPERACIONAL FINALIZADA")

    return {
        "tipo": "gestora",
        "total_dados": len(dados),
        "empresas": resultado
    }   