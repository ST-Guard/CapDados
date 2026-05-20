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
    dados_dicionario = json.loads(conteudo_texto)

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

    respFinanceiro = dashFinanceiro(dados_dicionario)
    respGestora = dashGestora(dados_dicionario)
    respGestoraOp = dashOperacional(dados_dicionario, geral)
    respAnalista = dashAnalista(dados_dicionario)
    respServidores = dashServidores(dados_dicionario)
    # DASHBOARD ALERTAS  - Victor G
    respAlertasGestora = dashAlertasGestora(dados_dicionario, geral, bucket)
 


    s3.put_object(
        Bucket=bucket,
        Key="client/gestoraOp_master.json",
        Body=json.dumps(respGestoraOp, default=str, indent=4)
    )

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
        Key="client/alertas_gestora.json",
        Body=json.dumps(respAlertasGestora, default=str, indent=4)
    )

    s3.put_object(
        Bucket=bucket,
        Key="client/servidores_master.json",
        Body=json.dumps(respServidores, default=str, indent=4)
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
    contagem_por_tempo.columns = ['DATE', 'qntServidores']
 
    # Junta a contagem com as médias e divide a receita por servidor
    # Resultado:
    #   DATE   | RECEITA_5MIN | qntServidores | RECEITA_POR_SERVIDOR
    #   10:00  | R$ 98.958    |       3        |      R$ 32.986
    medias_por_tempo = medias_por_tempo.merge(contagem_por_tempo, on='DATE')
    medias_por_tempo['RECEITA_POR_SERVIDOR'] = (
        medias_por_tempo['RECEITA_5MIN'] / medias_por_tempo['qntServidores']
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




####################################################################################################################################################################
##################################################### DASHBOARD ALERTAS ############################################################################################

# JSON dashboard Alertas - Victor G

def dashAlertasGestora(dados, geral, bucket):
 
    COMPONENTES = {
        "CPU":   "CPU_PER",
        "RAM":   "RAM_PER",
        "DISCO": "DISCO_PER",
        "REDE":  "LATENCIA"
    }
 
    def calcular_severidade(valor, limite):
        if limite <= 0:
            return None
        excedido = ((valor - limite) / limite) * 100
        if excedido >= 30:
            return "critico"
        elif excedido >= 10:
            return "medio"
        elif excedido > 0:
            return "baixo"
        return None  # dentro do limite

    SLA_HORAS = {"critico": 1, "medio": 4, "baixo": 24}
 
    resultado = {}
 

    ultimas_leituras = {}  
 
    for linha in dados:
        chave = (
            str(linha.get("EMPRESA",    "")),
            str(linha.get("DATACENTER", "")),
            str(linha.get("ZONA",       "")),
            str(linha.get("SERVIDOR",   ""))
        )
        data_str = str(linha.get("DATE", ""))
        try:
            data_linha = datetime.fromisoformat(data_str).replace(tzinfo=None)
        except:
            continue
 
        if chave not in ultimas_leituras:
            ultimas_leituras[chave] = (data_linha, linha)
        else:
            if data_linha > ultimas_leituras[chave][0]:
                ultimas_leituras[chave] = (data_linha, linha)
 
    chave_historico = "trusted/alertas_historico.json"
    historico_alertas = []
    try:
        resp_hist = s3.get_object(Bucket=bucket, Key=chave_historico)
        historico_alertas = json.loads(resp_hist['Body'].read().decode('utf-8'))
        print(f"Histórico de alertas carregado: {len(historico_alertas)} registros.")
    except:
        print("Histórico de alertas não encontrado. Criando do zero.")

    corte_30d = datetime.now() - timedelta(days=30)
    historico_alertas = [
        a for a in historico_alertas
        if datetime.fromisoformat(str(a["timestamp"])).replace(tzinfo=None) >= corte_30d
    ]
 
    alertas_novos = []  
 
    for (empresa, datacenter, zona, servidor), (data_leitura, linha) in ultimas_leituras.items():
 
        try:
            info_servidor = geral[empresa][datacenter][zona][servidor]
            limites       = info_servidor.get("limites", {})
            id_analista   = info_servidor.get("id_analista", None)
            nome_analista = info_servidor.get("funcionarios", ["Desconhecido"])[0]
        except (KeyError, TypeError):
            print(f"[AVISO] Servidor {servidor} não encontrado no geral.json — pulando.")
            continue
 
        if not limites:
            print(f"[AVISO] Sem limites definidos para {servidor} — pulando.")
            continue
 
        if empresa not in resultado:
            resultado[empresa] = {}
 
        if datacenter not in resultado[empresa]:
            resultado[empresa][datacenter] = {
    
                "KPIs": {
                    "CRITICOS_ABERTOS":         0,
                    "MEDIOS_ABERTOS":            0,
                    "BAIXOS_ABERTOS":            0,
                    # Preenchido pelo Java após consultar chamados resolvidos no Jira
                    "RESOLVIDOS_24H":            0,
                    "SERVIDOR_MAIS_ALERTAS":     None,
                    "QTD_ALERTAS_SERVIDOR_TOP":  0
                },
        
                "ALERTAS_ATIVOS": [],
       
                "GRAFICOS": {
                    "ALERTAS_POR_COMPONENTE": {
                        "CPU":   0,
                        "RAM":   0,
                        "DISCO": 0,
                        "REDE":  0
                    },
                    "ALERTAS_POR_SEMANA": [],
                    "MTTR_POR_SERVIDOR": []
                }
            }
 
        dc_dados = resultado[empresa][datacenter]
 
        alertas_servidor_ciclo = 0
 
        for nome_comp, chave_metrica in COMPONENTES.items():
            valor  = linha.get(chave_metrica, None)
            limite = limites.get(nome_comp, None)
 
            if valor is None or limite is None:
                continue
 
            try:
                valor  = float(valor)
                limite = float(limite)
            except (ValueError, TypeError):
                continue
 
            severidade = calcular_severidade(valor, limite)
            if severidade is None:
                continue  # componente dentro do limite — sem alerta
 
            alertas_servidor_ciclo += 1
            timestamp_str = str(data_leitura)
 
            if severidade == "critico":
                dc_dados["KPIs"]["CRITICOS_ABERTOS"] += 1
            elif severidade == "medio":
                dc_dados["KPIs"]["MEDIOS_ABERTOS"]   += 1
            else:
                dc_dados["KPIs"]["BAIXOS_ABERTOS"]   += 1
 
            dc_dados["GRAFICOS"]["ALERTAS_POR_COMPONENTE"][nome_comp] += 1
 
            alerta = {
                "servidor":          servidor,
                "zona":              zona,
                "componente":        nome_comp,
                "valor":             round(valor, 2),
                "threshold_momento": round(limite, 2),
                "severidade":        severidade,
                "id_responsavel":    id_analista,
                "nome_responsavel":  nome_analista,
                "timestamp":         timestamp_str,
                "sla_prazo_horas":   SLA_HORAS[severidade],
                "issue_key":         None,
                "status":            "aberto"
            }
 
            dc_dados["ALERTAS_ATIVOS"].append(alerta)
            alertas_novos.append({**alerta, "datacenter": datacenter, "empresa": empresa})

        top_atual = dc_dados["KPIs"]["QTD_ALERTAS_SERVIDOR_TOP"]
        if alertas_servidor_ciclo > top_atual:
            dc_dados["KPIs"]["SERVIDOR_MAIS_ALERTAS"]    = servidor
            dc_dados["KPIs"]["QTD_ALERTAS_SERVIDOR_TOP"] = alertas_servidor_ciclo
 

    agora = datetime.now()
    semanas = {} 
 
    for alerta_hist in historico_alertas:
        emp = str(alerta_hist.get("empresa",    ""))
        dc  = str(alerta_hist.get("datacenter", ""))
        sev = str(alerta_hist.get("severidade", ""))
        ts  = str(alerta_hist.get("timestamp",  ""))
 
        try:
            dt = datetime.fromisoformat(ts).replace(tzinfo=None)
        except:
            continue
 
        if dt < agora - timedelta(weeks=4):
            continue
 
        num_semana  = dt.isocalendar()[1]
        ano_semana  = dt.isocalendar()[0]
        inicio_semana = dt - timedelta(days=dt.weekday())
        chave_sem   = (emp, dc, f"{ano_semana}-S{num_semana:02d}")
 
        if chave_sem not in semanas:
            semanas[chave_sem] = {
                "semana":       f"S{num_semana:02d}",
                "inicio":       inicio_semana.strftime("%d/%m"),
                "baixo":        0,
                "medio":        0,
                "critico":      0,
                "total":        0
            }
 
        if sev in ("baixo", "medio", "critico"):
            semanas[chave_sem][sev]    += 1
            semanas[chave_sem]["total"] += 1
 
    for (emp, dc, _), dados_semana in sorted(semanas.items()):
        if emp in resultado and dc in resultado[emp]:
            resultado[emp][dc]["GRAFICOS"]["ALERTAS_POR_SEMANA"].append(dados_semana)
 
    for emp in resultado:
        for dc in resultado[emp]:
            semanas_dc = resultado[emp][dc]["GRAFICOS"]["ALERTAS_POR_SEMANA"]
            if not semanas_dc:
                resultado[emp][dc]["GRAFICOS"]["RESUMO_SEMANAS"] = {
                    "total_alertas":       0,
                    "semana_mais_critica": None,
                    "severidade_dominante": None,
                    "alertas_dominante":   0
                }
                continue
 
            total_alertas = sum(s["total"] for s in semanas_dc)
 
            semana_mais_critica = max(semanas_dc, key=lambda s: s["critico"])
 
            total_baixo   = sum(s["baixo"]   for s in semanas_dc)
            total_medio   = sum(s["medio"]   for s in semanas_dc)
            total_critico = sum(s["critico"] for s in semanas_dc)
 
            sev_dom, qtd_dom = max(
                [("baixo", total_baixo), ("medio", total_medio), ("critico", total_critico)],
                key=lambda x: x[1]
            )
 
            resultado[emp][dc]["GRAFICOS"]["RESUMO_SEMANAS"] = {
                "total_alertas":        total_alertas,
                "semana_mais_critica":  semana_mais_critica["semana"],
                "severidade_dominante": sev_dom,
                "alertas_dominante":    qtd_dom
            }
 
    historico_alertas.extend(alertas_novos)
    try:
        s3.put_object(
            Bucket=bucket,
            Key=chave_historico,
            Body=json.dumps(historico_alertas, default=str, indent=4)
        )
        print(f"Histórico atualizado: {len(historico_alertas)} registros.")
    except Exception as e:
        print(f"Erro ao salvar histórico: {e}")
 
    print(f"dashAlertasGestora concluída. Empresas processadas: {list(resultado.keys())}")
    return resultado


#################################################################################################################################################
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
            "penalidadeTendencia": 0
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

    return {
        "score": round(scoreFinal, 2),
        "status": classificarStatusScore(scoreFinal),
        "scoreParcialAtual": round(scoreParcialAtual, 2),
        "scoreParcialAnterior": round(scoreParcialAnterior, 2),
        "queda": round(queda, 2),
        "penalidadeTendencia": penalidadeTendencia
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

def dashOperacional(dados,geral):
    df = pd.DataFrame(dados)

    if df.empty:
        return {
            "tipo": "gestora",
            "total_dados": 0,
            "datacenters": {}
        }

    df["DATE"] = pd.to_datetime(df["DATE"])

    resultado = {}

    for (empresa, datacenter), df_dc in df.groupby(["EMPRESA", "DATACENTER"]):

        zonas = []
        servidores_datacenter = []

        for zona, df_zona in df_dc.groupby("ZONA"):

            servidoresZona = []

            for servidor, df_servidor in df_zona.groupby("SERVIDOR"):
                try:
                    info_servidor = geral[empresa][datacenter][zona][servidor]
                    limites = info_servidor.get("limites", {})
                except (KeyError, TypeError):
                    limites = {}

                df_servidor = df_servidor.sort_values("DATE")

                coletaServidor = df_servidor.to_dict(orient="records")

                resultadoScore = calcularScoreServidor(coletaServidor,limites)

                servidor_obj = {
                    "servidor": servidor,
                    "zona": zona,
                    "score": resultadoScore["score"],
                    "status": resultadoScore["status"],
                    "scoreParcialAtual": resultadoScore["scoreParcialAtual"],
                    "scoreParcialAnterior": resultadoScore["scoreParcialAnterior"],
                    "queda": resultadoScore["queda"],
                    "penalidadeTendencia": resultadoScore["penalidadeTendencia"]
                }

                servidoresZona.append(servidor_obj)
                servidores_datacenter.append(servidor_obj)

            resultadoZona = calcularScoreZona(servidoresZona)

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

        resultadoDatacenter = calcularScoreDatacenter(zonas)

        rankingSrvCriticosTop5 = sorted(
            servidores_datacenter,
            key=lambda servidor: servidor["score"]
        )[:5]

        resultado.setdefault(empresa, {})

        resultado[empresa][datacenter] = {
            "score": resultadoDatacenter["score"],
            "status": resultadoDatacenter["status"],
            "qntZonas": resultadoDatacenter["qntZonas"],
            "qntZonasCriticas": resultadoDatacenter["qntZonasCriticas"],
            "qntZonasAtencao": resultadoDatacenter["qntZonasAtencao"],
            "zonaPiorScore": resultadoDatacenter["zonaPiorScore"],
            "zonas": zonas,
            "rankingSrvCriticosTop5": rankingSrvCriticosTop5
        }

    return {
        "tipo": "gestora",
        "total_dados": len(dados),
        "datacenters": resultado
    }

###########################################################################################################################################################################