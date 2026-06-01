import csv
import json
import boto3
from urllib.parse import unquote_plus
from datetime import datetime, timedelta

s3 = boto3.client('s3')

# Função inicial que chama as demais
def lambda_handler(event, context):
    print("Lambda Iniciada!")
    print(f"Evento recebido: {event}")
    
    try:
        print("Iniciando Trusted")
        resultado_trusted = TrustedJson(event, context)

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
def TrustedJson(event, context):
    #Pega o arquivo que chegou no Lambda
    registro = event["Records"][0]["s3"]
    bucket = registro["bucket"]["name"]
    key = unquote_plus(registro["object"]["key"])

    #Valida se o evento que chamou o lambda seja outra coisa
    if key.endswith("/") or registro["object"]["size"] == 0:
        return f"Ignorado: {key} é um diretório ou arquivo vazio."

    #Pega informações do arquivo e pasta de origem e finalidade
    nome_arquivo = key.split("/")[-1]
    nome_base = nome_arquivo.rsplit('.', 1)[0]

    caminho_local_entrada = f"/tmp/{nome_arquivo}"
    caminho_local_mestre = "/tmp/dados_mestre.json"
    chave_destino_mestre = "trusted/dados_mestre.json"

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
    linhas = []

    #Ler o CSV
    with open(caminho_local_entrada, "r", encoding="utf-8") as entrada:
        leitor = csv.DictReader(entrada, delimiter=";")
        #Para cada linha
        for linha in leitor:
            data_hora = datetime.fromisoformat(linha["DATA_HORA"]).replace(tzinfo=None)

            # Pula a linha se for mais velha que 7 dias
            if data_hora < limite_tempo7:
                continue
        
            linha["RAM_TOTAL_GB"] = round(float(linha.get("RAM_TOTAL", 0)) / (1024 ** 3), 2)
            linha["RAM_USADA_GB"] = round(float(linha.get("RAM_USADA", 0)) / (1024 ** 3), 2)
            linha["DISCO_TOTAL_GB"] = round(float(linha.get("DISCO_TOTAL", 0)) / (1024 ** 3), 2)
            linha["DISCO_USADO_GB"] = round(float(linha.get("DISCO_USADO", 0)) / (1024 ** 3), 2)
            linha["LATENCIA"] = round(float(linha.get("LATENCIA", 0)), 2)

            linha["PROCESSO1_RAM_GB"] = round(float(linha.get("PORCENTAGEM_PROCESSO1_RAM", 0)) / (1024 ** 3), 2)
            linha["PROCESSO2_RAM_GB"] = round(float(linha.get("PORCENTAGEM_PROCESSO2_RAM", 0)) / (1024 ** 3), 2)
            linha["PROCESSO3_RAM_GB"] = round(float(linha.get("PORCENTAGEM_PROCESSO3_RAM", 0)) / (1024 ** 3), 2)

            if linha["RAM_TOTAL_GB"] > 0:
                linha["PROCESSO1_RAM_PERC"] = round((linha["PROCESSO1_RAM_GB"] * 100) / linha["RAM_TOTAL_GB"], 2)
                linha["PROCESSO2_RAM_PERC"] = round((linha["PROCESSO2_RAM_GB"] * 100) / linha["RAM_TOTAL_GB"], 2)
                linha["PROCESSO3_RAM_PERC"] = round((linha["PROCESSO3_RAM_GB"] * 100) / linha["RAM_TOTAL_GB"], 2)
            else:
                linha["PROCESSO1_RAM_PERC"] = linha["PROCESSO2_RAM_PERC"] = linha["PROCESSO3_RAM_PERC"] = 0.0

            linha["BOOTTIME_DT"] = datetime.fromtimestamp(float(linha.get("BOOTTIME", 0)))
            linha["UPTIME"] = str(data_hora - linha["BOOTTIME_DT"])
            linha["HORA_TRATAMENTO"] = str(datetime.now())

            linha_final = {}
            for coluna_antiga, coluna_nova in colunas_finais.items():
                linha_final[coluna_nova] = linha.get(coluna_antiga, "")

            linhas.append(linha_final)
                
    if not linhas:
        return f"Arquivo {nome_arquivo} lido, mas nenhuma linha se qualificou (dentro de 7 dias)."
    
    dados_unificados = []
    try:
        print(f"Tentando ler arquivo mestre existente: {chave_destino_mestre}")
        resposta_mestre = s3.get_object(Bucket=bucket, Key=chave_destino_mestre)
        conteudo_mestre = resposta_mestre['Body'].read().decode('utf-8')
        dados_unificados = json.loads(conteudo_mestre)
        print(f"Arquivo mestre carregado com {len(dados_unificados)} linhas.")
    except Exception as e:
        print("Arquivo mestre nao encontrado. Criando um novo do zero.")

    
    dados_unificados.extend(linhas)
    
    with open(caminho_local_mestre, "w", encoding="utf-8") as saida:
        json.dump(dados_unificados, saida, indent=4, ensure_ascii=False, default=str)

    print(f"Fazendo upload do JSON unificado para: {chave_destino_mestre}")
    s3.upload_file(caminho_local_mestre, bucket, chave_destino_mestre)

    return {
        "mensagem": f"Arquivo unificado. Adicionadas {len(linhas)} novas linhas. Total agora: {len(dados_unificados)}",
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
 
    # DASHBOARD ALERTAS  - Victor G
    respAlertasGestora = dashAlertasGestora(dados_dicionario, geral, bucket)

    # DASH DE ALERTAS - Victor G
    s3.put_object(
        Bucket=bucket,
        Key="client/alertas_gestora.json",
        Body=json.dumps(respAlertasGestora, default=str, indent=4)
    )

    return "Todas as paginas processadas e atualizadas."

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
        return None 

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
            print(f"Servidor {servidor} não encontrado no geral.json — pulando.")
            continue
 
        if not limites:
            print(f"Sem limites definidos para {servidor} — pulando.")
            continue
 
        if empresa not in resultado:
            resultado[empresa] = {}
 
        if datacenter not in resultado[empresa]:
            resultado[empresa][datacenter] = {
    
                "KPIs": {
                    "CRITICOS_ABERTOS":         0,
                    "MEDIOS_ABERTOS":            0,
                    "BAIXOS_ABERTOS":            0,
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
                continue 
 
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