import csv
import json
import boto3
from urllib.parse import unquote_plus
from datetime import datetime, timedelta

s3 = boto3.client('s3')

# Função inicial que chama as demais
def lambda_handler(event, context):
    print("Lambda Alertas Iniciada! 🪧")
    print(f"Evento recebido: {event}")
    
    try:
        # Desempacota a mensagem do SNS
        mensagem_sns_texto = event["Records"][0]["Sns"]["Message"]
        evento_s3_real = json.loads(mensagem_sns_texto)
        
        if "Evento" in evento_s3_real and evento_s3_real["Evento"] == "s3:TestEvent":
            print("Evento de teste do S3 recebido e ignorado com sucesso! ✅")
            return {"statusCode": 200, "body": "TestEvent ignorado"}
            
        registro = evento_s3_real["Records"][0]["s3"]
        bucket = registro["bucket"]["name"]
        key = unquote_plus(registro["object"]["key"])

        if key.endswith("/") or registro["object"]["size"] == 0:
            print(f"Ignorado: {key} é um diretório ou arquivo vazio.")
            return {"statusCode": 200, "body": f"Ignorado: {key} é um diretório ou arquivo vazio."}

        if key != "trusted/dados_tratados.csv":
            print(f"Ignorando arquivo que não é o trusted principal: {key}")
            return {"statusCode": 200, "body": f"Arquivo ignorado: {key}"}

        print(f"Lendo arquivo Trusted no S3: {key}")
        
        # Lê o CSV tratado diretamente do S3 ignorando caracteres invisíveis (BOM)
        resposta_csv = s3.get_object(Bucket=bucket, Key=key)
        conteudo_csv = resposta_csv['Body'].read().decode('utf-8-sig').splitlines()
        
        corte_30d = datetime.now() - timedelta(days=30)
        cabecalho = conteudo_csv[0]
        linhas_dados_invertidas = conteudo_csv[1:][::-1]
        conteudo_otimizado = [cabecalho] + linhas_dados_invertidas
        leitor = csv.DictReader(conteudo_otimizado, delimiter=";")
        
        dados_dicionario = []
        for linha in leitor:
            data_str = str(linha.get("DATE", ""))
            try:
                data_linha = datetime.fromisoformat(data_str).replace(tzinfo=None)
                if data_linha >= corte_30d:
                    dados_dicionario.append(linha)
                else: 
                    print(f"Alvo de 30 dias alcançado. Ignorando as próximas linhas.")
                    break 
            except Exception:
                continue
        
        # Le os arquivos do JSON feito pelo SAMU
        metricas = {}
        try:
            resp_geral = s3.get_object(Bucket=bucket, Key="raw/metricas.json")
            metricas = json.loads(resp_geral['Body'].read().decode('utf-8'))
            print(f"metricas.json carregado com sucesso.")
        except Exception as e:
            print(f"metricas.json não encontrado — . Erro: {e}")

        # DASHBOARD ALERTAS  - Victor G
        respAlertasGestora = dashAlertasGestora(dados_dicionario, metricas, bucket)

        # DASH DE ALERTAS - Victor G (Salvando o resultado)
        s3.put_object(
            Bucket=bucket,
            Key="client/alertas_gestora.json",
            Body=json.dumps(respAlertasGestora, default=str, indent=4)
        )
        print("Todas as paginas processadas e atualizadas. 🟩")

        return {
            "statusCode": 200,
            "body": "Pipeline completo executado com sucesso."
        }

    except Exception as e:
        print(f"❌ Erro fatal: {e}")
        return {
            "statusCode": 500,
            "body": str(e)
        }


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

    # Função auxiliar para buscar no JSON ignorando maiúsculas/minúsculas
    def buscar_no_json_case_insensitive(dicionario, caminho):
        atual = dicionario
        for chave in caminho:
            chave_procurada_lower = chave.lower()
            encontrado = False
            for k in atual.keys():
                if k.lower() == chave_procurada_lower:
                    atual = atual[k]
                    encontrado = True
                    break
            if not encontrado:
                raise KeyError(f"Chave '{chave}' não encontrada")
        return atual

    SLA_HORAS = {"critico": 1, "medio": 4, "baixo": 24}
 
    resultado = {}
    ultimas_leituras = {}  
 
    for linha in dados:
        # Mantemos as chaves originais para exibir bonito depois
        empresa_orig = str(linha.get("EMPRESA",    "")).strip()
        regiao_orig      = str(linha.get("REGIAO", "")).strip()
        dc_orig      = str(linha.get("DATACENTER", "")).strip()
        zona_orig    = str(linha.get("ZONA",       "")).strip()
        servidor_orig = str(linha.get("SERVIDOR",   "")).strip()

        chave = (empresa_orig, regiao_orig, dc_orig, zona_orig, servidor_orig)
        
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
 
    chave_historico = "client/alertas_historico.json"
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
 
    for (empresa, regiao_orig, datacenter, zona, servidor), (data_leitura, linha) in ultimas_leituras.items():
 
        # TRAVA DE SEGURANÇA CONTRA CSV EM BRANCO
        if not empresa or not datacenter or not zona or not servidor:
            print(f"CSV incompleto! Empresa:'{empresa}', Região: '{regiao_orig}', DC:'{datacenter}', Zona:'{zona}', Servidor:'{servidor}' — pulando.")
            continue
 
        try:
            caminho_busca = [empresa, regiao_orig, datacenter, zona, servidor]
            info_servidor = buscar_no_json_case_insensitive(geral, caminho_busca)
            
            limites       = info_servidor.get("limites", {})

            lista_funcionarios = info_servidor.get("funcionarios", [])
            if lista_funcionarios and isinstance(lista_funcionarios[0], dict):
                id_analista   = lista_funcionarios[0].get("id")
                nome_analista = lista_funcionarios[0].get("nome", "Desconhecido")
            else:
                id_analista   = None
                nome_analista = "Desconhecido"
        except (KeyError, TypeError):
            print(f"Servidor {servidor} não encontrado no metricas.json — pulando.")
            continue
 
        if not limites:
            print(f"Sem limites definidos para {servidor} — pulando.")
            continue
 
        if empresa not in resultado:
            resultado[empresa] = {}

        if regiao_orig not in resultado[empresa]:
            resultado[empresa][regiao_orig] = {}
 
        if datacenter not in resultado[empresa][regiao_orig]:
            resultado[empresa][regiao_orig][datacenter] = {
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
 
        dc_dados = resultado[empresa][regiao_orig][datacenter]
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
            alertas_novos.append({**alerta, "datacenter": datacenter, "empresa": empresa, "regiao": regiao_orig})   

        top_atual = dc_dados["KPIs"]["QTD_ALERTAS_SERVIDOR_TOP"]
        if alertas_servidor_ciclo > top_atual:
            dc_dados["KPIs"]["SERVIDOR_MAIS_ALERTAS"]    = servidor
            dc_dados["KPIs"]["QTD_ALERTAS_SERVIDOR_TOP"] = alertas_servidor_ciclo
 

    agora = datetime.now()
    semanas = {} 
 
    for alerta_hist in historico_alertas:
        emp = str(alerta_hist.get("empresa",    ""))
        regiao = str(alerta_hist.get("regiao",     "")) 
        dc  = str(alerta_hist.get("datacenter", ""))
        sev = str(alerta_hist.get("severidade", ""))
        ts  = str(alerta_hist.get("timestamp",  ""))
 
        try:
            dt = datetime.fromisoformat(ts).replace(tzinfo=None)
        except:
            continue
 
        if dt < agora - timedelta(weeks=4):
            continue
 
        meses = ["janeiro","fevereiro","março","abril","maio","junho","julho","agosto","setembro","outubro","novembro","dezembro"]
        semana_do_mes = ((dt.day - 1) // 7) + 1
        nome_mes = meses[dt.month - 1]

        label = f"{semana_do_mes}° sem {nome_mes}"

        chave_sem = (emp, regiao, dc, f"{dt.year}-{dt.month:02d}-sem{semana_do_mes}")
 
        if chave_sem not in semanas:
            semanas[chave_sem] = {
                "semana":       label,
                "inicio":       (dt - timedelta(days=dt.weekday())).strftime("%d/%m"),
                "baixo":        0,
                "medio":        0,
                "critico":      0,
                "total":        0
            }
 
        if sev in ("baixo", "medio", "critico"):
            semanas[chave_sem][sev]    += 1
            semanas[chave_sem]["total"] += 1
 
        
    
    for (emp, regiao, dc, _), dados_semana in sorted(semanas.items()):
         if emp in resultado and regiao in resultado[emp] and dc in resultado[emp][regiao]:
             resultado[emp][regiao][dc]["GRAFICOS"]["ALERTAS_POR_SEMANA"].append(dados_semana)

    for emp in resultado:
        for regiao in resultado[emp]:
            for dc in resultado[emp][regiao]:
                semanas_dc = resultado[emp][regiao][dc]["GRAFICOS"]["ALERTAS_POR_SEMANA"]
                if not semanas_dc:
                    resultado[emp][regiao][dc]["GRAFICOS"]["RESUMO_SEMANAS"] = {
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
    
                resultado[emp][regiao][dc]["GRAFICOS"]["RESUMO_SEMANAS"] = {
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