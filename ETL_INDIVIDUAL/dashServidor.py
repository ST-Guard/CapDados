import csv
import json
import boto3
from urllib.parse import unquote_plus
from datetime import datetime, timedelta

s3 = boto3.client('s3')

# Função inicial que chama as demais
def lambda_handler(event, context):
    print("Lambda Iniciada! 🪧")
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
        
        corte_7d = datetime.now() - timedelta(days=7)
        cabecalho = conteudo_csv[0]
        linhas_dados_invertidas = conteudo_csv[1:][::-1]
        conteudo_otimizado = [cabecalho] + linhas_dados_invertidas
        leitor = csv.DictReader(conteudo_otimizado, delimiter=";")
        
        dados_dicionario = []
        for linha in leitor:
            data_str = str(linha.get("DATE", ""))
            try:
                data_linha = datetime.fromisoformat(data_str).replace(tzinfo=None)
                if data_linha >= corte_7d:
                    dados_dicionario.append(linha)
                else: 
                    print(f"Alvo de 7 dias alcançado. Ignorando as próximas linhas.")
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

        # DASHBOARD Servidor  -Samuel
        respDashServidor = dashServidor(dados_dicionario, metricas, bucket)

        # DASH DE Servidor - Samuel (Salvando o resultado)
        s3.put_object(
            Bucket=bucket,
            Key="client/servidor.json",
            Body=json.dumps(respDashServidor, default=str, indent=4)
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


# JSON dashboard Servidor - Samuel
def dashServidor(dados, geral, bucket):

    servidores = {}

    todos = {
        "CPU": [],
        "RAM": [],
        "DISCO": [],
        "REDE": []
    }

    COMPONENTES = {
        "CPU": "CPU_PER",
        "RAM": "RAM_PER",
        "DISCO": "DISCO_PER",
        "REDE": "LATENCIA"
    }

    for linha in dados:

        todos["CPU"].append(float(linha[COMPONENTES["CPU"]]))
        todos["RAM"].append(float(linha[COMPONENTES["RAM"]]))
        todos["DISCO"].append(float(linha[COMPONENTES["DISCO"]]))
        todos["REDE"].append(float(linha[COMPONENTES["REDE"]]))

        nome_servidor = linha["SERVIDOR"]

        if nome_servidor not in servidores:
            servidores[nome_servidor] = {
                "CPU": [],
                "RAM": [],
                "DISCO": [],
                "REDE": []
            }

        servidores[nome_servidor]["CPU"].append(float(linha[COMPONENTES["CPU"]]))
        servidores[nome_servidor]["RAM"].append(float(linha[COMPONENTES["RAM"]]))
        servidores[nome_servidor]["DISCO"].append(float(linha[COMPONENTES["DISCO"]]))
        servidores[nome_servidor]["REDE"].append(float(linha[COMPONENTES["REDE"]]))

    def calcular_p99(valores):
        valores_ordenados = sorted(valores)
        indice = int(len(valores_ordenados) * 0.99)

        if indice >= len(valores_ordenados):
            indice = len(valores_ordenados) - 1

        return valores_ordenados[indice]
    
    resultado_servidores = {}

    for servidor, componentes in servidores.items():

        resultado_servidores[servidor] = {}

        for nome_componente, valores in componentes.items():
            valores_ordenados = sorted(valores)

            indice_p99 = int(len(valores_ordenados) * 0.99)

            if indice_p99 >= len(valores_ordenados):
                indice_p99 = len(valores_ordenados) - 1

            resultado_servidores[servidor][nome_componente] = {
                "p99": valores_ordenados[indice_p99],
                "media": round(sum(valores) / len(valores), 2),
                "maximo": max(valores),
                "minimo": min(valores)
            }

    return {
        "KPIS": {
            "P99CPUTotal": calcular_p99(todos["CPU"]),
            "P99RAMTotal": calcular_p99(todos["RAM"]),
            "P99DISCOTotal": calcular_p99(todos["DISCO"]),
            "P99REDETotal": calcular_p99(todos["REDE"])
        },
        "Servidores": resultado_servidores
    }