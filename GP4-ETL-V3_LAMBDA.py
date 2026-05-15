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
        print(f"❌ Erro fatal: {e}")
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
    
    respFinanceiro = dashFinanceiro(dados_dicionario)
    respGestora = dashGestora(dados_dicionario)
    respAnalista = dashAnalista(dados_dicionario)

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
    
    

    return "Todas as paginas processadas e atualizadas."








# ZONA DE TRABALHO

def dashFinanceiro(dados):
    return {"tipo": "financeiro", "total_dados": len(dados)} 

def dashGestora(dados):
    return {"tipo": "gestora", "total_dados": len(dados)}

def dashAnalista(dados):
    return {"tipo": "analista", "total_dados": len(dados)}