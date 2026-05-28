import csv
import json
import boto3
from urllib.parse import unquote_plus
from datetime import datetime, timedelta
import pandas as pd
import requests
import io

s3 = boto3.client('s3')




# Função inicial que chama as demais
def lambda_handler(event, context):
    print("Lambda Iniciada!" )
    print(f"Evento recebido: {event}")

    #Verifica se é o JSON de metricas
    registro = event["Records"][0]["s3"]
    key = unquote_plus(registro["object"]["key"])

    if not key.startswith("raw/"):
        print(f"Ignorando arquivo fora do raw/: {key}")
        return

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



def dashAnalista(dados, geral):
        print("Iniciando o client para a dashboard  analista 📀")

        df = pd.DataFrame(dados)

    

        if df.empty:
            print("⚠️ DataFrame vazio")
            return {
                "tipo": "analista",
                "total_dados": 0,
                "datacenters": {}
            }

        df["DATE"] = pd.to_datetime(df["DATE"])
        df["EMPRESA"] = df["EMPRESA"].astype(str)
        df["ZONA"]  = df["ZONA"].astype(str) 
        df["DATACENTER"] = df["DATACENTER"].astype(str)
        df["SERVIDOR"] = df["SERVIDOR"].astype(str)

        for empresa in df.groupby(["EMPRESA"]):
            print("AGORA EU ESTOU NA EMPRESA: ", empresa["EMPRESA"], "⛳")
        
            print("ESTA EMPRESA TEM: ", len(empresa), " PARA PROCESSAR")
            for datacenter in df.groupby(["DATACENTER"]):
                print("AGORA EU ESTOU NO DATACENTER: ", datacenter["DATACENTER"])
                for zona in df.groupby(["ZONA"]):
                    print("AGORA EU ESTOU NA ZONA: ", zona["CENTER"])
                    for servidor in df.groupby(["SERVIDOR"]):
                        print("ESTOU NO SERVIDOR", servidor["CENTER"])





       



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

  
    respGestoraOp = dashAnalista(dados_dicionario, geral)
   
 


    s3.put_object(
        Bucket=bucket,
        Key="client/gestoraOp_master.json",
        Body=json.dumps(respGestoraOp, default=str, indent=4)
    )

    
    print("Todas as paginas processadas e atualizadas.")
    return "Lambda concluida com sucesso! ✅"




