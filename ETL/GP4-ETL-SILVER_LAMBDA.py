import csv
import json
import boto3
from urllib.parse import unquote_plus
from datetime import datetime, timedelta
import pandas as pd
import io

s3 = boto3.client('s3')

def lambda_handler(event, context):
    print("Lambda Iniciada!")
    try:
        registro = event["Records"][0]["s3"]
        key = unquote_plus(registro["object"]["key"])

        if not key.startswith("raw/"):
            print(f"Ignorando arquivo fora do raw/: {key}")
            return {"statusCode": 200, "body": "Arquivo fora da pasta raw/."}

        if key.lower().endswith(".json"):
            return {"statusCode": 200, "body": f"Arquivo JSON ignorado: {key}"}
        
        print("Iniciando processamento Trusted...")
        resultado_trusted = TrustedCsv(event, context)

        if isinstance(resultado_trusted, dict) and "chave" in resultado_trusted:
            print(f"Sucesso: {resultado_trusted['mensagem']}")
            return {
                "statusCode": 200,
                "body": json.dumps(resultado_trusted)
            }
        else:
            print(f"Aviso: {resultado_trusted}")
            return {"statusCode": 200, "body": str(resultado_trusted)}

    except Exception as e:
        print(f"ERRO CRÍTICO NA LAMBDA: {str(e)}")
        import traceback
        traceback.print_exc() 
        return {
            "statusCode": 500,
            "body": f"Erro fatal no pipeline: {str(e)}"
        }

def TrustedCsv(event, context):
    registro = event["Records"][0]["s3"]
    bucket = registro["bucket"]["name"]
    key = unquote_plus(registro["object"]["key"])

    if key.endswith("/") or registro["object"]["size"] == 0:
        return f"Ignorado: {key} vazio ou diretório."

    nome_arquivo = key.split("/")[-1]
    caminho_local_entrada = f"/tmp/{nome_arquivo}"
    caminho_local_mestre = "/tmp/dados_mestre.csv"
    chave_destino_mestre = "trusted/dados_tratados.csv"

    print(f"Baixando arquivo de entrada: {key}")
    s3.download_file(bucket, key, caminho_local_entrada)

    mapeamento_colunas = {
        'EMPRESA': 'EMPRESA', 
        'REGIAO': 'REGIAO', 
        'DATACENTER': 'DATACENTER', 
        'ZONA': 'ZONA', 
        'SERVIDOR': 'SERVIDOR',
        'CPU': 'CPU_PER', 
        'QTD_NUCLEOS': 'QTD_NUCLEOS', 
        'RAM_TOTAL': 'RAM_TOTAL', 
        'RAM_USADA': 'RAM_USADO',
        'RAM_PERCENT': 'RAM_PER', 
        'DISCO_TOTAL': 'DISCO_TOTAL', 
        'DISCO_USADO': 'DISCO_USADO', 
        'DISCO_PERCENT': 'DISCO_PER',
        'LATENCIA': 'LATENCIA', 
        'PACOTES_ENVIADOS': 'PACOTES_ENV', 
        'PACOTES_RECEBIDOS': 'PACOTES_RCB', 
        'PACOTES_PERDIDOS': 'PACOTES_PER',
        'QTD_PR': 'QTR_PR', 
        'JOGADORES_ATIVOS': 'JOGADORES_ATIVOS', 
        'USO_USER': 'USO_USER', 
        'USO_SISTEM': 'USO_SISTEM',
        'PROCESSO1_CPU': 'PROCESSO01_CPU_N', 
        'PORCENTAGEM_PROCESSO1_CPU': 'PROCESSO1_CPU_P',
        'PROCESSO2_CPU': 'PROCESSO2_CPU_N', 
        'PORCENTAGEM_PROCESSO2_CPU': 'PROCESSO2_CPU_P',
        'PROCESSO3_CPU': 'PROCESSO3_CPU_N', 
        'PORCENTAGEM_PROCESSO3_CPU': 'PROCESSO3_CPU_P',
        'PROCESSO1_RAM': 'PROCESSO01_RAM_N', 
        'PROCESSO1_RAM_GB': 'PROCESSO1_RAM_T', 
        'PROCESSO1_RAM_PERC': 'PROCESSO1_RAM_P',
        'PROCESSO2_RAM': 'PROCESSO2_RAM_N', 
        'PROCESSO2_RAM_GB': 'PROCESSO2_RAM_T', 
        'PROCESSO2_RAM_PERC': 'PROCESSO2_RAM_P',
        'PROCESSO3_RAM': 'PROCESSO3_RAM_N', 
        'PROCESSO3_RAM_GB': 'PROCESSO3_RAM_T', 
        'PROCESSO3_RAM_PERC': 'PROCESSO3_RAM_P',
        'BOOTTIME_DT': 'BOOTTIME', 
        'DATA_HORA': 'DATE', 
        'UPTIME': 'UPTIME', 
        'HORA_TRATAMENTO': 'HORA_TRATAMENTO', 
        'DIA_SEMANA': 'DIA_SEMANA'
    }
    # Leitura do CSV
    df = pd.read_csv(caminho_local_entrada, delimiter=";", encoding="utf-8-sig")
    df.columns = df.columns.str.strip()

    # Tratamento da Data do psutil 
    df["DATA_HORA"] = pd.to_datetime(df["DATA_HORA"])

    if df.empty:
        return f"Arquivo {nome_arquivo} processado, mas não contém dados."


    # Convertendo Bytes para Gigabytes
    df["RAM_TOTAL_GB"]   = (df["RAM_TOTAL"].astype(float) / (1024 ** 3)).round(2)
    df["RAM_USADA_GB"]   = (df["RAM_USADA"].astype(float) / (1024 ** 3)).round(2)
    df["DISCO_TOTAL_GB"] = (df["DISCO_TOTAL"].astype(float) / (1024 ** 3)).round(2)
    df["DISCO_USADO_GB"] = (df["DISCO_USADO"].astype(float) / (1024 ** 3)).round(2)
    df["LATENCIA"]       = df["LATENCIA"].astype(float).round(2)

 
    df["PROCESSO1_RAM_GB"] = (df["PORCENTAGEM_PROCESSO1_RAM"].astype(float) / (1024 ** 3)).round(2)
    df["PROCESSO2_RAM_GB"] = (df["PORCENTAGEM_PROCESSO2_RAM"].astype(float) / (1024 ** 3)).round(2)
    df["PROCESSO3_RAM_GB"] = (df["PORCENTAGEM_PROCESSO3_RAM"].astype(float) / (1024 ** 3)).round(2)

    df["PROCESSO1_RAM_PERC"] = ((df["PROCESSO1_RAM_GB"] * 100) / df["RAM_TOTAL_GB"]).where(df["RAM_TOTAL_GB"] > 0, 0.0).round(2)
    df["PROCESSO2_RAM_PERC"] = ((df["PROCESSO2_RAM_GB"] * 100) / df["RAM_TOTAL_GB"]).where(df["RAM_TOTAL_GB"] > 0, 0.0).round(2)
    df["PROCESSO3_RAM_PERC"] = ((df["PROCESSO3_RAM_GB"] * 100) / df["RAM_TOTAL_GB"]).where(df["RAM_TOTAL_GB"] > 0, 0.0).round(2)

    # Tratamento do BOOTTIME 
    df["BOOTTIME_DT"]     = df["BOOTTIME"].astype(float).apply(datetime.fromtimestamp)
    df["UPTIME"]          = (df["DATA_HORA"] - df["BOOTTIME_DT"]).astype(str)
    df["HORA_TRATAMENTO"] = str(datetime.now())
    df["DIA_SEMANA"]      = df["DATA_HORA"].dt.day_name()

    if "RAM_PERCENT" not in df.columns: df["RAM_PERCENT"] = ((df["RAM_USADA"] / df["RAM_TOTAL"]) * 100).round(2)
    if "DISCO_PERCENT" not in df.columns: df["DISCO_PERCENT"] = ((df["DISCO_USADO"] / df["DISCO_TOTAL"]) * 100).round(2)

    # Filtragem Final
    colunas_disponiveis = [col for col in mapeamento_colunas.keys() if col in df.columns]
    df_filtrado = df[colunas_disponiveis].rename(columns=mapeamento_colunas)


    try:
        print(f"Baixando arquivo mestre: {chave_destino_mestre}")
        s3.download_file(bucket, chave_destino_mestre, caminho_local_mestre)
        
        df_filtrado.to_csv(caminho_local_mestre, mode='a', header=False, index=False, sep=";", encoding="utf-8-sig")
        print("Linha adicionada ao mestre existente.")
        
    except s3.exceptions.ClientError as e:
        
        if e.response['Error']['Code'] == "404":
            print("Arquivo mestre inexistente. Será criado o primeiro registro.")
            df_filtrado.to_csv(caminho_local_mestre, mode='w', header=True, index=False, sep=";", encoding="utf-8-sig")
        else:
            print(f"Erro inesperado ao buscar mestre: {e}")
            raise e

    
    print(f"Efetuando upload para o S3 destino: {chave_destino_mestre}")
    s3.upload_file(caminho_local_mestre, bucket, chave_destino_mestre)

    return {
        "mensagem": f"Dados tratados com sucesso! 🟩 ",
        "bucket": bucket,
        "chave": chave_destino_mestre
    }