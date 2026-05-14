import csv
import json
import mysql.connector
import boto3
from urllib.parse import unquote_plus
from datetime import datetime, timedelta

s3 = boto3.client('s3')

def lambda_handler(event, context):

    TrustedCsv(event, context)

    # TrustedJson(event, context)

    return {
        "statusCode": 200,
        "body": "GG"
    }

def TrustedCsv(event, context):
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    prefixo = "raw/"

    resposta = s3.list_objects_v2(Bucket=bucket, Prefix=prefixo)

    if "Contents" not in resposta:
        return {
            "statusCode": 400,
            "body": "Nenhum arquivo encontrado em raw/"
        }

    linhas = []

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

    limite_tempo7 = datetime.now() - timedelta(days=7)

    for objeto in resposta["Contents"]:
        key = unquote_plus(objeto["Key"])

        if key.endswith("/") or objeto["Size"] == 0:
            continue

        nome_arquivo = key.split("/")[-1]
        caminho_local = f"/tmp/{nome_arquivo}"

        s3.download_file(bucket, key, caminho_local)

        with open(caminho_local, "r", encoding="utf-8") as entrada:
            leitor = csv.DictReader(entrada, delimiter=";")

            for linha in leitor:
                data_hora = datetime.fromisoformat(linha["DATA_HORA"])

                linha["RAM_TOTAL_GB"] = round(float(linha["RAM_TOTAL"]) / (1024 ** 3), 2)
                linha["RAM_USADA_GB"] = round(float(linha["RAM_USADA"]) / (1024 ** 3), 2)
                linha["DISCO_TOTAL_GB"] = round(float(linha["DISCO_TOTAL"]) / (1024 ** 3), 2)
                linha["DISCO_USADO_GB"] = round(float(linha["DISCO_USADO"]) / (1024 ** 3), 2)
                linha["LATENCIA"] = round(float(linha["LATENCIA"]), 2)

                linha["PROCESSO1_RAM_GB"] = round(float(linha["PORCENTAGEM_PROCESSO1_RAM"]) / (1024 ** 3), 2)
                linha["PROCESSO2_RAM_GB"] = round(float(linha["PORCENTAGEM_PROCESSO2_RAM"]) / (1024 ** 3), 2)
                linha["PROCESSO3_RAM_GB"] = round(float(linha["PORCENTAGEM_PROCESSO3_RAM"]) / (1024 ** 3), 2)

                linha["PROCESSO1_RAM_PERC"] = round((float(linha["PROCESSO1_RAM_GB"]) * 100) / float(linha["RAM_TOTAL_GB"]), 2)
                linha["PROCESSO2_RAM_PERC"] = round((float(linha["PROCESSO2_RAM_GB"]) * 100) / float(linha["RAM_TOTAL_GB"]), 2)
                linha["PROCESSO3_RAM_PERC"] = round((float(linha["PROCESSO3_RAM_GB"]) * 100) / float(linha["RAM_TOTAL_GB"]), 2)

                linha["BOOTTIME_DT"] = datetime.fromtimestamp(float(linha["BOOTTIME"]))
                linha["UPTIME"] = str(data_hora - linha["BOOTTIME_DT"])
                linha["HORA_TRATAMENTO"] = str(datetime.now())

                linha_final = {}

                for coluna_antiga, coluna_nova in colunas_finais.items():
                    linha_final[coluna_nova] = linha[coluna_antiga]

                if data_hora >= limite_tempo7:
                    linhas.append(linha_final)

    if len(linhas) == 0:
        return {
            "statusCode": 400,
            "body": "Nenhum CSV válido foi processado"
        }

    colunas = list(colunas_finais.values())

    caminho_tratado = "/tmp/dados_tratados.csv"

    with open(caminho_tratado, "w", encoding="utf-8", newline="") as saida:
        escritor = csv.DictWriter(saida, fieldnames=colunas, delimiter=";")
        escritor.writeheader()
        escritor.writerows(linhas)

    s3.upload_file(
        caminho_tratado,
        bucket,
        "trusted/dados_tratados.csv"
    )

    client()

    return {
        "statusCode": 200,
        "body": f"CSV Tratado. Total: {len(linhas)} linhas"
    }

def client():
    conexao = mysql.connector.connect(
        host="172.31.38.56",
        user="root",
        password="urubu100",
        database="smartdata"
    )

    cursor = conexao.cursor(dictionary=True)
    query = """
        SELECT zona.*, empresa.razaoSocial FROM empresa 
        JOIN regiao ON idEmpresa = fkRegiaoEmpresa
        JOIN datacenter ON idDataCenter = fkRegiaoDataCenter 
        JOIN zona ON fkDataCenter = idDataCenter;
        """
    
    cursor.execute(query)
    zonas = cursor.fetchall()

    zonas_json = json.dumps(
        zonas,
        ensure_ascii=False,
        indent=4,
        default=str
    )

    caminho_json = "/tmp/zonas.json"

    with open(caminho_json, "w", encoding="utf-8") as arquivo:
        arquivo.write(zonas_json)
    
    s3.upload_file(
        caminho_json,
        "smart-data-teste-samuel",
        "client/zonas.json"
    )

    cursor.close()
    conexao.close()

