import logging
import boto3
from botocore.exceptions import ClientError
import os

key = os.getenv("AWS_ACCESS_KEY_ID")
secret = os.getenv("AWS_SECRET_ACCESS_KEY")
sessionToken = os.getenv("AWS_SESSION_TOKEN")

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client(
        's3',
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        aws_session_token= sessionToken,
        region_name='us-east-1'
    ) 
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


teste = upload_file(r"C:\SSD\programacao\Sptech\2 semestre\so\testando.txt", "smart-data-teste-samuel", "raw/testando.txt")

if teste:
    print("gg")
else:
    print("mb")



import csv
import json
import boto3
from urllib.parse import unquote_plus

s3 = boto3.client("s3")

def lambda_handler(event, context):
    print(event)

    return rawCsv(event, context)


def rawCsv(event, context):
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = unquote_plus(event["Records"][0]["s3"]["object"]["key"])

    print("BUCKET:", bucket)
    print("KEY:", key)

    if not key.startswith("raw/"):
        return {
            "statusCode": 400,
            "body": "Arquivo não está na pasta raw"
        }

    nome_arquivo = key.split("/")[-1]

    caminho_local = f"/tmp/{nome_arquivo}"
    caminho_tratado = f"/tmp/tratado-{nome_arquivo}"

    s3.download_file(bucket, key, caminho_local)

    linhas = []

    with open(caminho_local, "r", encoding="utf-8") as entrada:
        leitor = csv.DictReader(entrada, delimiter=";")

        for linha in leitor:
            linha["RAM_TOTAL_GB"] = round(float(linha["RAM_TOTAL"]) / (1024 ** 3), 2)
            linha["RAM_USADA_GB"] = round(float(linha["RAM_USADA"]) / (1024 ** 3), 2)
            linha["DISCO_TOTAL_GB"] = round(float(linha["DISCO_TOTAL"]) / (1024 ** 3), 2)
            linha["DISCO_USADO_GB"] = round(float(linha["DISCO_USADO"]) / (1024 ** 3), 2)
            linha["LATENCIA"] = round(float(linha["LATENCIA"]), 2)

            linhas.append(linha)

    if len(linhas) == 0:
        return {
            "statusCode": 400,
            "body": "CSV vazio"
        }

    colunas = list(linhas[0].keys())

    with open(caminho_tratado, "w", encoding="utf-8", newline="") as saida:
        escritor = csv.DictWriter(saida, fieldnames=colunas, delimiter=";")
        escritor.writeheader()
        escritor.writerows(linhas)

    s3.upload_file(
        caminho_tratado,
        bucket,
        f"trusted/{nome_arquivo}"
    )

    return {
        "statusCode": 200,
        "body": f"CSV tratado com sucesso: {key}"
    }