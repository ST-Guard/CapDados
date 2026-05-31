import csv
import json
import boto3
from urllib.parse import unquote_plus
from datetime import datetime, timedelta
import pandas as pd
import requests
import io

s3 = boto3.client('s3')

def lambda_handler(event, context):
    print("Iniciando o client para a dashboard  analista 📀")
    try:
        registro = event["Records"][0]["s3"]
        key = unquote_plus(registro["object"]["key"])
        bucket = registro["bucket"]["name"]
        if key.lower().endswith(".json"):
            return {"statusCode": 200, "body": f"Arquivo JSON ignorado: {key}"}
        
        
        resultado = dashAnalista(event, context)
        
        if isinstance(resultado, dict) and "KPIS" in resultado:
            chave_destino_json = "client/dashboard_analista.json"
            print(f"Salvando o Dashboard Financeiro em: {chave_destino_json}")
            s3.put_object(
                Bucket=bucket,
                Key=chave_destino_json,
                Body=json.dumps(resultado, ensure_ascii=False, indent=4),
                ContentType='application/json'
            )

            print(f"Sucesso: Dashboard Analista gerado e gravado no S3! 🟩")
            return {
                "statusCode": 200,
                "body": json.dumps({"status": "Sucesso", "chave": chave_destino_json})
            }
        else:
            print(f"Aviso 🟥: {resultado}")
            return {"statusCode": 200, "body": str(resultado)}

    except Exception as e:
        print(f"ERRO CRÍTICO NA LAMBDA 🟥: {str(e)}")
        import traceback
        traceback.print_exc() 
        return {
            "statusCode": 500,
            "body": f"Erro fatal no pipeline: {str(e)}"
        }










def dashAnalista(event, context):
        registro = event["Records"][0]["s3"]
        bucket = registro["bucket"]["name"]
        key = unquote_plus(registro["object"]["key"])
            

        print("Baixando dados")

         # Ler o arquivo CSV direto da memória 
        resposta = s3.get_object(Bucket=bucket, Key=key)
        conteudo = resposta['Body'].read().decode('utf-8-sig')
        df = pd.read_csv(io.StringIO(conteudo), delimiter=";")

        df = pd.DataFrame(df)


        json_analista = {}
    

        if df.empty:
            print("⚠️ DataFrame vazio")
            return {
                "tipo": "analista",
                "total_dados": 0,
                "datacenters": {}
            }

        df["DATE"] = pd.to_datetime(df["DATE"])





        empresas =  df.groupby(["EMPRESA"])


        print(type(empresas))



    


        for empresa in empresas:
            nome_empresa = empresa[0][0]
            
            print("📠 Estou tratando o client da empresa: ", nome_empresa)


            json_analista[nome_empresa] = {}

            df_empresa = df[df["EMPRESA"] == nome_empresa]

            datacenters = df_empresa.groupby(["DATACENTER"])

            for datacenter in datacenters:

                nome_datacenter = datacenter[0][0]
                print("Estou tratando os dados do datacenter: ", nome_datacenter)

                df_datacenter = df_empresa[df_empresa["DATACENTER"] == nome_datacenter]

                zonas = df_datacenter.groupby(["ZONA"])

                jogadores_data_center = round((df_datacenter["JOGADORES_ATIVOS"].iloc[-1] * 0.04) / 3)
                json_analista[nome_empresa][nome_datacenter] = {
                    "TOTAL_JOGADORES_DATACENTER": jogadores_data_center
                }




                print("nos temos o total de  quantidade de zonas igual a: ", len(zonas))




                for zona in zonas:

                    nome_zona = zona[0][0]
                    print("🗻 Estou tratando os dados da zona: ", nome_zona)


                    df_zonas = df_datacenter[df_datacenter["ZONA"] == nome_zona]

                    jogadores_zona = round(jogadores_data_center / 3)


                    hoje = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                    ontem = hoje - timedelta(days=1)
                    limite_tempo7 = datetime.now() - timedelta(days=7)


            
                    limite_tempo7 = pd.Timestamp.now() - timedelta(days=14)
                    semana_anterior = limite_tempo7 + timedelta(days=7)
                    df_filtrado7DA = df_zonas[(df_zonas['DATE'] >= limite_tempo7) & (df_zonas['DATE'] <= semana_anterior)].copy()


                    joadores_anteior = 0
                    if df_filtrado7DA.empty:
                            joadores_anteior = jogadores_zona * 0.90
                    else:
                            joadores_anteior = round((df_filtrado7DA["JOGADORES_ATIVOS"].ilog[-1] * 0.04  / 3) / 3)

                    

                    dia_semana = datetime.now().strftime("%A") 

                    servidores = df_zonas.groupby(["SERVIDOR"])

                    qtd_sobrecarregados = 0
                    qtd_alta_latencia = 0
                    p99_latencia = round(df_zonas["LATENCIA"].quantile(0.99), 2)
                    total_disco = round(df_zonas["DISCO_USADO"].sum() /( 1024 ** 4), 2)


                    print("DADOS DA ZONA")
                    print("QUANTIDADE DE COMPUTADORES SOBRECARREGADOS: ", qtd_sobrecarregados)
                    print("QUANtIDADE DE COMPUTADOrES COM ALTA LAtENCIA: ", qtd_alta_latencia)
                    print("P99 DA LATENCIA: ", p99_latencia)
                    print("TOTAL DO DISCO DOS SERVIDORES: ", total_disco)





                    

                    servidores_json = {}




 

                    for servidor in servidores:
                        nome_servidor = servidor[0][0]




                        df_servidor = df_zonas[df_zonas["SERVIDOR"] == nome_servidor]

                        ultima_ram = round(df_servidor["RAM_PER"].iloc[-1], 2)
                        ultima_cpu = round(df_servidor["CPU_PER"].iloc[-1], 2 )
                        ultima_disco = round(df_servidor["DISCO_PER"].iloc[-1],2)

                        ultima_latencia = df_servidor["LATENCIA"].iloc[-1]

                        if(ultima_ram > 70 or ultima_cpu > 70 or ultima_disco > 70):
                            qtd_sobrecarregados = qtd_sobrecarregados + 1

                        if (ultima_latencia > 110):
                            qtd_alta_latencia = qtd_alta_latencia + 1


                        score_servidor = 0

                        if (ultima_ram > 70):
                            score_servidor = score_servidor + 50 
                        
                        if (ultima_cpu > 70):
                            score_servidor = score_servidor + 50

                        if (ultima_disco > 80):
                            score_servidor = score_servidor + 80


                        



                        servidores_json[nome_servidor] = {
                            "NOME_SERVIDOR": nome_servidor,
                            "NOME_ZONA": nome_zona,
                            "CPU": ultima_cpu,
                            "RAM": ultima_ram,
                            "DISCO": ultima_disco,
                            "SCORE_SERVIDOR": score_servidor
                        }


  
                        print("🛄 este é o servidor: ", nome_servidor, " Tem a quantidade de dados igual a: ", len(df_servidor))



                    

                         

                    json_analista[nome_empresa][nome_datacenter][nome_zona] = {

                        
                        "QUANTIDADE_SOBRECAREGADOS": qtd_sobrecarregados,
                        "QUANTIDADE_ALTA_LATENCIA": qtd_alta_latencia,
                        "P99_LATENCIA": p99_latencia,
                        "TOTAL_DISCO": total_disco,
                        "JOGADORES_ZONA": jogadores_zona,
                        "JOGADORES_SEMANA_ANTERIOR": joadores_anteior,
                        "DIA_SEMANA": dia_semana,
                        "servidores_zona": servidores_json
                        }



                    print("DADOS DA ZONA")
                    print("QUANTIDADE DE COMPUTADORES SOBRECARREGADOS: ", qtd_sobrecarregados)
                    print("QUANtIDADE DE COMPUTADOrES COM ALTA LAtENCIA: ", qtd_alta_latencia)
                    print("P99 DA LATENCIA: ", p99_latencia)
                    print("TOTAL DO DISCO DOS SERVIDORES: ", total_disco)




        return {"KPIS": json_analista}


 
                   


        





