import json
import boto3
import pandas as pd
import io
import numpy as np
import unicodedata
from urllib.parse import unquote_plus
from datetime import datetime, timedelta

s3 = boto3.client('s3')

def lambda_handler(event, context):
    print("Lambda dashboard do analista Iniciada! ")
    try:
        mensagem_sns_texto = event["Records"][0]["Sns"]["Message"]
        evento_s3_real = json.loads(mensagem_sns_texto)
        
        if "Evento" in evento_s3_real and evento_s3_real["Evento"] == "s3:TestEvent":
            print("Evento de teste do S3 recebido e ignorado com sucesso! ✅")
            return {"statusCode": 200, "body": "TestEvent ignorado"}
            
   
        registro = evento_s3_real["Records"][0]["s3"]
        key = unquote_plus(registro["object"]["key"])
        bucket = registro["bucket"]["name"]
        
        if key.lower().endswith(".json"):
            return {"statusCode": 200, "body": f"Arquivo JSON ignorado: {key}"}
            
        if key != "trusted/dados_tratados.csv":
            print(f"Ignorando arquivo que não é o trusted principal: {key}")
            return {"statusCode": 200, "body": f"Arquivo ignorado: {key}"}
        
        
        resultado = dashAnalista(event, context)
        
        if isinstance(resultado, dict) and "KPIS" in resultado:
            chave_destino_json = "client/dashboard_analista.json"
            print(f"Salvando o Dashboard do analista em: {chave_destino_json}")
            s3.put_object(
                Bucket=bucket,
                Key=chave_destino_json,
                Body=json.dumps(resultado, ensure_ascii=False, indent=4),
                ContentType='application/json'
            )

            print(f"Sucesso: Dashboard do Analista gerado e gravado no S3! 🟩")
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


def padronizarNomeZona(valor):
    if valor is None or pd.isna(valor):
        return valor

    textoNormalizado = normalizarTextoEstrutura(
        valor
    )


    mapeamento = {
        "zonaa": "Zona A",
        "zonab": "Zona B",
        "zonac": "Zona C"
    }

    return mapeamento.get(textoNormalizado,str(valor).strip())


def padronizarNomeServidor(valor):
    if valor is None or pd.isna(valor):
        return valor

    textoNormalizado = normalizarTextoEstrutura(valor)

    if textoNormalizado.startswith("servidor"):
        restante = textoNormalizado.replace("servidor", "",1)

        if len(restante) >= 4:
            uf = restante[:2].upper()
            numero = restante[2:]

            if numero.isdigit():
                return (
                    f"SERVIDOR-{uf}-"
                    f"{numero.zfill(2)}"
                )

    return str(valor).strip()

def verificarDuplicidadeServidores(dataframe):
    associacoes = (
        dataframe[
            [
                "EMPRESA",
                "DATACENTER",
                "ZONA",
                "SERVIDOR"
            ]
        ]
        .drop_duplicates()
    )

    quantidadeZonasPorServidor = (
        associacoes
        .groupby(
            [
                "EMPRESA",
                "DATACENTER",
                "SERVIDOR"
            ]
        )["ZONA"]
        .nunique()
    )

    duplicados = quantidadeZonasPorServidor[
        quantidadeZonasPorServidor > 1
    ]

    if duplicados.empty:
        print("✅ Nenhum servidor associado "
            "a mais de uma zona."
        )
        return

    print(
        "⚠️ Servidores associados a mais de uma zona:"
    )

    print(duplicados)


def normalizarTextoEstrutura(valor):
    if valor is None or pd.isna(valor):
        return ""

    texto = str(valor).strip().lower()
    texto = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("ascii")
    return (texto.replace(" ", "").replace("-", "").replace("_", "").replace("\u00a0", ""))






def dashAnalista(event, context):


    
        mensagem_sns_texto = event["Records"][0]["Sns"]["Message"]
        evento_s3_real = json.loads(mensagem_sns_texto)
        registro = evento_s3_real["Records"][0]["s3"]


        key = unquote_plus(registro["object"]["key"])
        bucket = registro["bucket"]["name"]
            

        print("Baixando dados dos servidores")

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

        df["DATE"] = pd.to_datetime(df["DATE"], format='mixed')
        df = df[df["DATACENTER"].str.match(r'^DC-[A-Z]+-\d+$')]

        df["ZONA"] = df["ZONA"].apply(
        padronizarNomeZona
        )

        # Remove diferenças de espaço, caixa e separadores.
        df["SERVIDOR"] = df["SERVIDOR"].apply(
        padronizarNomeServidor
        )

        print(
        "Zonas encontradas após padronização:",
        sorted(
            df["ZONA"]
            .dropna()
            .unique()
            .tolist()
        )
    )

        quantidadeAntes = len(df)

        df = df.drop_duplicates(
        subset=[
            "EMPRESA",
            "REGIAO",
            "DATACENTER",
            "ZONA",
            "SERVIDOR",
            "DATE"
        ],
        keep="last"
    )

        print(
        "Duplicidades removidas:",
        quantidadeAntes - len(df)
    )

        
        

        resp_hist = s3.get_object(
                Bucket=bucket,
                Key="dados_alertas/ultimos_alertas.json"
        )

        historicoAlertas = json.loads(
        resp_hist["Body"].read().decode("utf-8")
        )

        print(f"✅ Histórico carregado: {len(historicoAlertas)} alertas")
            
   
            


        print("DEMONSTANDO O HISTORICO DE ALERTAS")    
        print(historicoAlertas)

        resposta = s3.get_object(
            Bucket=bucket,
            Key="raw/metricas.json"
        )

        conteudo = resposta["Body"].read().decode("utf-8")
        metricasJson = json.loads(conteudo)

        print("✅ metricas.json carregado do S3")
        





        empresas =  df.groupby(["EMPRESA"])


        print(type(empresas))




        for empresa in empresas:
            nome_empresa = empresa[0][0]
            


    
            
            metricas_empresa  = metricasJson[nome_empresa]




        

            historico_alertas_empresa = historicoAlertas[nome_empresa]


            print("📠 Estou tratando o client da empresa: ", nome_empresa)


            json_analista[nome_empresa] = {}

            df_empresa = df[df["EMPRESA"] == nome_empresa]

            datacenters = df_empresa.groupby(["DATACENTER"])


            for datacenter in datacenters:
                nome_datacenter = datacenter[0][0]
                print("ESTOU TRATANDO OS DADOS DO DATACENTER: ", nome_datacenter)
            



                metricas_datacenter = ""

                try: 
                    metricas_datacenter = metricas_empresa[nome_datacenter]
                except:
                    print("esssa empresa não possue métricas")                     
                


                alertas_datacenter = ""
                try: 
                    alertas_datacenter = historico_alertas_empresa[nome_datacenter]
                except:
                     print("essa empresa não possue alertas")

                print("os ultimos alertas do datacenter são: ")
                print(alertas_datacenter)






                df_datacenter = df_empresa[df_empresa["DATACENTER"] == nome_datacenter]

                zonas = df_datacenter.groupby(["ZONA"])

                jogadores_data_center = round(df_datacenter["JOGADORES_ATIVOS"].iloc[-1]  / 3)
                json_analista[nome_empresa][nome_datacenter] = {
                    "TOTAL_JOGADORES_DATACENTER": jogadores_data_center
                }




                print("nos temos o total de  quantidade de zonas igual a: ", len(zonas))




                for zona in zonas:

                    nome_zona = zona[0][0]
                    print("🗻 Estou tratando os dados da zona: ", nome_zona)



                    chaves_zona = nome_zona.replace(" ", "")
                    metricas_zona = ""
                    

                    try:
                        metricas_zona = metricas_datacenter[chaves_zona]

                    except:
                         print("essa zona nao popssui metricas")
                    


                    alertas_zona = None
                    quantidade_servidores = 0
                    quantidade_chamados_aberto = 0
                    mttr_zona = 0




                    try:
                            alertas_zona = alertas_datacenter[chaves_zona]                     
                            quantidade_servidores = alertas_zona["QTD_SERVIDORES"]
                            quantidade_chamados_aberto = alertas_zona["QUANTIDADE_ABERTO"]
                            mttr_zona = alertas_zona["MTTR_ZONA"]

                    except:
                         print("nao possui alertas salvos")






                    


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
                            joadores_anteior = round(df_filtrado7DA["JOGADORES_ATIVOS"].iloc[-1]  / 3)

                    
                    

                    data_hora = df_zonas['DATE'].iloc[-1]

                    dia_semana = datetime.now().strftime("%A") 

                    if dia_semana == "Monday":
                        dia_semana = "Segunda"
                    
                    elif dia_semana == "Tuesday":
                        dia_semana = "Terça"

                    elif dia_semana ==  "Wednesday":
                        dia_semana = "Quarta"

                    elif dia_semana == "Thursday":
                        dia_semana = "Quinta"
                    elif dia_semana == "Friday":
                        dia_semana = "Sexta"
                    elif dia_semana == "Saturday":
                        dia_semana = "Sábado"
                    elif dia_semana == "Sunday":
                        dia_semana = "Domingo"
                    else:
                        print("data invalida")

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



                        limite_ram = 70
                        limite_cpu = 70
                        limite_disco = 70
                        limite_rede = 110



                        metrica_servidor = None

                        try: 
                            metrica_servidor = metricas_zona[nome_servidor]
                            limite_ram = metrica_servidor["limites"]["RAM"]
                            limite_cpu = metrica_servidor["limites"]["CPU"]
                            limite_disco = metrica_servidor["limites"]["DISCO"]
                            limite_rede = metrica_servidor["limites"]["REDE"]
                        
                        except:
                             print("esta zona nao tem metricas")
                      

                        

                        df_servidor = df_zonas[df_zonas["SERVIDOR"] == nome_servidor]

                        ultima_ram = round(df_servidor["RAM_PER"].iloc[-1], 2)
                        ultima_cpu = round(df_servidor["CPU_PER"].iloc[-1], 2 )
                        ultima_disco = round(df_servidor["DISCO_PER"].iloc[-1],2)

                        ultima_latencia = df_servidor["LATENCIA"].iloc[-1]

                        if ultima_ram > (limite_ram - (limite_ram * 0.10))  or ultima_cpu > (limite_cpu - (limite_cpu * 0.10))  or ultima_disco > (limite_disco - (limite_disco * 0.10)):
                            qtd_sobrecarregados = qtd_sobrecarregados + 1



                        score_servidor = 0

                        if (ultima_latencia > limite_rede):
                            qtd_alta_latencia = qtd_alta_latencia + 1
                            score_servidor = score_servidor + 50




                        if (ultima_ram > limite_ram):
                            score_servidor = score_servidor + 50 
                        
                        if (ultima_cpu > limite_cpu):
                            score_servidor = score_servidor + 50

                        if (ultima_disco > limite_disco):
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
                        "QUANTIDADE_SERVIDORES": quantidade_servidores,
                        "MTTR_ZONA": mttr_zona,
                        "QUANTIDADE_ALERTA_ABERTOS": quantidade_chamados_aberto,
                        "QUANTIDADE_SOBRECAREGADOS": qtd_sobrecarregados,
                        "QUANTIDADE_ALTA_LATENCIA": qtd_alta_latencia,
                        "P99_LATENCIA": p99_latencia,
                        "TOTAL_DISCO": total_disco,
                        "JOGADORES_ZONA": round(jogadores_zona),
                        "JOGADORES_SEMANA_ANTERIOR": round(joadores_anteior),
                        "DIA_SEMANA": dia_semana,
                        "servidores_zona": servidores_json,
                        "data-hora": data_hora.strftime('%d/%m/%Y %H:%M:%S')
                        }



                    print("DADOS DA ZONA")
                    print("QUANTIDADE DE COMPUTADORES SOBRECARREGADOS: ", qtd_sobrecarregados)
                    print("QUANtIDADE DE COMPUTADOrES COM ALTA LAtENCIA: ", qtd_alta_latencia)
                    print("P99 DA LATENCIA: ", p99_latencia)
                    print("TOTAL DO DISCO DOS SERVIDORES: ", total_disco)




        return {"KPIS": json_analista}


 
                   


        





