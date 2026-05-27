import json
import boto3
import pandas as pd
import io
import numpy as np
from urllib.parse import unquote_plus
from datetime import datetime


s3 = boto3.client('s3')

def lambda_handler(event, context):
    print("Lambda dashboard financiera Iniciada! 💵")
    try:
        registro = event["Records"][0]["s3"]
        key = unquote_plus(registro["object"]["key"])
        bucket = registro["bucket"]["name"]
        if key.lower().endswith(".json"):
            return {"statusCode": 200, "body": f"Arquivo JSON ignorado: {key}"}
        
     
        resultado = dashFinanceiro(event, context)
        
        if isinstance(resultado, dict) in resultado:
            chave_destino_json = "client/dashboard_financeiro.json"
            print(f"Salvando o Dashboard Financeiro em: {chave_destino_json}")
            s3.put_object(
                Bucket=bucket,
                Key=chave_destino_json,
                Body=json.dumps(resultado, ensure_ascii=False, indent=4),
                ContentType='application/json'
            )

            print(f"Sucesso: {resultado['mensagem']} 🟩")
            return {
                "statusCode": 200,
                "body": json.dumps(resultado)
            }
        else:
            print(f"Aviso: {resultado}")
            return {"statusCode": 200, "body": str(resultado)}

    except Exception as e:
        print(f"ERRO CRÍTICO NA LAMBDA 🟥: {str(e)}")
        import traceback
        traceback.print_exc() 
        return {
            "statusCode": 500,
            "body": f"Erro fatal no pipeline: {str(e)}"
        }



# *********************************************
#  REGRESSÃO LINEAR: TEMPO -> VARIÁVEL
# *********************************************

#formula equanção reduzida da reta
#R: Descobrir o COeficiente Angular e Linear
def regressaoLinear(y):
    """Calcula a regressão linear baseada em passos de tempo sequenciais (x = 1, 2, 3...)."""
    n = len(y)
    if n < 2: return None
    
    x = list(range(1, n + 1))
    mean_x = sum(x) / n
    mean_y = sum(y) / n

    num = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n))
    den = sum((x[i] - mean_x) ** 2 for i in range(n))
    
    if den == 0: return None

    slope = num / den
    intercept = mean_y - slope * mean_x
    sse = sum((y[i] - (slope * x[i] + intercept)) ** 2 for i in range(n))
    se = (sse / (n - 2)) ** 0.5 if n > 2 else 0.0

    return {"CoeficienteAngular": slope, "CoeficienteLinear": intercept, "MargemErro": se}

#Formula Função de Hipótese da Regressão Linear 
#R: Descobri o valor de Y quando X for alguma coisa (Prever the future)
def forecastLinear(modelo, x):
    """Projeta o valor no ponto x fornecido."""
    return round(modelo["CoeficienteAngular"] * x + modelo["CoeficienteLinear"], 2)

#Formula Coeficiente de Determinação
#R: Descobrir o R²
def calcularR2(x, y, modelo):
    """Coeficiente de determinação R² relacionando Tempo (X) e Y."""
    n = len(y)
    if n == 0 or modelo is None: return None
    mean_y = sum(y) / n
    ss_tot = sum((yi - mean_y) ** 2 for yi in y)
    ss_res = sum((y[i] - forecastLinear(modelo, x[i])) ** 2 for i in range(n))
    return round(1 - ss_res / ss_tot, 4) if ss_tot else 1.0

#Formula Erro medio absoluto
def calcularMAE(x, y, modelo):
    """Erro Médio Absoluto."""
    if not modelo or not y: return None
    erros = [abs(y[i] - forecastLinear(modelo, x[i])) for i in range(len(y))]
    return round(sum(erros) / len(erros), 2)

# *********************************************
#  FUNÇÃO PRINCIPAL: dashFinanceiro

def dashFinanceiro(event, context):
    # Extrair informações do S3
    registro = event["Records"][0]["s3"]
    bucket = registro["bucket"]["name"]
    key = unquote_plus(registro["object"]["key"])
    
    print(f"Baixando dados tratados de: {key}")
    
    # Ler o arquivo CSV direto da memória 
    resposta = s3.get_object(Bucket=bucket, Key=key)
    conteudo = resposta['Body'].read().decode('utf-8-sig')
    df = pd.read_csv(io.StringIO(conteudo), delimiter=";")
    
    if df is None or len(df) == 0:
        return "ERRO: Sem dados para processar"

    df.columns = df.columns.str.strip()
    colunas_numericas = ['CPU_PER', 'RAM_PER', 'DISCO_PER', 'LATENCIA',
                         'PACOTES_ENV', 'PACOTES_RCB', 'PACOTES_PER', 'JOGADORES_ATIVOS']
    
    # Convertendo colunas para numérico por segurança
    for coluna in colunas_numericas:
        if coluna in df.columns:
            df[coluna] = pd.to_numeric(df[coluna], errors='coerce').fillna(0)

    df['DATE'] = pd.to_datetime(df['DATE'])
    df['MES'] = df['DATE'].dt.to_period('M').astype(str)
    df['DATE_5MIN'] = df['DATE'].dt.floor('5min')


    # ── RECEITA ~~~~~~~~~~~~~~~~~~
    RECEITA_POR_JOGADOR_5MIN = 0.15
    INTERVALOS_MES = 30 * 24 * 12

    efetivos = df['JOGADORES_ATIVOS'].astype(float)
    fator_latencia = np.select(
        [
            df['LATENCIA'] > 500,
            df['LATENCIA'] > 200,
            df['LATENCIA'] > 100,
        ], [0.75, 0.88, 0.95],
        default=1.0
    )

    fator_cpu = np.select(
        [
            df['CPU_PER'] > 90,
            df['CPU_PER'] > 85,
            df['CPU_PER'] < 15,
        ], [0.88, 0.95, 0.98],
        default=1.0
    )

    fator_ram = np.select(
        [
            df['RAM_PER'] > 95,
            df['RAM_PER'] > 85,
        ], [0.85, 0.93],
        default=1.0
    )

    fator_disco = np.select(
        [
            df['DISCO_PER'] > 95,
            df['DISCO_PER'] > 85,
        ], [0.90, 0.96],
        default=1.0
    )

    df['RECEITA_5MIN'] = (
        efetivos
        * fator_latencia
        * fator_cpu
        * fator_ram
        * fator_disco
        * RECEITA_POR_JOGADOR_5MIN
    ).round(2)

    # ── CUSTO ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    cpu = df['CPU_PER']   / 100
    ram = df['RAM_PER']   / 100
    disco = df['DISCO_PER'] / 100
    PESO_CPU_ENERGIA   = 0.75
    PESO_RAM_ENERGIA   = 0.35
    PESO_DISCO_ENERGIA = 0.20


        #CUSTO VARIAVEL 
    #ENERGIA
    POTENCIA_MIN_W     = 500
    POTENCIA_MAX_W     = 1500
    TARIFA_KWH         = 0.90
    MARGEM_ORCAMENTO = 0.05 

    fator_carga = (cpu * PESO_CPU_ENERGIA) + (ram * PESO_RAM_ENERGIA) + (disco * PESO_DISCO_ENERGIA)
    potencia_w = POTENCIA_MIN_W + (POTENCIA_MAX_W - POTENCIA_MIN_W) * fator_carga
    energia_kwh = (potencia_w / 1000) * 5 / 60
    
    #REDE
    CUSTO_BANDA_POR_PACOTE = 0.0035 
    custo_rede = (df['PACOTES_ENV'] + df['PACOTES_RCB']) * CUSTO_BANDA_POR_PACOTE
    

    df['CUSTO_VAR_5MIN'] = (energia_kwh * TARIFA_KWH + custo_rede)

    # ── UNIFICANDO ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    df_financeiro = (
        df.groupby('DATE_5MIN')
        .agg(
            MES            = ('MES',          'first'),
            EMPRESA        = ('EMPRESA',       'first'),
            REGIAO         = ('REGIAO',        'first'),
            DATACENTER     = ('DATACENTER',    'first'),
            ZONA           = ('ZONA',          'first'),
            QTD_SERVIDORES = ('SERVIDOR',      'nunique'),
            CUSTO_VAR_5MIN = ('CUSTO_VAR_5MIN', 'sum'), 
            RECEITA_5MIN   = ('RECEITA_5MIN',  'sum'),
            JOGADORES_SIM  = ('JOGADORES_ATIVOS', 'sum')
        )
        .reset_index()
    )
    # ── FINALIZNADO CUSTO ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    
        #CUSTO FIXO
    CUSTO_GLOBAL_5MIN = (4500.00 + 5000.00) / INTERVALOS_MES  # Licença e DevOps 
    CUSTO_HW_5MIN     = 4500.00 / INTERVALOS_MES              # Hardware (Pago por Servidor ligado)
    
    df_financeiro['CUSTO_5MIN'] = ( CUSTO_GLOBAL_5MIN + (df_financeiro['QTD_SERVIDORES'] * CUSTO_HW_5MIN) + df_financeiro['CUSTO_VAR_5MIN']
    ).round(2)

    

    # ── LUCRO ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    df_financeiro['LUCRO_5MIN'] = (
        df_financeiro['RECEITA_5MIN'] - df_financeiro['CUSTO_5MIN']
    ).round(2)

    df_financeiro = df_financeiro.sort_values('DATE_5MIN').reset_index(drop=True)

    # ── KPIs mês corrente ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    mes_corrente = df_financeiro['MES'].max()
    df_mes_corr = df_financeiro[df_financeiro['MES'] == mes_corrente]
    receita_corrente = round(df_mes_corr['RECEITA_5MIN'].sum(), 2)
    custo_corrente = round(df_mes_corr['CUSTO_5MIN'].sum(),   2)
    lucro_corrente = round(receita_corrente - custo_corrente, 2)
    roi_corrente = round(((receita_corrente - custo_corrente) / custo_corrente) * 100, 2) if custo_corrente else 0.0

    # ── KPIs mês anterior ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    mes_anterior = (pd.Period(mes_corrente, 'M') - 1).strftime('%Y-%m')
    df_mes_ant = df_financeiro[df_financeiro['MES'] == mes_anterior]
    receita_anterior = round(df_mes_ant['RECEITA_5MIN'].sum(), 2) if not df_mes_ant.empty else 0.0
    custo_anterior = round(df_mes_ant['CUSTO_5MIN'].sum(),   2) if not df_mes_ant.empty else 0.0
    roi_anterior = round(((receita_anterior - custo_anterior) / custo_anterior) * 100, 2) if custo_anterior else 0.0

    # ── Deltas ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    delta_custo = round(custo_corrente   - custo_anterior,   2)
    delta_receita = round(receita_corrente - receita_anterior, 2)
    delta_roi = round(roi_corrente     - roi_anterior,     2)

    #*************************************************************
    #   REGRESSÃO TEMPORAL 
    #*************************************************************
    historico_mensal = (
        df_financeiro.groupby('MES')
        .agg(
            CUSTO_MES = ('CUSTO_5MIN',    'sum'),
            RECEITA_MES = ('RECEITA_5MIN',  'sum'),
            MEDIA_JOGADORES = ('JOGADORES_SIM', 'mean'),
            QTD_INTERVALOS = ('MES', 'count') 
        )
        .reset_index()
        .sort_values('MES')  
    )

    print(historico_mensal.head())
    # Normalizador: Projeta meses quebrados (como o primeiro de 6 dias) para 30 dias - PRO-RATA
    INTERVALOS_PADRAO = 30 * 24 * 12 # 8640 intervalos de 5 min
    historico_mensal['CUSTO_MES'] = historico_mensal['CUSTO_MES'] * (INTERVALOS_PADRAO / historico_mensal['QTD_INTERVALOS'])
    historico_mensal['RECEITA_MES'] = historico_mensal['RECEITA_MES'] * (INTERVALOS_PADRAO / historico_mensal['QTD_INTERVALOS'])

    treino = historico_mensal[historico_mensal['MES'] < mes_corrente]

    lista_custo = treino['CUSTO_MES'].tolist()
    lista_receita = treino['RECEITA_MES'].tolist()
    n_meses = len(lista_custo)

    # Regressão Temporal: Tempo -> Custo / Receita
    modelo_custo = regressaoLinear(lista_custo)    # x = 1,2,3... meses
    modelo_receita = regressaoLinear(lista_receita)  # x = 1,2,3... meses

    # Métricas de qualidade
    xs = list(range(1, n_meses + 1))
    r2_custo  = calcularR2(xs,  lista_custo,   modelo_custo)
    mae_custo = calcularMAE(xs, lista_custo,   modelo_custo)
    r2_rec    = calcularR2(xs,  lista_receita, modelo_receita)

    # Projeção do próximo mês (x = n_meses + 1)
    x_prox = n_meses + 1
    custo_previsto = forecastLinear(modelo_custo,   x_prox) if modelo_custo else None
    receita_prevista = forecastLinear(modelo_receita, x_prox) if modelo_receita else None
    #Intervalo de Confiança de 95%
    ic_95 = round(1.96 * modelo_custo["MargemErro"], 2) if modelo_custo else None

    roi_previsto = None
    if custo_previsto and receita_prevista and custo_previsto > 0:
        roi_previsto = round(((receita_prevista - custo_previsto) / custo_previsto) * 100, 2)

    print("ROI : ", roi_corrente)
    print("R² Custo : ", r2_custo)
    print("R² Receita : ", r2_rec)

    return {
        "KPIS": {
            "ROI": {
                "ROI_MES_CORRENTE": roi_corrente,
                "MARGEM_LIQUIDO":   lucro_corrente,
                "DELTA_ROI":        delta_roi
            },
            "FATURAMENTO_TOTAL": {
                "FATURAMENTO":       receita_corrente,
                "DELTA_FATURAMENTO": delta_receita
            },
            "CUSTO_TOTAL": {
                "CUSTO":       custo_corrente,
                "DELTA_CUSTO": delta_custo
            },
            "ORCAMENTO": {
                "CUSTO_PREVISTO":  custo_previsto,
                "CUSTO_CORRENTE":        custo_corrente,
            },
            "CUSTO_PREVISTO": {
                "CUSTO_PREVISTO":   custo_previsto,
                "RECEITA_PREVISTA": receita_prevista,
                "ROI_PREVISTO":     roi_previsto,
                "IC_95":            ic_95
            }
        },
        "MODELO": {
            "N_MESES_HISTORICO": n_meses,
            "R2_CUSTO":          r2_custo,
            "MAE_CUSTO":         mae_custo,
            "R2_RECEITA":        r2_rec,
            "COEFI_ANGULAR_CUSTO":       round(modelo_custo["CoeficienteAngular"],   2) if modelo_custo   else None,
            "COEFI_ANGULAR_RECEITA":     round(modelo_receita["CoeficienteAngular"], 2) if modelo_receita else None,
        },
        "HISTORICO_MENSAL": [
            {
                "mes":     row["MES"],
                "custo":   round(row["CUSTO_MES"],   2),
                "receita": round(row["RECEITA_MES"], 2),
                "roi":     round(((row["RECEITA_MES"] - row["CUSTO_MES"]) / row["CUSTO_MES"]) * 100, 2)
                           if row["CUSTO_MES"] > 0 else 0.0
            }
            for _, row in historico_mensal.iterrows()
        ],
        "total_dados": len(df)
    }


