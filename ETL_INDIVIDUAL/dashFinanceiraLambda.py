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
        if key != "trusted/dados_tratados.csv":
            print(f"Ignorando arquivo que não é o trusted principal: {key}")
            return {"statusCode": 200, "body": f"Arquivo ignorado: {key}"}
        
        
        resultado = dashFinanceiro(event, context)
        
        if isinstance(resultado, dict) and "KPIS" in resultado:
            chave_destino_json = "client/dashboard_financeiro.json"
            print(f"Salvando o Dashboard Financeiro em: {chave_destino_json}")
            s3.put_object(
                Bucket=bucket,
                Key=chave_destino_json,
                Body=json.dumps(resultado, ensure_ascii=False, indent=4),
                ContentType='application/json'
            )

            print(f"Sucesso: Dashboard Financeiro gerado e gravado no S3! 🟩")
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
    colunas_financeiras = [
        'DATE', 'EMPRESA', 'REGIAO', 'DATACENTER', 'ZONA', 'SERVIDOR',
        'CPU_PER', 'RAM_PER', 'DISCO_PER', 'LATENCIA',
        'PACOTES_ENV', 'PACOTES_RCB', 'JOGADORES_ATIVOS'
    ]
    df = pd.read_csv(io.StringIO(conteudo), delimiter=";", usecols=colunas_financeiras)
    
    if df is None or len(df) == 0:
        return "ERRO: Sem dados para processar"

    df.columns = df.columns.str.strip()
    colunas_numericas = ['CPU_PER', 'RAM_PER', 'DISCO_PER', 'LATENCIA',
                         'PACOTES_ENV', 'PACOTES_RCB', 'PACOTES_PER', 'JOGADORES_ATIVOS']
    
    # Convertendo colunas para numérico por segurança
    for coluna in colunas_numericas:
        if coluna in df.columns:
            df[coluna] = pd.to_numeric(df[coluna], errors='coerce').fillna(0)

    df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce', format='mixed')
    df = df.dropna(subset=['DATE'])
    
    df['MES'] = df['DATE'].dt.to_period('M').astype(str)
    df['DATE_5MIN'] = df['DATE'].dt.floor('5min')

    df = df.sort_values(['EMPRESA', 'DATACENTER', 'ZONA', 'SERVIDOR', 'DATE'])
    df['PACOTES_ENV_DELTA'] = (
        df.groupby(['EMPRESA', 'DATACENTER', 'ZONA', 'SERVIDOR'])['PACOTES_ENV']
        .diff()
    )

    df['PACOTES_RCB_DELTA'] = (
        df.groupby(['EMPRESA', 'DATACENTER', 'ZONA', 'SERVIDOR'])['PACOTES_RCB']
        .diff()
    )

    df['PACOTES_ENV_DELTA'] = df['PACOTES_ENV_DELTA'].where(df['PACOTES_ENV_DELTA'] >= 0, 0).fillna(0)
    df['PACOTES_RCB_DELTA'] = df['PACOTES_RCB_DELTA'].where(df['PACOTES_RCB_DELTA'] >= 0, 0).fillna(0)


    # ── RECEITA ~~~~~~~~~~~~~~~~~~
    RECEITA_POR_JOGADOR_5MIN = 0.105
    INTERVALOS_MES = 30 * 24 * 12

    efetivos = df['JOGADORES_ATIVOS'].astype(float)
    efetivos = np.where(efetivos > 10000, efetivos / 10000, efetivos)
    df['JOGADORES_ATIVOS_AJUSTADO'] = efetivos
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
    cpu = df['CPU_PER'] / 100
    ram = df['RAM_PER'] / 100
    disco = df['DISCO_PER'] / 100
    PESO_CPU_ENERGIA = 0.75
    PESO_RAM_ENERGIA = 0.35
    PESO_DISCO_ENERGIA = 0.20


        #CUSTO VARIAVEL 
    #ENERGIA
    POTENCIA_MIN_W = 6000
    POTENCIA_MAX_W = 18000
    TARIFA_KWH = 2.20
    MARGEM_ORCAMENTO = 0.05 
    CUSTO_OPERACIONAL_POR_JOGADOR_5MIN = 0.006

    fator_carga = (cpu * PESO_CPU_ENERGIA) + (ram * PESO_RAM_ENERGIA) + (disco * PESO_DISCO_ENERGIA)
    potencia_w = POTENCIA_MIN_W + (POTENCIA_MAX_W - POTENCIA_MIN_W) * fator_carga
    energia_kwh = (potencia_w / 1000) * 5 / 60
    
    #REDE
    CUSTO_BANDA_POR_PACOTE = 0.0035
    custo_rede = (df['PACOTES_ENV_DELTA'] + df['PACOTES_RCB_DELTA']) * CUSTO_BANDA_POR_PACOTE
    custo_jogadores = df['JOGADORES_ATIVOS_AJUSTADO'] * CUSTO_OPERACIONAL_POR_JOGADOR_5MIN
    

    df['CUSTO_VAR_5MIN'] = (energia_kwh * TARIFA_KWH + custo_rede + custo_jogadores)

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
            JOGADORES_SIM  = ('JOGADORES_ATIVOS_AJUSTADO', 'mean')
        )
        .reset_index()
    )
    # ── FINALIZNADO CUSTO ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    
        #CUSTO FIXO
    CUSTO_GLOBAL_5MIN = (20000.00 + 25000.00) / INTERVALOS_MES  # Licença e DevOps 
    CUSTO_HW_5MIN = 45000.00 / INTERVALOS_MES              # Hardware (Pago por Servidor ligado)
    
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
    receita_corrente = float(round(df_mes_corr['RECEITA_5MIN'].sum(), 2))
    custo_corrente   = float(round(df_mes_corr['CUSTO_5MIN'].sum(),   2))
    lucro_corrente   = float(round(receita_corrente - custo_corrente, 2))
    roi_corrente     = float(round(((receita_corrente - custo_corrente) / custo_corrente) * 100, 2)) if custo_corrente else 0.0

    # ── KPIs mês anterior ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    mes_anterior = (pd.Period(mes_corrente, 'M') - 1).strftime('%Y-%m')
    df_mes_ant = df_financeiro[df_financeiro['MES'] == mes_anterior]
    receita_anterior = float(round(df_mes_ant['RECEITA_5MIN'].sum(), 2)) if not df_mes_ant.empty else 0.0
    custo_anterior   = float(round(df_mes_ant['CUSTO_5MIN'].sum(),   2)) if not df_mes_ant.empty else 0.0
    roi_anterior     = float(round(((receita_anterior - custo_anterior) / custo_anterior) * 100, 2)) if custo_anterior else 0.0

    # ── Deltas ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    delta_custo   = float(round(custo_corrente   - custo_anterior,   2))
    delta_receita = float(round(receita_corrente - receita_anterior, 2))
    delta_roi     = float(round(roi_corrente     - roi_anterior,     2))

    #*************************************************************
    #   REGRESSÃO TEMPORAL 

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

   
    # Normalizador: Projeta meses quebrados (como o primeiro de 6 dias) para 30 dias - PRO-RATA
    INTERVALOS_PADRAO = 30 * 24 * 12 # 8640 intervalos de 5 min
    historico_mensal['CUSTO_MES'] = historico_mensal['CUSTO_MES'] * (INTERVALOS_PADRAO / historico_mensal['QTD_INTERVALOS'])
    historico_mensal['RECEITA_MES'] = historico_mensal['RECEITA_MES'] * (INTERVALOS_PADRAO / historico_mensal['QTD_INTERVALOS'])

    treino = historico_mensal[historico_mensal['MES'] < mes_corrente]

    lista_custo = [float(x) for x in treino['CUSTO_MES'].tolist()]
    lista_receita = [float(x) for x in treino['RECEITA_MES'].tolist()]
    n_meses = len(lista_custo)

    # Regressão Temporal: Tempo -> Custo / Receita
    modelo_custo = regressaoLinear(lista_custo)    # x = 1,2,3... meses
    modelo_receita = regressaoLinear(lista_receita)  # x = 1,2,3... meses

    # Métricas de qualidade
    xs = list(range(1, n_meses + 1))
    r2_custo  = float(calcularR2(xs,  lista_custo,   modelo_custo)) if modelo_custo else None
    mae_custo = float(calcularMAE(xs, lista_custo,   modelo_custo)) if modelo_custo else None
    mae_receita = float(calcularMAE(xs, lista_receita, modelo_receita)) if modelo_custo else None
    r2_rec    = float(calcularR2(xs,  lista_receita, modelo_receita)) if modelo_receita else None

    # Projeção do próximo mês (x = n_meses + 1)
    x_prox = n_meses + 1
    custo_previsto = float(forecastLinear(modelo_custo,   x_prox)) if modelo_custo else None
    receita_prevista = float(forecastLinear(modelo_receita, x_prox)) if modelo_receita else None
    #Intervalo de Confiança de 95%
    ic_95 = float(round(1.96 * modelo_custo["MargemErro"], 2)) if modelo_custo else None
    ic_95_receita = float(round(1.96 * modelo_receita["MargemErro"], 2)) if modelo_receita else None
  

    roi_previsto = None
    if custo_previsto and receita_prevista and custo_previsto > 0:
        roi_previsto = float(round(((receita_prevista - custo_previsto) / custo_previsto) * 100, 2))

    print("ROI : ", roi_corrente)
    print("R² Custo : ", r2_custo)
    print("R² Receita : ", r2_rec)


    #HISTORICO 24HRS
    agora = df_financeiro['DATE_5MIN'].max()
    corte_24h = agora - pd.Timedelta(hours=24)
    df_24h = df_financeiro[df_financeiro['DATE_5MIN'] >= corte_24h].copy()

    df_24h['HORA'] = df_24h['DATE_5MIN'].dt.floor('h')

    historico_24h = (
        df_24h.groupby('HORA')
        .agg(
            CUSTO_HORA = ('CUSTO_5MIN', 'sum'),
            RECEITA_HORA = ('RECEITA_5MIN', 'sum'),
            MEDIA_JOGADORES = ('JOGADORES_SIM', 'mean')
        )
        .reset_index()
    )
    resumo_24H = [
        {
            "hora": row["HORA"].strftime('%Y-%m-%d %H:00'),
            "custo": float(round(row["CUSTO_HORA"], 2)),
            "receita": float(round(row["RECEITA_HORA"], 2)),
            "jogadores_media": float(round(row["MEDIA_JOGADORES"], 2))
        }
        for _, row in historico_24h.iterrows()
    ]

    # GRÁFICO DONUT — Distribuição de custos por categoria (mês corrente)
    energia_mes = float(round(df_mes_corr.merge(
        df[['DATE_5MIN', 'SERVIDOR']].assign(
            ENERGIA = (((POTENCIA_MIN_W + (POTENCIA_MAX_W - POTENCIA_MIN_W) *
                ((df['CPU_PER']/100 * PESO_CPU_ENERGIA) + 
                 (df['RAM_PER']/100 * PESO_RAM_ENERGIA) + 
                 (df['DISCO_PER']/100 * PESO_DISCO_ENERGIA))) / 1000) * (5/60) * TARIFA_KWH)
        ), on='DATE_5MIN', how='left')['ENERGIA'].sum(), 2))
    
    intervalos_mes_corr = len(df_mes_corr)
    rede_mes = float(round(custo_corrente - (CUSTO_GLOBAL_5MIN * intervalos_mes_corr) - 
                     (df_mes_corr['QTD_SERVIDORES'].mean() * CUSTO_HW_5MIN * intervalos_mes_corr) - 
                     energia_mes, 2))
    hardware_mes = float(round(df_mes_corr['QTD_SERVIDORES'].mean() * CUSTO_HW_5MIN * intervalos_mes_corr, 2))
    global_mes = float(round(CUSTO_GLOBAL_5MIN * intervalos_mes_corr, 2))
    
    donut_custos = {
        "energia":  energia_mes,
        "rede":     rede_mes,
        "hardware": hardware_mes,
        "fixo_global": global_mes
    }
    
   # GRÁFICO BARRAS Custo por datacenter e zona (mês corrente)
    df_mes_completo = df[df['MES'] == mes_corrente].copy()
    
    df_mes_completo['CUSTO_ENERGIA'] = (
        ((POTENCIA_MIN_W + (POTENCIA_MAX_W - POTENCIA_MIN_W) *
          ((df_mes_completo['CPU_PER']/100 * PESO_CPU_ENERGIA) +
           (df_mes_completo['RAM_PER']/100 * PESO_RAM_ENERGIA) +
           (df_mes_completo['DISCO_PER']/100 * PESO_DISCO_ENERGIA))) / 1000) * (5/60) * TARIFA_KWH
    )
    
    df_mes_completo['CUSTO_VAR'] = df_mes_completo['CUSTO_ENERGIA'] + (df_mes_completo['PACOTES_ENV_DELTA'] + df_mes_completo['PACOTES_RCB_DELTA']) * CUSTO_BANDA_POR_PACOTE + (df_mes_completo['JOGADORES_ATIVOS_AJUSTADO'] * CUSTO_OPERACIONAL_POR_JOGADOR_5MIN)
   
    qtd_servidores_intervalo = df_mes_completo.groupby('DATE_5MIN')['SERVIDOR'].transform('nunique')
    df_mes_completo['CUSTO_TOTAL_LINHA'] = df_mes_completo['CUSTO_VAR'] + CUSTO_HW_5MIN + (CUSTO_GLOBAL_5MIN / qtd_servidores_intervalo)
    
    barras_datacenter = (
        df_mes_completo.groupby(['DATACENTER', 'ZONA'])
        .agg(
            custo=('CUSTO_VAR', 'sum'),                
            custo_total=('CUSTO_TOTAL_LINHA', 'sum'),   
            receita=('RECEITA_5MIN', 'sum')
        )
        .reset_index()
        .assign(
            custo = lambda x: x['custo'].round(2),
            roi = lambda x: np.where(x['custo_total'] > 0, ((x['receita'] - x['custo_total']) / x['custo_total'] * 100).round(2), 0.0)
        )
        .drop(columns=['custo_total', 'receita'])
        .to_dict(orient='records')
    )
    
    # TABELA Top servidores por custo (mês corrente)
    custos_servidor = df_mes_completo.groupby(['SERVIDOR', 'DATACENTER', 'ZONA'])[['CUSTO_VAR', 'CUSTO_ENERGIA']].sum()
    
    top_servidores = (
        custos_servidor
        .reset_index()
        .rename(columns={'SERVIDOR': 'servidor', 'CUSTO_VAR': 'custo', 'CUSTO_ENERGIA': 'custo_energia'})
        .assign(
            custo         = lambda x: x['custo'].round(2),
            custo_energia = lambda x: x['custo_energia'].round(2),
            percentual = lambda x: (x['custo'] / x['custo'].sum() * 100).round(1),
            datacenter = lambda x: x['DATACENTER'].astype(str).str.split('-').str[0],
            zona = lambda x: x['ZONA'],
            status = lambda x: 'ativo'
        )
        .sort_values('custo', ascending=False)
        [['servidor', 'datacenter', 'zona', 'custo', 'custo_energia', 'percentual', 'status']]
        .to_dict(orient='records')
    )
    zonas_agg = df_mes_completo.groupby(['DATACENTER', 'ZONA']).agg(
        servidores  = ('SERVIDOR', 'nunique'),
        custo_total = ('CUSTO_TOTAL_LINHA', 'sum'),
        energia     = ('CUSTO_ENERGIA', 'sum')
    ).reset_index()

    top_zonas = (
        zonas_agg
        .assign(
            datacenter  = lambda x: x['DATACENTER'].astype(str).str.split('-').str[0],
            zona        = lambda x: x['ZONA'],
            custo_total = lambda x: x['custo_total'].round(2),
            energia     = lambda x: x['energia'].round(2),
            fatia_custo = lambda x: x['custo_total'] / x['custo_total'].sum(),
            status      = lambda x: 'ativo'
        )
        .sort_values('custo_total', ascending=False)
        [['zona', 'datacenter', 'servidores', 'custo_total', 'energia', 'status']] 
        .to_dict(orient='records')
    )
    
    # ── CARDS PREDITIVOS ──
    periodo_base = pd.Period(mes_corrente, 'M')
    historico_mensal = historico_mensal.reset_index(drop=True)
    projecoes = []
    for i in range(1, 13):
        x_i = n_meses + i
        periodo_previsto = periodo_base + i
        mes_ano_passado = str(periodo_previsto - 12)
        mes_base = historico_mensal[historico_mensal['MES'] == mes_ano_passado]
        if not mes_base.empty:
            x_base = int(mes_base.index[0]) + 1
            custo_base_real = float(mes_base.iloc[0]['CUSTO_MES'])
            receita_base_real = float(mes_base.iloc[0]['RECEITA_MES'])
            custo_base_regressao = float(forecastLinear(modelo_custo, x_base)) if modelo_custo else custo_base_real
            receita_base_regressao = float(forecastLinear(modelo_receita, x_base)) if modelo_receita else receita_base_real
            ajuste_sazonal_custo = custo_base_real - custo_base_regressao
            ajuste_sazonal_receita = receita_base_real - receita_base_regressao
            ajuste_sazonal_custo = max(-(ic_95 or 0), min(ajuste_sazonal_custo, ic_95 or 0))
            ajuste_sazonal_receita = max(-(ic_95_receita or 0), min(ajuste_sazonal_receita, ic_95_receita or 0))
        else:
            ajuste_sazonal_custo = 0
            ajuste_sazonal_receita = 0
        c_prev_base = float(forecastLinear(modelo_custo,   x_i)) if modelo_custo   else None
        r_prev_base = float(forecastLinear(modelo_receita, x_i)) if modelo_receita else None
        c_prev = float(round(max(0, c_prev_base + ajuste_sazonal_custo), 2)) if c_prev_base is not None else None
        r_prev = float(round(max(0, r_prev_base + ajuste_sazonal_receita), 2)) if r_prev_base is not None else None
        orc = float(round(c_prev * (1 + MARGEM_ORCAMENTO), 2)) if c_prev else None
        roi_p = float(round(((r_prev - c_prev) / c_prev) * 100, 2)) if (c_prev and r_prev and c_prev > 0) else None
        confianca = max(50, round((r2_custo or 0) * 100 * (1 - i * 0.03)))
        projecoes.append({
            "mes": str(periodo_previsto),
            "custo_previsto": c_prev,
            "receita_prevista": r_prev,
            "orcamento": orc,
            "roi_previsto": roi_p,
            "ic_95": ic_95,
            "confianca": confianca,
            "mes_base_sazonal": mes_ano_passado,
            "fluxo_sazonal": "alto" if ajuste_sazonal_receita >= 0 else "baixo",
            "ajuste_sazonal_custo": float(round(ajuste_sazonal_custo, 2)),
            "ajuste_sazonal_receita": float(round(ajuste_sazonal_receita, 2))
        })

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
            "R2_CUSTO":  r2_custo,
            "MAE_CUSTO": mae_custo,
            "MAE_RECEITA": mae_receita,
            "R2_RECEITA": r2_rec,
            "COEFI_ANGULAR_CUSTO": float(round(modelo_custo["CoeficienteAngular"],   2)) if modelo_custo   else None,
            "COEFI_ANGULAR_RECEITA": float(round(modelo_receita["CoeficienteAngular"], 2)) if modelo_receita else None,
        },
        "HISTORICO_MENSAL": [
            {
                "mes": row["MES"],
                "custo":  float(round(row["CUSTO_MES"], 2)),
                "receita": float(round(row["RECEITA_MES"], 2)),
                "roi": float(round(((row["RECEITA_MES"] - row["CUSTO_MES"]) / row["CUSTO_MES"]) * 100, 2))
                           if row["CUSTO_MES"] > 0 else 0.0
            }
            for _, row in historico_mensal.iterrows()
        ],
        "HISTORICO_24HRS":resumo_24H,
        "GRAFICOS": {
            "DONUT_CUSTOS":      donut_custos,
            "BARRAS_DATACENTER": barras_datacenter,
            "TOP_SERVIDORES":    top_servidores,
            "TOP_ZONAS":         top_zonas
        },
        "PROJECOES": projecoes,
        "total_dados": len(df)
    }
