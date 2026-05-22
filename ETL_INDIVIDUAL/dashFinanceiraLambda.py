import csv
import pandas as pd
import requests
import random 
 

# *********************************************
#  CONSTANTES — API STEAM
STEAM_APP_ID = 730
STEAM_API_URL = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={STEAM_APP_ID}"
TOTAL_SERVIDORES_STEAM = 10000   

# *********************************************
#  CONSTANTES — RECEITA

#  Steam fatura ~R$ 105.350/min globalmente
#  Com ~35M jogadores simultâneos no pico: 
#  Com ~10.000 servidores
#  R$ 105.350 / 35.000.000 = R$ 0,003 por jogador
#  R$ 0,003 * 5 = R$ 0.015 por jogador a cada 5 minutos


RECEITA_POR_JOGADOR_5MIN = 0.015 


 
# *********************************************
#  CONSTANTES — CUSTO FIXO
 
CUSTO_LICENCA_MES  =  1200.0   # R$ — SO (Ubuntu Pro) + monitoramento
CUSTO_HARDWARE_MES =  3000.0   # R$ — amortização do hardware 
CUSTO_OPERACAO_MES =  2000.0   # R$ — salário dos funcionários (rateio por servidor)

# Total de intervalos de 5 min em um mês (30 dias × 24h × 12 intervalos/h)
INTERVALOS_MES = 30 * 24 * 12  # = 8.640

# Custo fixo por servidor a cada 5 minutos
CUSTO_FIXO_5MIN = (CUSTO_LICENCA_MES + CUSTO_HARDWARE_MES + CUSTO_OPERACAO_MES) / INTERVALOS_MES




# *********************************************
#  CONSTANTES — CUSTO VARIÁVEL (ENERGIA)
#  Potência estimada com base no uso de CPU, RAM e Disco.
 
POTENCIA_MIN_W = 500   # Watts — servidor ligado sem nenhuma carga
POTENCIA_MAX_W = 1500   # Watts — servidor a 100% de CPU + RAM + Disco
 
TARIFA_KWH = 0.90      # R$/kWh — tarifa comercial media brasileira

# Pesos de cada recurso no consumo de energia
# CPU aquece mais e consome mais, por isso tem o maior peso
PESO_CPU_ENERGIA   = 0.75
PESO_RAM_ENERGIA   = 0.35
PESO_DISCO_ENERGIA = 0.20






# *********************************************
#  FUNÇÃO: buscar jogadores ativos via API Steam
#
#  Em caso de falha retorna fallback baseado na media histórica 

def buscarJogadoresAtivos():
    FALLBACK_JOGADORES = 700.000   

    try:
        resposta = requests.get(STEAM_API_URL, timeout=5)
        dados = resposta.json()
        jogadores = int(dados["response"]["player_count"])
        print(f"API Steam: {jogadores:,} jogadores ativos (AppID {STEAM_APP_ID})")
        return jogadores
    except Exception as e:
        print(f"[AVISO] Falha na API Steam: {e}. Usando fallback: {FALLBACK_JOGADORES:,}")
        return FALLBACK_JOGADORES


 
# *********************************************
#  FUNÇÃO: EstimarReceita
#
#  Calcula a receita gerada em um intervalo de 5 minutos
#  Penalidades reduzem os jogadores "efetivos", aqueles com boa
#  experiência que estão gerando receita no intervalo.
 

 
def EstimarReceita(jogadores, latencia,  cpu, ram, disco):

    efetivos = jogadores


      # Penalidades de latência
    if latencia > 500:
        efetivos *= 0.75    # −25%: falha de serviço
    elif latencia > 200:
        efetivos *= 0.88    # −12%: abandono de sessão
    elif latencia > 100:
        efetivos *= 0.95    # −5%:  lentidão perceptível

    # Penalidades de CPU
    if cpu > 90:
        efetivos *= 0.88    # −12%: risco de queda
    elif cpu > 85:
        efetivos *= 0.95    # −5%:  sobrecarga
    elif cpu < 15:
        efetivos *= 0.98    # −2%:  servidor ocioso

    # Penalidades de RAM
    if ram > 95:
        efetivos *= 0.85    # −15%: sem memoria - crítico
    elif ram > 85:
        efetivos *= 0.93    # −7%:  pressão de memória

    # Penalidades de Disco
    if disco > 95:
        efetivos *= 0.90    # −10%: disco cheio
    elif disco > 85:
        efetivos *= 0.96    # −4%:   Input/Output degradado

    return round(efetivos * RECEITA_POR_JOGADOR_5MIN, 2)
 

 
# *********************************************
#  FUNÇÃO PRINCIPAL: dashFinanceiro

 
def dashFinanceiro(dados):
    if not dados:
        print("ERRO: Sem dados para processar")
        return
 
    df = pd.DataFrame(dados)
    
    # Adicione esta linha aqui para limpar os nomes das colunas
    df.columns = df.columns.str.strip()

    # Converte as colunas numéricas — o CSV pode usar vírgula como decimal
    colunas_numericas = ['CPU_PER', 'RAM_PER', 'DISCO_PER', 'LATENCIA',
                     'PACOTES_ENV', 'PACOTES_RCB', 'PACOTES_PER']

    for coluna in colunas_numericas:
        df[coluna] = (
            df[coluna]
            .astype(str)
            .str.replace(',', '.', regex=False)  # troca vírgula por ponto
            .astype(float)
        )
    
    df['DATE'] = pd.to_datetime(df['DATE'])
    df['MES']  = df['DATE'].dt.to_period('M').astype(str)
    df['DATE_5MIN'] = df['DATE'].dt.floor('5min')

    # ******** Busca jogadores via API Steam
    #
    # ~10k servidores na Steam
    # ~10k -------------------- qtd_jogadores
    # qtd_servidores_nossos --- qtd_jogadores_nossos
    # qtd_jogadores * (qtd_servidores_nossos / ~10k) = jogadores_nossos 
    # (exemplo) 1.200.000 * (10 / 10.000) = 1.200 jogadores 

    jogadores_globais  = buscarJogadoresAtivos()
    qtd_servidores_sim = df['SERVIDOR'].nunique()
    fator_escala       = qtd_servidores_sim / TOTAL_SERVIDORES_STEAM
    jogadores_nossos   = jogadores_globais * fator_escala

    print(f"Servidores simulados : {qtd_servidores_sim}")
    print(f"Fator de escala      : {fator_escala:.6f}  ({qtd_servidores_sim}/{TOTAL_SERVIDORES_STEAM})")
    print(f"Jogadores atribuídos : {jogadores_nossos:,.0f}")

    # *********** RECEITA ******************
    # Média das metricas por intervalo de 5min: EstimarReceita
    medias_por_tempo = (
        df.groupby('DATE_5MIN')[['LATENCIA', 'CPU_PER', 'RAM_PER', 'DISCO_PER']]
        .mean()
        .reset_index()
    )

    medias_por_tempo['RECEITA_5MIN'] = medias_por_tempo.apply(
        lambda row: EstimarReceita(
            jogadores = jogadores_nossos * random.uniform(0.90, 1.10),
            latencia  = row['LATENCIA'],
            cpu       = row['CPU_PER'],
            ram       = row['RAM_PER'],
            disco     = row['DISCO_PER']
        ),
        axis=1
    )
    
 
 
    # *********** CUSTO ******************
    # Calculado individualmente por servidor (energia + fixo)
 
    cpu   = df['CPU_PER']   / 100
    ram   = df['RAM_PER']   / 100
    disco = df['DISCO_PER'] / 100
    
    # Média ponderada da carga 
    fator_carga = ((cpu   * PESO_CPU_ENERGIA)   +(ram   * PESO_RAM_ENERGIA)   +(disco * PESO_DISCO_ENERGIA))
    print("fator de carga:", fator_carga[0])
    # Potência estimada de cada servidor nesse instante
    # 150W   |----[?W]----------| 500W
    # ocioso                     máximo
    potencia_w = POTENCIA_MIN_W + (POTENCIA_MAX_W - POTENCIA_MIN_W) * fator_carga
    
    # Energia consumida no intervalo de 5 min (em kWh)
    # Energia (kWh) = Potência (kW) × Tempo (h)
    energia_kwh = (potencia_w / 1000) * 5/ 60
    

    #Custo Rede
    CUSTO_BANDA_POR_PACOTE = 0.008
    custo_rede_5min = (df['PACOTES_ENV'] + df['PACOTES_RCB']) * CUSTO_BANDA_POR_PACOTE
    
    # Aplica a tarifa e soma o custo fixo
    df['CUSTO_5MIN'] = (energia_kwh * TARIFA_KWH + CUSTO_FIXO_5MIN + custo_rede_5min).round(4)
    
    # Junta a receita calculada de volta no DataFrame principal
    df = df.merge(
        medias_por_tempo[['DATE_5MIN', 'RECEITA_5MIN']],
        on='DATE_5MIN', how='left'
    )

    #MONTANDO UMA dF COM SOMENTE AS COLUNAS QUE ME IMPORTAM
    df_financeiro = (
        df.groupby('DATE_5MIN')
        .agg(
            MES            = ('MES',          'first'),
            EMPRESA        = ('EMPRESA',       'first'),
            REGIAO         = ('REGIAO',        'first'),
            DATACENTER     = ('DATACENTER',    'first'),
            ZONA           = ('ZONA',          'first'),
            QTD_SERVIDORES = ('SERVIDOR',      'count'),
            CUSTO_5MIN     = ('CUSTO_5MIN',    'sum'),    
            RECEITA_5MIN   = ('RECEITA_5MIN',  'first'),  
        )
        .reset_index()
    )

    #Calcula o lucro liquido
    df_financeiro['LUCRO_5MIN']  = (
        df_financeiro['RECEITA_5MIN'] - df_financeiro['CUSTO_5MIN']
    ).round(2)
   
    df_financeiro = df_financeiro.sort_values('DATE_5MIN').reset_index(drop=True)
    print(df_financeiro.head())

    # #Aplica uma % a mais para deixar o custo mais condizente com a receita
    # # A Steam gasta ~65–70% da receita em infraestrutura
    # df_financeiro['CUSTO_5MIN'] = (
    #     df_financeiro['RECEITA_5MIN'] * 0.70
    # ).round(2)


    ################ KPIS ##################


    # ===== MÊS CORRENTE ========
    mes_corrente = df_financeiro['MES'].max()
    df_mes_corr = df_financeiro[df_financeiro['MES'] == mes_corrente]
    receita_corrente = round(df_mes_corr['RECEITA_5MIN'].sum(), 2)
    custo_corrente   = round(df_mes_corr['CUSTO_5MIN'].sum(),   2)
    lucro_corrente   = round(receita_corrente - custo_corrente, 2)
    roi_corrente  = round(((receita_corrente - custo_corrente) / custo_corrente) * 100, 2) 
  
    
    # ===== MÊS ANTERIOR ========
    mes_anterior = ( pd.Period(mes_corrente, 'M') - 1).strftime('%Y-%m')
    df_mes_ant  = df_financeiro[df_financeiro['MES'] == mes_anterior]
    receita_anterior = round(df_mes_ant['RECEITA_5MIN'].sum(), 2) if not df_mes_ant.empty else 0.0
    custo_anterior = round(df_mes_ant['CUSTO_5MIN'].sum(),   2) if not df_mes_ant.empty else 0.0
    roi_anterior  = round(((receita_anterior - custo_anterior) / custo_anterior) * 100, 2) if custo_anterior else 0.0
    
    # ===== DELTAS ========
    delta_custo = round(custo_corrente - custo_anterior,   2)
    delta_receita = round(receita_corrente - receita_anterior, 2)
    delta_roi = round(roi_corrente - roi_anterior,     2)

    
    # ======== REGRESSÂO LINEAR ======




    
    #ROI
    ROI_MES_CORRENTE = roi_corrente
    MARGEM_LIQUIDA = lucro_corrente
    DELTA_ROI = delta_roi
    print("ROI: ")
    print(ROI_MES_CORRENTE)
    #FATURAMENTO_TOTAL
    FATURAMENTO_CORRENTE = receita_corrente
    DELTA_FATURAMENTO = delta_receita

    #CUSTO TOTAL
    CUSTO_CORRENTE = custo_corrente
    DELTA_CUSTO = delta_custo

    #ORÇAMENTO
        #PEGA DO BANCO
    CUSTO_REFERENCIA = custo_corrente
    CUSTO_PREVISTO = 0

    #CUSTO PREVISTO
    CUSTO_PREVISTO = 0


    return {
        "KPIS": {
            "ROI":{
                "ROI_MES_CORRENTE": ROI_MES_CORRENTE,
                "MARGEM_LIQUIDO": MARGEM_LIQUIDA,
                "DELTA_ROI": DELTA_ROI
            },
            "FATURAMENTO_TOTAL": {
                "FATURAMENTO": FATURAMENTO_CORRENTE,
                "DELTA_FATURAMENTO": DELTA_FATURAMENTO
            },
            "CUSTO_TOTAL":{
                "CUSTO": CUSTO_CORRENTE,
                "DELTA_CUSTO": DELTA_CUSTO
            },
            "ORÇAMENTO":{
                "CUSTO_REFERENCIA": CUSTO_REFERENCIA,
                "CUSTO_PREVISTO": CUSTO_PREVISTO
            },
            "CUSTO_PREVISTO":{
                'CUSTO_PREVISTO': CUSTO_PREVISTO
            }
        },

    
        "total_dados": len(dados)
    }   








dados = []

with open('./dados_tratados.csv', mode='r', encoding='utf-8-sig') as arquivo:
    leitor = csv.DictReader(arquivo, delimiter=';')
    for linha in leitor:
        dados.append(linha)

dashFinanceiro(dados)

