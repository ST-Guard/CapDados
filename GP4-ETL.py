import pandas as pd
from datetime import timedelta
import time 
import csv


print("""\033[33m
  /$$$$$$                                      /$$     /$$$$$$$              /$$              
 /$$__  $$                                    | $$    | $$__  $$            | $$              
| $$  \__/ /$$$$$$/$$$$   /$$$$$$   /$$$$$$  /$$$$$$  | $$  \ $$  /$$$$$$  /$$$$$$    /$$$$$$ 
|  $$$$$$ | $$_  $$_  $$ |____  $$ /$$__  $$|_  $$_/  | $$  | $$ |____  $$|_  $$_/   |____  $$
 \____  $$| $$ \ $$ \ $$  /$$$$$$$| $$  \__/  | $$    | $$  | $$  /$$$$$$$  | $$      /$$$$$$$
 /$$  \ $$| $$ | $$ | $$ /$$__  $$| $$        | $$ /$$| $$  | $$ /$$__  $$  | $$ /$$ /$$__  $$
|  $$$$$$/| $$ | $$ | $$|  $$$$$$$| $$        |  $$$$/| $$$$$$$/|  $$$$$$$  |  $$$$/|  $$$$$$$
 \______/ |__/ |__/ |__/ \_______/|__/         \___/  |_______/  \_______/   \___/   \_______/
                                                                                              
                                                                                              
\033[m""")




caminho_csv = 'dados-brutos_maquina.csv'



while True: 


    dados_brutos = pd.read_csv(caminho_csv, sep=';')
    dados_brutos['DATA_HORA'] = pd.to_datetime(dados_brutos['DATA_HORA'])

    # Indentificando o horario de 5 minutos atras (Tempo agora - 5 minutos)
    limite_tempo = pd.Timestamp.now() - timedelta(minutes=5)
    # Quantidade de linhas menos o zero (Header)
    qtd_linha_atual = len(dados_brutos) - 1

    # criando as variaveis pico calcular a média
    mediaram = 0
    mediadisco = 0 
    mediapercentram = 0
    mediapercentcpu = 0
    mediapercentdisco = 0 

    # criando as variaveis de pico
    picoram = dados_brutos['RAM_USADA'][qtd_linha_atual]
    picodisco = dados_brutos['DISCO_USADO'][qtd_linha_atual]

    picopercentram =  dados_brutos['RAM_PERCENT'][qtd_linha_atual]
    picopercentdisco = dados_brutos['DISCO_PERCENT'][qtd_linha_atual]
    picopercentcpu = dados_brutos['CPU'][qtd_linha_atual]

    # está variavel serve para contar quantas linhas tem que passam no limite de 5 minutos
    cont_linhas_soma = 0
    
    while qtd_linha_atual >= 0:
        if(dados_brutos['DATA_HORA'][qtd_linha_atual] >= limite_tempo ):
            
            
            mediaram = mediaram + dados_brutos['RAM_USADA'][qtd_linha_atual]
            mediapercentram = mediapercentram + dados_brutos['RAM_PERCENT'][qtd_linha_atual]
        
            mediadisco = mediadisco + dados_brutos['DISCO_USADO'][qtd_linha_atual]
            mediapercentdisco = mediapercentdisco + dados_brutos['DISCO_PERCENT'][qtd_linha_atual]

            mediapercentcpu = mediapercentcpu + dados_brutos['CPU'][qtd_linha_atual]
            cont_linhas_soma = cont_linhas_soma + 1
        
            # pegando os picos do uso de ram
            momento_atual_uso_ram = dados_brutos['RAM_USADA'][qtd_linha_atual]
            momento_atual_percent_ram = dados_brutos['RAM_PERCENT'][qtd_linha_atual]

            if momento_atual_uso_ram > picoram:
                picoram = momento_atual_uso_ram

            if momento_atual_percent_ram > picopercentram:
                picopercentram = momento_atual_percent_ram


            # pegando os picos do uso de disco

            momento_atual_uso_disco = dados_brutos['DISCO_USADO'][qtd_linha_atual]
            momento_atual_percent_disco = dados_brutos['DISCO_PERCENT'][qtd_linha_atual]

            if momento_atual_uso_disco > picodisco:
                picodisco = momento_atual_uso_disco

            if momento_atual_percent_disco > picopercentdisco:
                picopercentdisco = momento_atual_percent_disco

            # pegando os picos do uso de cpu

            momento_atual_percent_cpu = dados_brutos['CPU'][qtd_linha_atual]

            if momento_atual_percent_cpu > picopercentcpu:
                picopercentcpu = momento_atual_percent_cpu

        else:
            break
         
        
        qtd_linha_atual -= 1

    

    if(cont_linhas_soma == 0):
        print("Nenhum dado encontrado a 5 minutos atrás")
        break

    # deixando as médias que estão em byte para gb

    mediaram = mediaram / (1024 ** 3)
    mediadisco = mediadisco / (1024 ** 3)

    # calculando a média da porcentagem e dos dados com a quantidade de linhas 

    mediaram = round(mediaram / cont_linhas_soma, 2)
    mediadisco = round(mediadisco / cont_linhas_soma, 2)

    mediapercentcpu = round(mediapercentcpu / cont_linhas_soma, 2)
    mediapercentdisco = round(mediapercentdisco / cont_linhas_soma,2)
    mediapercentram = round(mediapercentram / cont_linhas_soma,2)


    # deixando os picos que estão em byte para gb

    picodisco = round(picodisco / (1024 ** 3), 2)
    picoram = round(picoram / (1024 ** 3),2 )


    # pegando o usuario atual

    usuario_atual = dados_brutos['EMPRESA'][qtd_linha_atual + 1]

    # criando o csv com os dados tratados

    arquivo_csv = 'dados_tratados_maquina.csv'

    with open(arquivo_csv, 'a', newline='') as csvfile:
        colunas = ['EMPRESA', 'MEDIA_CPU','PICO_CPU', 'MEDIA_RAM', 'PICO_RAM','PORCENTAGEM_RAM', 'PICO_P_RAM', 'MEDIA_DISCO', 'PICO_DISCO', 'PORCENTAGEM_DISCO', 'PICO_P_DISCO']
        CSV_DIC_WRITER = csv.DictWriter(csvfile, fieldnames=colunas, delimiter=';')

        if csvfile.tell() == 0:
            CSV_DIC_WRITER.writeheader()

        print(f"""
        Escrevendo os dados tratados dos ultimos 10 minutos
        
        Média uso cpu: {mediapercentcpu}
        Pico uso cpu: {picopercentcpu}

        Média uso ram: {mediaram}
        Média uso ram porcentagem: {mediapercentram}
        Pico uso ram: {picoram}
        Pico uso ram porcentagem {picopercentram}

        Média uso de disco: {mediadisco}
        Média uso de disco porcentagem: {mediapercentdisco}
        Pico uso de disco: {picodisco}
        Pico uso de disco porcentagem {picopercentdisco}

        """)

        escrita_dados = {'EMPRESA': usuario_atual, 'MEDIA_CPU': mediapercentcpu, 'PICO_CPU': picopercentcpu, 'MEDIA_RAM': mediaram, 'PICO_RAM': picoram, 'PORCENTAGEM_RAM': mediapercentram, 'PICO_P_RAM': picopercentram, 
                         'MEDIA_DISCO': mediadisco, 'PICO_DISCO': picodisco, 'PORCENTAGEM_DISCO': mediapercentdisco, 'PICO_P_DISCO': picopercentdisco  }
    
        CSV_DIC_WRITER.writerow(escrita_dados)
        csvfile.flush()
    
    time.sleep(10)


    """
----------------------------------------------------------------------INFORMÇÔES QUE O ETL VAI BUSCAR ****GESTOR PT1--
KPI
Processo com maior consumo (VARCHAR)
Quantidade de alertas (INT)
Indice de Confiança para Manutenção Preventiva (ICMP) (%)
Servidores Ativos (INT)
Servidores Inativos (INT)

Tendencia de degradação (DIARIA)(ULTIMO 30 DIAS) {
29/03: {QTD_SOBRECARREADO: 30, QTD_ESTRESSADOS: 20},
28/03: {QTD_SOBRECARREADO: 30, QTD_ESTRESSADOS: 20},
29/03: {QTD_SOBRECARREADO: 30, QTD_ESTRESSADOS: 20}
....
}

Pico de Uso Semanal (DIARIA)(DIA SEMANA){
CPU:   {PICO_MAXIMO: 91%, PICO_MEDIO: 77%, PICO_ MINIMO: 30%, SEG: 30%,TER: 20% . . .},
RAM:   {PICO_MAXIMO: 91%, PICO_MEDIO: 77%, PICO_ MINIMO: 30%, SEG: 30%,TER: 20% . . .},
DISCO: {PICO_MAXIMO: 91%, PICO_MEDIO: 77%, PICO_ MINIMO: 30%, SEG: 30%,TER: 20% . . .},
}


****GESTOR PT2
--KPI
Processo com maior consumo (VARCHAR)
Quantidade de alertas (INT)
Indice de Confiança para Manutenção Preventiva (ICMP) (%)
Servidores Ativos (INT)
Servidores Inativos (INT)

Uso de Disco vs Rede (HORA)(ULTIMO 24 HRS) {
00:00: {UsoDisco: 30%, TrafegoRede: 20%},
03:00: {UsoDisco: 50%, TrafegoRede: 30%},
05:00: {UsoDisco: 70%, TrafegoRede: 20%},
...
}
Pico de Disco (%)
Horário do pico de Disco (VARCHAR)
Pico de Rede (%)
Horário do pico de Rede (VARCHAR)

Tempo de atividade vc Latência (HORA)(ULTIMO 24 HRS) {
00:00: {Uptime: 30%, Latencia: 20%},
03:00: {Uptime: 50%, Latencia: 30%},
05:00: {Uptime: 70%, Latencia: 20%},
...
}
Servidor com menor UPTIME (VARCHAR)
Horário do servidor com menor UPTIME (VARCHAR)
Pico de Latência (MS)

Servidores online (INT)
Servidores críticos (INT)
    



************************ANALISTA************************
----KPI

Total de servidores (INT)
Servidores inativos (INT)
P99 da RAM (%)
P99 do CPU (%)
Uso do Disco (%)
Uso do Disco (TB)
Total do Disco (TB)
Qtd de servidores com baixa latência (INT)



--Gráficos


Servidores_críticos(ORDENADO){
SERVIDOR1: {USO_CPU: 90%, USO_RAM: 90%},
SERVIDOR1: {USO_CPU: 80%, USO_RAM: 80%},
...
}
qtd total servidores (INT)

TOP3_PROCESSOS_USO_RAM(ORDENADO){
processo1: {USO_GB: 90, %TOTAL: 41},
processo2: {USO_GB: 80, %TOTAL: 31},
processo3: {USO_GB: 50, %TOTAL: 21},
}
Soma do TOP3 GB (FLOAT)
Porcentagem da soma do TOP3 GB (%)


TOP3_PROCESSOS_USO_CPU(ORDENADO){
processo1: {USO_GB: 90, %TOTAL: 41},
processo2: {USO_GB: 80, %TOTAL: 31},
processo3: {USO_GB: 50, %TOTAL: 21},
}
Porcentagem da soma do TOP3 GB (%)


Monitoramento_uso_recursos(ULTIMAS 24HRS){
00:00: {media_uso_ram: 30%, media_uso_disco: 40%, media_uso_cpu: 10%, latencia: 30ms},
01:00: {media_uso_ram: 30%, media_uso_disco: 40%, media_uso_cpu: 10%, latencia: 30ms},
02:30: {media_uso_ram: 30%, media_uso_disco: 40%, media_uso_cpu: 10%, latencia: 30ms},
....
}







SPOILER ESPECIFICA
Quantidade de servidores com uso de CPU de mais de 90% (INT)
Indice de servidores com a RAM disponível menor que 5% (INT)

    
""" 



