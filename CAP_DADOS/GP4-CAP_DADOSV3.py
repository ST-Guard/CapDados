import psutil
import csv
import json
from datetime import datetime
import time
import speedtest
import boto3 
import os
import requests
import mysql.connector
from dotenv import load_dotenv
import random
#pip install python-dotenv
#pip install mysql-connector-python
#pip install speedtest-cli


arquivo_csv = "dados-brutos_maquina.csv"
bucket_name = 'smartdatabucket4'

#STE12345
#SERVIDOR-SP-01

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

load_dotenv(".env.dev")
print("=== VERIFICANDO VARIÁVEIS DE AMBIENTE ===")
print(f"DB_HOST: {os.getenv('DB_HOST')}")
print(f"DB_USER: {os.getenv('DB_USER')}")
print(f"DB_PASSWORD: {'******' if os.getenv('DB_PASSWORD') else 'NÃO ENCONTRADA'}")
print(f"DB_NAME: {os.getenv('DB_NAME')}")
print("==========================================")

chave_acesso = os.getenv('aws_access_key_id')
chave_secreta = os.getenv('aws_secret_access_key')
token_sessao = os.getenv('aws_session_token')

# Banco de Dados
banco_host = os.getenv('DB_HOST')
banco_user = os.getenv('DB_USER')
banco_senha = os.getenv('DB_PASSWORD')
banco_nome = os.getenv('DB_NAME')
banco_porta = int(os.getenv('DB_PORT', 3306))

conexao = mysql.connector.connect(
    host=banco_host,
    user=banco_user,
    password=banco_senha,
    database=banco_nome,
    port=banco_porta
)


def upload_file(file_name, bucket, object_name=None):
    session = boto3.client(
        's3', region_name='us-east-1',
        aws_access_key_id=chave_acesso,
        aws_secret_access_key=chave_secreta,
        aws_session_token=token_sessao
    )
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    try:
        response = session.upload_file(file_name, bucket, object_name)
        print("Enviado para o S3 com sucesso")
    except ValueError as e:
        print(f"Erro ao enviar para o S3: {e}")
    return True




TOTAL_SERVIDORES_STEAM = 10000
def buscarJogadoresAtivos():
    STEAM_APP_ID = 730
    STEAM_API_URL = f"https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={STEAM_APP_ID}"

    FALLBACK_JOGADORES = 700000
    try:
        resposta  = requests.get(STEAM_API_URL, timeout=5)
        dados = resposta.json()
        jogadores = int(dados["response"]["player_count"])
        print(f"API Steam: {jogadores:,} jogadores ativos (AppID {STEAM_APP_ID})")
        return jogadores
    except Exception as e:
        print(f"[AVISO] Falha na API Steam: {e}. Usando fallback: {FALLBACK_JOGADORES:,}")
        return FALLBACK_JOGADORES 



#------------------------------------------------------------------------------------------------------------------
#FUNÇÕES DA SIMULAÇÃO

#Função utilizada para que os valores simulados não ultrapassem 100% no caso do disco e cpu, ou ultrapassem o que eles tem disponível para utilizar como a Ram 
def limitar(valor, minimo, maximo):
    return max(minimo, min(valor, maximo))

#A steam tem um comportamento especifico em cada dia da semana e para isso ser simulado com veracidade, iremos colcoar um fator de cada dia 
def fator_por_dia(dia_semana):  
    #[0] monday ...
    if dia_semana in [3, 4]:
        return 2.0
    elif dia_semana in [0, 1]:
        return 1.5
    elif dia_semana in [5, 6]:
        return 1.1
    elif dia_semana == 2:
        return 0.7
    #caso ele não capte nada, o default vai ser 1.0
    return 1.0


#Na steam há horários onde tem mais ou menos jogadores, para simular isso, irei fazer um peso para cada faixa horária
def fator_por_horario(hora):
    if 18 <= hora <= 23:
        return 1.8
    elif 12 <= hora < 18:
        return 1.2
    elif 6 <= hora < 12:
        return 0.8
    else:
        return 0.5


#Na steam, toda terça feira entre 18 - 21 horas, há manutenção dos seus servidores para aliviar a carga e isso afeta os servidores
def esta_em_manutencao(agora):
    return agora.weekday() == 1 and 18 <= agora.hour < 21

#------------------------------------------------------------------------------------------------------------------


def validarServidor():
    token = input("Digite o token da empresa: ")
    cursor = conexao.cursor()
    cursor.execute("SELECT * FROM empresa WHERE tokenEmpresa = %s", (token,))
    empresa = cursor.fetchall()

    if len(empresa) == 0:
        print("TOKEN INVALIDO!")
        return None

    nome_servidor = input("Digite o nome do servidor: ")


    

    id_empresa = empresa[0][0]



    





    cursor.execute(f"""
        select * from servidor  s 
        JOIN zona on s.fkZona = idZOna 
        JOIN datacenter on fkDataCenter =  idDataCenter 
        JOIN regiao ON fkRegiaoDataCenter = fkDataCenter 
        JOIN empresa ON fkRegiaoEmpresa = idEmpresa 
        WHERE idEmpresa = {id_empresa} AND s.nome = '{nome_servidor}';
    """)

    servidor = cursor.fetchall()

    if len(servidor) == 0:
        print("SERVIDOR NÃO É VALIDO")
        return None

    return servidor


def capturaCSV(servidor):
    print("INICIANDO A CAPTAÇÃO DOS DADOS")
    arquivo_csv = f"dados-brutos-{servidor[0][1]}.csv"
    

    with open(arquivo_csv, 'a', newline='') as csvfile:       
        colunas = ['EMPRESA', 'REGIAO', 'DATACENTER', 'ZONA', 'SERVIDOR', 'CPU', 'RAM_TOTAL', 'RAM_USADA', 'RAM_PERCENT',
                   'DISCO_TOTAL', 'DISCO_USADO', 'DISCO_PERCENT', 'LATENCIA', 'PACOTES_ENVIADOS', 'PACOTES_RECEBIDOS', 'PACOTES_PERDIDOS',
                   'QTD_PR', 'PROCESSO1_CPU', 'PORCENTAGEM_PROCESSO1_CPU', 'PROCESSO2_CPU', 'PORCENTAGEM_PROCESSO2_CPU', 'PROCESSO3_CPU', 'PORCENTAGEM_PROCESSO3_CPU',
                   'PROCESSO1_RAM', 'PORCENTAGEM_PROCESSO1_RAM', 'PROCESSO2_RAM', 'PORCENTAGEM_PROCESSO2_RAM', 'PROCESSO3_RAM', 'PORCENTAGEM_PROCESSO3_RAM',
                   'QTD_NUCLEOS', 'USO_USER', 'USO_SISTEM', 'BOOTTIME', 'DATA_HORA', 'DIA_SEMANA', 'JOGADORES_ATIVOS']
        CSV_DIC_WRITER = csv.DictWriter(csvfile, fieldnames=colunas, delimiter=';')

        if os.path.getsize(arquivo_csv) == 0:
            CSV_DIC_WRITER.writeheader()

        #pegando os componentes normais
        cpu_real = psutil.cpu_percent(interval=1)
        ram = psutil.virtual_memory()
        disk = psutil.disk_usage("/")

        # pegando hora e contagem de processos
        tempo_agora = datetime.now()
        dia_semana = tempo_agora.strftime('%A')
        dia = tempo_agora.weekday()
        hora = tempo_agora.hour

        contagem_processos = len(psutil.pids())
        bootime = psutil.boot_time()
        qtd_nucleos = psutil.cpu_count(logical=False)

        # pegando porcentagens especificas
        uso_cpu = psutil.cpu_times_percent(interval=1)
        uso_user = uso_cpu.user
        uso_sistema = uso_cpu.system

        try:
            #pegando rede
            latencia_speed_test = speedtest.Speedtest(secure=True)
            latencia_speed_test.get_best_server()
            ping_real = latencia_speed_test.results.ping
        except Exception as e:
            print(f"Erro no speedtest: {e}")
            ping_real = 0.0

        dados_internet = psutil.net_io_counters()
        pacotes_enviados = dados_internet.packets_sent
        pacotes_recebidos = dados_internet.packets_recv
        pacotes_perdidos = dados_internet.errin + dados_internet.errout

        #----------------------------------------------------------
        #FATORES DE CADA DIA
        fator_dia = fator_por_dia(dia)
        fator_hora = fator_por_horario(hora)
        manutencao = esta_em_manutencao(tempo_agora)

        fator_latencia_dia = {
            0: 1.2,  
            1: 1.3,  
            2: 0.8,  
            3: 1.4,  
            4: 1.5,  
            5: 1.4,  
            6: 1.2   
        }

        #----------------------------------------------------------
        #Aplicando a regra de negócio:

        #CPU
        cpu_negocio = cpu_real * fator_dia * fator_hora

        if manutencao:
            cpu_negocio *= 0.45

        cpu_negocio += random.uniform(-3, 3)
        cpu_negocio = limitar(cpu_negocio, 5, 98)

        #RAM
        ram_percent_negocio = ram.percent
        ram_percent_negocio *= 1 + ((fator_hora - 1) * 0.15)
        ram_percent_negocio += random.uniform(-2, 2)
        ram_percent_negocio = limitar(ram_percent_negocio, 30, 92)
        ram_usada_negocio = int(ram.total * (ram_percent_negocio / 100))

        #DISCO
        disco_percent_negocio = disk.percent
        disco_percent_negocio *= 1 + ((fator_dia - 1) * 0.10)

        if dia == 2:
            disco_percent_negocio *= 0.85

        if manutencao:
            disco_percent_negocio *= random.uniform(0.75, 0.90)

        disco_percent_negocio += random.uniform(-1.5, 1.5)
        disco_percent_negocio = limitar(disco_percent_negocio, 20, 95)
        disco_usado_negocio = int(disk.total * (disco_percent_negocio / 100))

        #LATÊNCIA
        latencia_base = ping_real

        if latencia_base <= 0:
            latencia_base = random.uniform(8, 25)

        if manutencao:
            latencia_negocio = latencia_base * random.uniform(6, 12)
        else:
            latencia_negocio = latencia_base
            # impacto menor do horário
            latencia_negocio *= (1 + ((fator_hora - 1) * 0.15))
            # impacto menor do dia
            latencia_negocio *= (1 + ((fator_latencia_dia[dia] - 1) * 0.10))
            # oscilação natural da rede
            latencia_negocio += random.uniform(-5, 5)

        # sábado à noite: pico de jogadores
        if dia == 5 and 18 <= hora <= 23:
            latencia_negocio = limitar(latencia_negocio, 25, 120)
        else:
            latencia_negocio = limitar(latencia_negocio, 5, 500)

        #PACOTES
        pacotes_enviados_negocio = int(pacotes_enviados * (1 + ((fator_hora - 1) * 0.20)))
        pacotes_recebidos_negocio = int(pacotes_recebidos * (1 + ((fator_hora - 1) * 0.35)))

        # Na quinta e na sexta começam as promoções de fim de semana, por há um aumento
        if dia in [3, 4]:
            pacotes_recebidos_negocio = int(pacotes_recebidos_negocio * random.uniform(1.3, 2.0))

        if manutencao:
            pacotes_enviados_negocio = int(pacotes_enviados_negocio * random.uniform(0.30, 0.60))
            pacotes_recebidos_negocio = int(pacotes_recebidos_negocio * random.uniform(0.20, 0.50))

        #ruído natural do dia a dia
        pacotes_enviados_negocio = max(0, pacotes_enviados_negocio + random.randint(-5000, 5000))
        pacotes_recebidos_negocio = max(0, pacotes_recebidos_negocio + random.randint(-10000, 10000))
        pacotes_perdidos_negocio = max(0, pacotes_perdidos + random.randint(-10, 10))

        #PROCESSOS
        qtd_processos_negocio = int(contagem_processos * (1 + ((fator_hora - 1) * 0.05)))
        qtd_processos_negocio += random.randint(-3, 3)
        qtd_processos_negocio = max(1, qtd_processos_negocio)

        lista_processos = []

        #Fazendo um for para que o uso da cpu não venho como 0, pq a primeira rodada dele vem 0
        for p in psutil.process_iter():
            try:
                p.cpu_percent(None)
            except:
                pass

        time.sleep(0.5)

        #TOP3 CPU E RAM
        for p in psutil.process_iter(['name', 'cpu_percent', 'memory_info', 'ppid']):
            try:
                info = p.info

                if info['name'] and info['memory_info'] is not None:
                    cpu_processo = info['cpu_percent']
                    ram_processo = info['memory_info'].rss

                    cpu_processo *= 1 + ((fator_hora - 1) * 0.20)
                    ram_processo *= 1 + ((fator_dia - 1) * 0.08)

                    if manutencao:
                        cpu_processo *= random.uniform(0.35, 0.70)
                        ram_processo *= random.uniform(0.80, 0.95)

                    cpu_processo += random.uniform(-1, 1)
                    cpu_processo = limitar(cpu_processo, 0, 100)

                    lista_processos.append({
                        'nome': info['name'],
                        'cpu': cpu_processo,
                        'memoria': int(ram_processo),
                        'pid': info['ppid']
                    })

            except:
                pass

        #Como funciona o sorted: ordene a lista x, key=lambda x = pelo o que vc quer ordenar, reverse=TRUE[:3] : do maior para o menor, pegar o top3
        top_cpu = sorted(lista_processos, key=lambda x: x['cpu'], reverse=True)[:3]
        top_ram = sorted(lista_processos, key=lambda x: x['memoria'], reverse=True)[:3]

        #Se não tiver 3 processos...
        while len(top_cpu) < 3:
            top_cpu.append({'nome': 'N/A', 'cpu': 0, 'memoria': 0})

        while len(top_ram) < 3:
            top_ram.append({'nome': 'N/A', 'cpu': 0, 'memoria': 0})

        # API Steam 
        jogadores_globais  = buscarJogadoresAtivos()
        jogadores_nossos = round(jogadores_globais * 0.4,2)



        print()
        dados_dict = {
            'EMPRESA': f'{servidor[0][19]}',
            'REGIAO': f'{servidor[0][14]}',
            'DATACENTER': f'{servidor[0][9]}',
            'ZONA': f'{servidor[0][6].replace(" ", "")}',
            'SERVIDOR': f'{servidor[0][1]}',
            'CPU': cpu_negocio,
            'RAM_TOTAL': ram.total,
            'RAM_USADA': ram_usada_negocio,
            'RAM_PERCENT': ram_percent_negocio,
            'DISCO_TOTAL': disk.total,
            'DISCO_USADO': disco_usado_negocio,
            'DISCO_PERCENT': disco_percent_negocio,
            'LATENCIA': latencia_negocio,
            'PACOTES_ENVIADOS': pacotes_enviados_negocio,
            'PACOTES_RECEBIDOS': pacotes_recebidos_negocio,
            'PACOTES_PERDIDOS': pacotes_perdidos_negocio,
            'QTD_PR': qtd_processos_negocio,
            'PROCESSO1_CPU': top_cpu[0]['nome'],
            'PORCENTAGEM_PROCESSO1_CPU': top_cpu[0]['cpu'],
            'PROCESSO2_CPU': top_cpu[1]['nome'],
            'PORCENTAGEM_PROCESSO2_CPU': top_cpu[1]['cpu'],
            'PROCESSO3_CPU': top_cpu[2]['nome'],
            'PORCENTAGEM_PROCESSO3_CPU': top_cpu[2]['cpu'],
            'PROCESSO1_RAM': top_ram[0]['nome'],
            'PORCENTAGEM_PROCESSO1_RAM': top_ram[0]['memoria'],
            'PROCESSO2_RAM': top_ram[1]['nome'],
            'PORCENTAGEM_PROCESSO2_RAM': top_ram[1]['memoria'],
            'PROCESSO3_RAM': top_ram[2]['nome'],
            'PORCENTAGEM_PROCESSO3_RAM': top_ram[2]['memoria'],
            'QTD_NUCLEOS': qtd_nucleos,
            'USO_USER': uso_user,
            'USO_SISTEM': uso_sistema,
            'BOOTTIME': bootime,
            'DATA_HORA': tempo_agora,
            'DIA_SEMANA': dia_semana,
            'JOGADORES_ATIVOS': jogadores_nossos
        }


        print("OS DADOS DO SERVIDOR SÃO: ", servidor[0][19])

        print(f"""
            ----------------CAPTURANDO DADOS ----------------

            DATA/HORA: {tempo_agora}

            EMPRESA: {servidor[0][19]}
            REGIAO: {servidor[0][14]}
            DATACENTER: {servidor[0][9]}
            ZONA: {servidor[0][6]}
            SERVIDOR: {servidor[0][1]}

            ---------------- COMPONENTES ----------------

            CPU: {cpu_negocio:.2f}%

            RAM TOTAL: {ram.total}
            RAM USADA: {ram_usada_negocio}
            RAM USO: {ram_percent_negocio:.2f}%

            DISCO TOTAL: {disk.total}
            DISCO USADO: {disco_usado_negocio}
            DISCO USO: {disco_percent_negocio:.2f}%

            LATENCIA: {latencia_negocio:.2f}ms

            ---------------- REDE ----------------

            PACOTES ENVIADOS: {pacotes_enviados_negocio}
            PACOTES RECEBIDOS: {pacotes_recebidos_negocio}
            PACOTES PERDIDOS: {pacotes_perdidos_negocio}

            ---------------- PROCESSOS ----------------

            QTD PROCESSOS: {qtd_processos_negocio}
            BOOT TIME: {bootime}

            TOP 3 CPU:

            1° {top_cpu[0]['nome']} -> {top_cpu[0]['cpu']:.2f}%
            2° {top_cpu[1]['nome']} -> {top_cpu[1]['cpu']:.2f}%
            3° {top_cpu[2]['nome']} -> {top_cpu[2]['cpu']:.2f}%

            TOP 3 RAM:

            1° {top_ram[0]['nome']} -> {top_ram[0]['memoria']}
            2° {top_ram[1]['nome']} -> {top_ram[1]['memoria']}
            3° {top_ram[2]['nome']} -> {top_ram[2]['memoria']}

            -------------------JOGADORES STEAM (API)------------------------------

            JOGADORES ATIVOS: {jogadores_nossos} 
             """)

        CSV_DIC_WRITER.writerow(dados_dict)
        csvfile.flush()

        nome_arquivo_s3 = f"raw/{dados_dict['EMPRESA']}_{dados_dict['DATACENTER']}_{dados_dict['ZONA']}_{dados_dict['SERVIDOR']}_dadosBrutos.csv"
        print("Enviando dados capturados")
        upload_file(arquivo_csv, bucket_name, nome_arquivo_s3)


def capturaJson():
    conexao_json = mysql.connector.connect(
        host=banco_host,
        user=banco_user,
        password=banco_senha,
        database=banco_nome,
        port=banco_porta
    )

    cursor = conexao_json.cursor(dictionary=True)
    query = """
        SELECT
            e.idEmpresa,
            e.razaoSocial AS empresa,
            r.idRegiao,
            r.estado,
            r.cep,
            r.numero,
            r.estado,
            d.idDataCenter,
            d.nome AS datacenter,
            d.capacidadeServidores,
            z.idZona,
            z.nome AS zona,
            s.idServidor,
            s.nome AS servidor,
            s.tipo AS tipoServidor,
            s.estado AS estadoServidor,
            c.idComponente,
            c.nome AS componente,
            c.tipo AS tipoComponente,
            c.unidadeMedida,
            c.capacidadeMaxima,
            cs.idComponenteServidor,
            cs.limite,
            gestor.idUsuario AS idGestor,
            gestor.nome AS nomeGestor,
            gestor.email AS emailGestor,
            analista.idUsuario AS idAnalista,
            analista.nome AS nomeAnalista,
            analista.email AS emailAnalista

        FROM empresa e JOIN regiao r
            ON r.fkRegiaoEmpresa = e.idEmpresa
        JOIN datacenter d
            ON d.idDataCenter = r.fkRegiaoDataCenter
        JOIN zona z
            ON z.fkDataCenter = d.idDataCenter
        JOIN servidor s
            ON s.fkZona = z.idZona
        JOIN componentes_servidores cs
            ON cs.fkServidor = s.idServidor
        JOIN componentes c
            ON c.idComponente = cs.fkComponentes
        LEFT JOIN datacenters_gestores dg
            ON dg.fk_datacenter = d.idDataCenter
            AND dg.ativo = 1
        LEFT JOIN usuario gestor
            ON gestor.idUsuario = dg.fk_usuario
        LEFT JOIN analista_zona az
            ON az.zona_id = z.idZona
            AND az.ativo = 1
        LEFT JOIN usuario analista
            ON analista.idUsuario = az.usuario_id
        ORDER BY
            e.idEmpresa,
            d.idDataCenter,
            z.idZona,
            s.idServidor,
            c.idComponente;
        """

    cursor.execute(query)
    geral = cursor.fetchall()

    estruturaGeral = {}
    for linha in geral:
        empresa = linha["empresa"]
        regiao = linha["estado"]
        datacenter = linha["datacenter"]
        zona = linha["zona"].replace(" ", "")
        servidor = linha["servidor"]
        componente = linha["componente"]

        if empresa not in estruturaGeral:
            estruturaGeral[empresa] = {}

        if datacenter not in estruturaGeral[empresa]:
            estruturaGeral[empresa][regiao] = {}

        if zona not in estruturaGeral[empresa][regiao]:
            estruturaGeral[empresa][regiao][datacenter] = {}

        if zona not in estruturaGeral[empresa][regiao][datacenter]:
            estruturaGeral[empresa][regiao][datacenter][zona] = {}


        if servidor not in estruturaGeral[empresa][regiao][datacenter][zona]:
            estruturaGeral[empresa][regiao][datacenter][zona][servidor] = {
                "tipo": linha["tipoServidor"],
                "estado": linha["estadoServidor"],
                "idServidor": linha["idServidor"],
                "idDataCenter": linha["idDataCenter"],
                "limites": {},
                "limiteIds": {},
                "funcionarios": []
            }

        estruturaGeral[empresa][regiao][datacenter][zona][servidor]["limites"][componente] = linha["limite"]
        estruturaGeral[empresa][regiao][datacenter][zona][servidor]["limiteIds"][componente] = linha["idComponente"]
        
    if linha["nomeAnalista"] and linha["nomeAnalista"] not in [f["nome"] for f in estruturaGeral[empresa][datacenter][zona][servidor]["funcionarios"]]:
        estruturaGeral[empresa][regiao][datacenter][zona][servidor]["funcionarios"].append({
            "id": linha["idAnalista"],
            "nome": linha["nomeAnalista"]
        })

    geral_json = json.dumps(estruturaGeral, ensure_ascii=False, indent=4, default=str)

    caminho_json = "metricas.json"

    with open(caminho_json, "w", encoding="utf-8") as arquivo:
        arquivo.write(geral_json)

    nome_arquivo_json = "raw/metricas.json"
    print("Enviando metricas em json")
    upload_file(caminho_json, bucket_name, nome_arquivo_json)
















     # pegando os dados das empresas

     # pegando os dados das empresas
    cursor = conexao_json.cursor()

    query_empresas = "SELECT * FROM empresa"
    cursor.execute(query_empresas)
    emrpesas = cursor.fetchall()

    dados_alertas = {}
    for empresa in emrpesas:
        

        id_empresa = empresa[0]
        nome_empresa = empresa[1]

        query_datacenters = f"SELECT * FROM datacenter JOIN regiao ON fkRegiaoDataCenter = idDatacenter JOIN empresa ON fkRegiaoEmpresa = idEmpresa WHERE fkRegiaoEmpresa = {id_empresa};"
        cursor.execute(query_datacenters)
        datacenters  = cursor.fetchall()


   

        existe_empresa_no_json = False

    
        for dados in dados_alertas:
      
            if dados == nome_empresa:
                existe_empresa_no_json = True

        if existe_empresa_no_json == False:
            dados_alertas[nome_empresa] = {}

        

        

        for datacenter in datacenters:

            nome_datacenter = datacenter[1]

            id_datacenter = datacenter[0]

            query_zonas = f"SELECT * FROM zona WHERE fkDataCenter = {id_datacenter};"
            cursor.execute(query_zonas)
            zonas = cursor.fetchall()


            dados_alertas[nome_empresa][nome_datacenter] = {}

            for zona in zonas:
                        id_zona = zona[0]
                        nome_zona = zona[1].replace(" ", "")

                  

                        query_servidores = f"SELECT * FROM servidor WHERE fkZona = {id_zona};"
                        cursor.execute(query_servidores)
                        servidores = cursor.fetchall()



                        query_mttr_z = f"""SELECT z.nome, count(z.idZona) as "Quantidade de alertas em aberto" FROM zona z 
                        JOIN servidor ON fkZona = z.idZona 
                        JOIN registros_alertas ON fkRegistroServidor = idServidor   WHERE resolvido_em is null and z.idZona = {id_zona} GROUP BY z.nome;"""
                        cursor.execute(query_mttr_z)
                        quantidade_aberto = cursor.fetchall()


                        

                        query_mttr_zona = f"""
                            SELECT z.nome, AVG(mttr_minutos) FROM zona z 
                            JOIN servidor ON fkZona = z.idZona  JOIN registros_alertas ON fkRegistroServidor = idServidor WHERE z.idZona = {id_zona} AND severidade like "critico" GROUP BY z.nome;
                            """
                        cursor.execute(query_mttr_zona)
                        mttr = cursor.fetchall()




                        query_qtd_servidores = f"""
                                SELECT z.nome, count(z.idZona) as "Quantidade de servidores" FROM zona z 
                                            JOIN servidor ON fkZona = z.idZona 
                                            WHERE z.idZona = {id_zona} GROUP BY z.nome;
                                            """
                        
                        cursor.execute(query_qtd_servidores)
                        qtd_servidores = cursor.fetchall()


                        

                        qtd_servidores = qtd_servidores[0][1]


                        



                        quantidade_alerta = 0
                        if len(quantidade_aberto) == 0:
                            quantidade_alerta = 0
                        else :
                            quantidade_alerta = quantidade_aberto[0][1]

                        mttr_m = ""


                        
                        if len(mttr) == 0:
                            mttr_m = 0
                        else:
                            mttr_m = mttr[0][1]



                        dados_alertas[nome_empresa][nome_datacenter][nome_zona] = {
                            "QUANTIDADE_ABERTO": quantidade_alerta,
                            "MTTR_ZONA": mttr_m,
                            "QTD_SERVIDORES": qtd_servidores

                        }

                        for servidor in servidores:
                                
                                id_servidor = servidor[0]
                                nome_servidor = servidor[1] 
                   


                                query_alerta = """
                                    SELECT
                                        r.idRegistro,
                                        r.valor,
                                        r.threshold_momento,
                                        r.severidade,
                                        r.issue_key,
                                        r.aberto_em,
                                        r.resolvido_em,
                                        r.mttr_minutos,
                                        r.sla_ok,
                                        r.fkRegistroServidor,
                                        r.fkRegistroComponente,
                                        c.nome AS componente
                                    FROM registros_alertas r
                                    JOIN componentes c
                                        ON c.idComponente = r.fkRegistroComponente
                                    WHERE r.fkRegistroServidor = %s
                                    AND r.aberto_em < NOW()
                                    AND (
                                            r.resolvido_em >= DATE_SUB(NOW(), INTERVAL 30 DAY)
                                            OR r.resolvido_em IS NULL
                                        )
                                    ORDER BY r.aberto_em;
                                """

                                cursor.execute(query_alerta, (id_servidor,))
                                alertas_servidor = cursor.fetchall()

                                alerta_servidor_dicionario = {}

                                for alerta in alertas_servidor:
                                    id_alerta = alerta[0]
                                    valor = alerta[1]
                                    threshold_momento = alerta[2]
                                    severidade = alerta[3]
                                    issue_key = alerta[4]
                                    abertura = alerta[5]
                                    resolvido_em = alerta[6]
                                    mttr_minutos = alerta[7]
                                    sla_ok = alerta[8]
                                    componente = alerta[11]

                                    if resolvido_em is None:
                                        status_chamado = "aberto"
                                        fechamento = None

                                        if abertura is not None:
                                            duracao_atual_minutos = int(
                                                (datetime.now() - abertura).total_seconds() / 60
                                            )
                                        else:
                                            duracao_atual_minutos = None

                                    else:
                                        status_chamado = "fechado"
                                        fechamento = resolvido_em.isoformat()

                                        if mttr_minutos is not None:
                                            duracao_atual_minutos = mttr_minutos
                                        elif abertura is not None:
                                            duracao_atual_minutos = int(
                                                (resolvido_em - abertura).total_seconds() / 60
                                            )
                                        else:
                                            duracao_atual_minutos = None

                                    alerta_servidor_dicionario[str(id_alerta)] = {
                                        "id_alerta": id_alerta,
                                        "componente": componente,
                                        "valor": valor,
                                        "limite": threshold_momento,
                                        "severidade": severidade,
                                        "issueKey": issue_key,
                                        "status": status_chamado,
                                        "abertura": (
                                            abertura.isoformat()
                                            if abertura is not None
                                            else None
                                        ),
                                        "fechamento": fechamento,
                                        "duracaoMinutos": duracao_atual_minutos,
                                        "slaOk": sla_ok
                                    }


                                dados_alertas[nome_empresa][nome_datacenter][nome_zona][nome_servidor] = {
                                        "idServidor": id_servidor,
                                        "chamados": alerta_servidor_dicionario
                                }


    geral_json = json.dumps(dados_alertas, ensure_ascii=False, indent=4, default=str)

    
    caminho_json = "ultimos_alertas.json"

    with open(caminho_json, "w", encoding="utf-8") as arquivo:
        arquivo.write(geral_json)

    nome_arquivo_json = "dados_alertas/ultimos_alertas.json"
    print("Enviando os ultimos alertas")
    upload_file(caminho_json, bucket_name, nome_arquivo_json)


    # pegando os dados dos alertas/mttr


servidor = validarServidor()






    

if servidor:
    cont = -1
    while True:
        capturaCSV(servidor)
        time.sleep(30)

        if (cont == 30 or cont == -1):
            cont = 0
            capturaJson()

        cont += 1
else:
    print("Erro na validação")







    