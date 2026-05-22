import psutil
import csv
import json
from datetime import datetime
import time
import speedtest # baixar como pip install speedtest-cli
import boto3 
import os
import mysql.connector
from dotenv import load_dotenv
#pip install python-dotenv
#pip install mysql-connector-python


arquivo_csv = "dados-brutos_maquina.csv"
bucket_name = 'smartdatabucket1'

#STE12345
#SRV-DC01-WEB-05

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
        's3',
        aws_access_key_id=os.getenv('aws_access_key_id'),
        aws_secret_access_key=os.getenv('aws_secret_access_key'),
        aws_session_token=os.getenv('aws_session_token')
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

def validarServidor():
    token = input("Digite o token da empresa: ")
    cursor = conexao.cursor()
    cursor.execute("SELECT * FROM empresa WHERE tokenEmpresa = %s", (token,))
    empresa = cursor.fetchall()

    if len(empresa) == 0:
        print("TOKEN INVALIDO!")
        return None

    nome_servidor = input("Digite o nome do servidor: ")

    cursor.execute("""
        SELECT *
        FROM servidor AS s
        JOIN zona ON s.fkZona = idZona
        JOIN regiao ON fkRegiaoDatacenter = fkDataCenter
        JOIN empresa ON fkRegiaoEmpresa = idEmpresa
        JOIN datacenter ON fkRegiaoDatacenter = idDatacenter
        WHERE idEmpresa = %s AND s.nome = %s
    """, (empresa[0][0], nome_servidor))

    servidor = cursor.fetchall()

    if len(servidor) == 0:
        print("SERVIDOR NÃO É VALIDO")
        return None

    return servidor

def capturaCSV(servidor):
        print("INICIANDO A CAPTAÇÃO DOS DADOS")
        arquivo_csv = f"dados-brutos-{servidor[0][1]}.csv"
        agora = datetime.now().strftime("%Y%m%d_%H%M")
        with open(arquivo_csv, 'a', newline='') as csvfile:
                colunas = ['EMPRESA','REGIAO', 'DATACENTER', 'ZONA', 'SERVIDOR','CPU','RAM_TOTAL','RAM_USADA','RAM_PERCENT','DISCO_TOTAL','DISCO_USADO','DISCO_PERCENT', 'LATENCIA', 'PACOTES_ENVIADOS', 'PACOTES_RECEBIDOS', 'PACOTES_PERDIDOS', 
                        'QTD_PR','PROCESSO1_CPU', 'PORCENTAGEM_PROCESSO1_CPU','PROCESSO2_CPU', 'PORCENTAGEM_PROCESSO2_CPU', 'PROCESSO3_CPU', 'PORCENTAGEM_PROCESSO3_CPU'
                        ,'PROCESSO1_RAM', 'PORCENTAGEM_PROCESSO1_RAM','PROCESSO2_RAM', 'PORCENTAGEM_PROCESSO2_RAM', 'PROCESSO3_RAM', 'PORCENTAGEM_PROCESSO3_RAM'  
                        ,'QTD_NUCLEOS', 'USO_USER', 'USO_SISTEM','BOOTTIME', 'DATA_HORA', 'DIA_SEMANA']
                CSV_DIC_WRITER = csv.DictWriter(csvfile, fieldnames=colunas, delimiter=';')

                if csvfile.tell() == 0:
                        CSV_DIC_WRITER.writeheader()

                #pegando os componentes normais
                cpu_usage = psutil.cpu_percent(interval=1)
                ram = psutil.virtual_memory() 
                disk = psutil.disk_usage("/")

                # pegando hora e contagem de processos
                tempo_agora = datetime.now()
                dia = tempo_agora.strftime('%D %H:%M:%S')
                dia_semana = tempo_agora.strftime('%A')
                tempo_agora = datetime.now()
                contagem_processos = len(psutil.pids())
                
                try:
                    #pegando rede
                    instancia_speed_test = speedtest.Speedtest(secure=True)
                    instancia_speed_test.get_best_server()
                    bootime = psutil.boot_time()
                except Exception as e:
                    print(f"Speedtest bloqueado ou sem conexão. Ping zerado. Erro: {e}")
                    ping = 0.0  # Valor padrão para o script não morrer!
                    
                ping = instancia_speed_test.results.ping
                dados_internet = psutil.net_io_counters()
                pacotes_enviados = dados_internet.packets_sent
                pacotes_recebidos = dados_internet.packets_recv
                pacotes_perdidos = dados_internet.errin + dados_internet.errout
                qtd_nucleos = psutil.cpu_count(logical=False)

                # pegando porcentagens especificas
                uso_cpu = psutil.cpu_times_percent(interval=1)
                uso_user = uso_cpu.user
                uso_sistema = uso_cpu.system

                # pegando processos com maior consumo
                lista_tres_ultimos = []

                for p in psutil.process_iter(['name', 'cpu_percent', 'memory_info', 'ppid']):
                    info = p.info
                    if info['name'] and info['memory_info'] is not None:
                        dados_enviar = {'nome': info['name'], 'cpu': info['cpu_percent'], 'memoria': info['memory_info'], 'pid': info['ppid']}
                        lista_tres_ultimos.append(dados_enviar)

                top_maior_processo_cpu= lista_tres_ultimos[0]
                top_segundo_processo_cpu = lista_tres_ultimos[1]
                top_terceiro_processo_cpu = lista_tres_ultimos[2]

                top_maior_processo_ram= lista_tres_ultimos[0]
                top_segundo_processo_ram = lista_tres_ultimos[1]
                top_terceiro_processo_ram = lista_tres_ultimos[2]

                for produto_agora in lista_tres_ultimos:
                    if (top_maior_processo_cpu['cpu'] < produto_agora['cpu']):
                        top_terceiro_processo_cpu = top_segundo_processo_cpu
                        top_segundo_processo_cpu = top_maior_processo_cpu
                        top_maior_processo_cpu= produto_agora

                    elif (top_segundo_processo_cpu['cpu'] < produto_agora['cpu']):
                        top_terceiro_processo_cpu = top_segundo_processo_cpu
                        top_segundo_processo_cpu = produto_agora

                    elif (top_terceiro_processo_cpu['cpu'] < produto_agora['cpu']):
                        top_terceiro_processo_cpu = produto_agora

                    if (top_maior_processo_ram['memoria'].rss < produto_agora['memoria'].rss):
                        top_terceiro_processo_ram = top_segundo_processo_ram
                        top_segundo_processo_ram = top_maior_processo_ram
                        top_maior_processo_ram= produto_agora

                    elif (top_segundo_processo_ram['memoria'].rss < produto_agora['memoria'].rss):
                        top_terceiro_processo_ram = top_segundo_processo_ram
                        top_segundo_processo_ram = produto_agora

                    elif (top_terceiro_processo_ram['memoria'].rss < produto_agora['memoria'].rss ):
                        top_terceiro_processo_ram = produto_agora
                print()
                dados_dict =  {
                    'EMPRESA': f'{servidor[0][16]}', 
                    'REGIAO': f'{servidor[0][12]}',
                    'DATACENTER': f'{servidor[0][21]}',
                    'ZONA': f'{servidor[0][6]}',
                    'SERVIDOR': f'{servidor[0][1]}', 
                    'CPU': cpu_usage * 2, 
                    'RAM_TOTAL': ram.total, 
                    'RAM_USADA': ram.used, 
                    'RAM_PERCENT': ram.percent, 
                    'DISCO_TOTAL': disk.total,
                    'DISCO_USADO': disk.used, 
                    'DISCO_PERCENT': disk.percent,
                    'LATENCIA': ping, 
                    'PACOTES_ENVIADOS': pacotes_enviados, 
                    'PACOTES_RECEBIDOS': pacotes_recebidos,
                    'PACOTES_PERDIDOS': pacotes_perdidos,  
                    'QTD_PR': contagem_processos,
                    'PROCESSO1_CPU': top_maior_processo_cpu['nome'], 
                    'PORCENTAGEM_PROCESSO1_CPU': top_maior_processo_cpu['cpu'],
                    'PROCESSO2_CPU': top_segundo_processo_cpu['nome'], 
                    'PORCENTAGEM_PROCESSO2_CPU': top_segundo_processo_cpu['cpu'], 
                    'PROCESSO3_CPU': top_terceiro_processo_cpu['nome'], 
                    'PORCENTAGEM_PROCESSO3_CPU': top_terceiro_processo_cpu['cpu'],
                    'PROCESSO1_RAM': top_maior_processo_ram['nome'], 
                    'PORCENTAGEM_PROCESSO1_RAM': top_maior_processo_ram['memoria'].rss,
                    'PROCESSO2_RAM': top_segundo_processo_ram['nome'], 
                    'PORCENTAGEM_PROCESSO2_RAM': top_segundo_processo_ram['memoria'].rss, 
                    'PROCESSO3_RAM': top_terceiro_processo_ram['nome'], 
                    'PORCENTAGEM_PROCESSO3_RAM': top_terceiro_processo_ram['memoria'].rss,
                    'QTD_NUCLEOS': qtd_nucleos,
                    'USO_USER':  uso_user,
                    'USO_SISTEM': uso_sistema,
                    'BOOTTIME': bootime,     
                    'DATA_HORA': tempo_agora,
                    'DIA_SEMANA': dia_semana
                    }
                
                print(f"""
                ----------------CAPTURANDO   DADOS ----------------
                      
                {tempo_agora}
                EMPRESA: {servidor[0][16]} 
                REGIAO: {servidor[0][12]}
                DATACENTER: {servidor[0][21]}
                ZONA: {servidor[0][6]}
                SERVIDOR: {servidor[0][1]}
                CPU: {cpu_usage}
                RAM: {ram.percent}
                DISCO: {disk.percent}
                LATENCIA {ping}
                PACOTES ENVIADOS: {pacotes_enviados}
                PACOTES RECEBIDOS: {pacotes_recebidos}
                QUANTIDADE DE PROCESSOS: {contagem_processos}
                PROCESSO COM MAIOR CONSUMO DE CPU: {top_maior_processo_cpu['nome']}  {top_maior_processo_cpu['cpu']}%
                PROCESSO COM MAIOR CONSUMO DE RAM: {top_maior_processo_ram['nome']}  {top_maior_processo_cpu['memoria'].rss}

                ----------------------------------------------------
            """)
                
                CSV_DIC_WRITER.writerow(dados_dict)
                csvfile.flush()

                nome_arquivo_s3 = f"raw/{dados_dict['EMPRESA']}_{dados_dict['DATACENTER']}_{dados_dict['ZONA']}_{dados_dict['SERVIDOR']}_{agora}_dadosBrutos.csv"
                upload_file(arquivo_csv, bucket_name, nome_arquivo_s3)

def capturaJson():
    conexao = mysql.connector.connect(
            host=banco_host,
            user=banco_user,
            password=banco_senha,
            database=banco_nome,
            port=banco_porta
    )

    cursor = conexao.cursor(dictionary=True)
    query = """
        SELECT
            e.idEmpresa,
            e.razaoSocial AS empresa,
            r.idRegiao,
            r.estado,
            r.cep,
            r.numero,
            r.complemento,
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
        datacenter = linha["datacenter"]
        zona = linha["zona"]
        servidor = linha["servidor"]
        componente = linha["componente"]

        if empresa not in estruturaGeral:
            estruturaGeral[empresa] = {}

        if datacenter not in estruturaGeral[empresa]:
            estruturaGeral[empresa][datacenter] = {}

        if zona not in estruturaGeral[empresa][datacenter]:
            estruturaGeral[empresa][datacenter][zona] = {}

        if servidor not in estruturaGeral[empresa][datacenter][zona]:
            estruturaGeral[empresa][datacenter][zona][servidor] = {"tipo": linha["tipoServidor"], "estado": linha["estadoServidor"],"limites": {}, "funcionarios": []}

        estruturaGeral[empresa][datacenter][zona][servidor]["limites"][componente] = linha["limite"]
        funcionario = linha["nomeAnalista"]

        if funcionario not in estruturaGeral[empresa][datacenter][zona][servidor]["funcionarios"]:
            estruturaGeral[empresa][datacenter][zona][servidor]["funcionarios"].append(funcionario)

    geral_json = json.dumps(
        estruturaGeral,
        ensure_ascii=False,
        indent=4,
        default=str
    )

    caminho_json = "metricas.json"

    with open(caminho_json, "w", encoding="utf-8") as arquivo:
        arquivo.write(geral_json)

    nome_arquivo_json = f"raw/metricas.json"
    upload_file(caminho_json, bucket_name, nome_arquivo_json)

servidor = validarServidor()

if servidor:
    cont = -1
    while True:
        capturaCSV(servidor)
        time.sleep(10)
        
        if(cont == 30 | cont == -1):
            cont = 0
            capturaJson()
        cont +=1
else:
    print("Erro na validação")



