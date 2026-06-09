import csv
from datetime import timedelta, datetime
import boto3
import os
from dotenv import load_dotenv
import random
import mysql.connector
import time

bucket_name = 'smartdatabucket3'

load_dotenv(".env.dev")
chave_acesso = os.getenv('aws_access_key_id')
chave_secreta = os.getenv('aws_secret_access_key')
token_sessao = os.getenv('aws_session_token')

def conectar_banco():
    """Conecta ao MySQL e busca todos os servidores ativos e sua hierarquia."""
    try:
        conexao = mysql.connector.connect(
            host=os.getenv("DB_HOST", "localhost"),
            user=os.getenv("DB_USER", "root"),
            password=os.getenv("DB_PASSWORD", "urubu100"),
            database=os.getenv("DB_NAME", "smartData")
        )
        cursor = conexao.cursor(dictionary=True)
        
        query = """
            SELECT 
                e.razaoSocial AS EMPRESA,
                r.uf AS REGIAO,
                d.nome AS DATACENTER,
                z.nome AS ZONA,
                s.nome AS SERVIDOR
            FROM servidor s
            JOIN zona z ON s.fkZona = z.idZona
            JOIN datacenter d ON z.fkDataCenter = d.idDataCenter
            JOIN regiao r ON r.fkRegiaoDataCenter = d.idDataCenter
            JOIN empresa e ON r.fkRegiaoEmpresa = e.idEmpresa
            WHERE s.estado = 'Ativo';
        """
        cursor.execute(query)
        servidores = cursor.fetchall()
        conexao.close()
        return servidores
    except Exception as e:
        print(f"Erro ao conectar no banco de dados: {e}")
        return []

def upload_file(file_name, bucket, object_name=None):
    session = boto3.client(
        's3', region_name='us-east-1',
        aws_access_key_id=chave_acesso,
        aws_secret_access_key=chave_secreta,
        aws_session_token=token_sessao
    )
    if object_name is None:
        object_name = file_name
    try:
        session.upload_file(file_name, bucket, object_name)
        print(f"  [OK] Enviado para o S3: {object_name}")
    except ValueError as e:
        print(f"  [ERRO] Falha ao enviar para o S3: {e}")
    return True

def get_fator_horario_suave(hora_decimal):
    pontos = [
        (0.0, 0.5), (4.0, 0.4), (8.0, 0.8), 
        (12.0, 1.2), (18.0, 1.8), (21.0, 1.9), 
        (23.99, 1.5), (24.0, 0.5)
    ]
    for i in range(len(pontos) - 1):
        h1, v1 = pontos[i]
        h2, v2 = pontos[i+1]
        if h1 <= hora_decimal <= h2:
            return v1 + (v2 - v1) * (hora_decimal - h1) / (h2 - h1)
    return 0.5

def main():
    print("=== GERADOR DE DADOS: LINHA DO TEMPO INTERCALADA ===")
    
    servidores_db = conectar_banco()
    if not servidores_db:
        print("Nenhum servidor encontrado ou falha na conexão. Encerrando.")
        return
        
    qtd_servidores = len(servidores_db)
    print(f"Encontrados {qtd_servidores} servidores ativos no banco de dados.")
    
    try:
        dias_simulacao = int(input("Por quantos dias para trás você deseja gerar os dados (ex: 7)? "))
        intervalo_minutos = int(input("Qual o intervalo (em minutos) entre cada geração de dados? "))
    except ValueError:
        print("Por favor, digite apenas números inteiros.")
        return

    data_final = datetime.now()
    data_inicial = data_final - timedelta(days=dias_simulacao)

    # Inicializa o "estado" e a lista de linhas para cada servidor separadamente
    estado_servidores = {}
    linhas_por_servidor = {}

    for s in servidores_db:
        nome_servidor = s['SERVIDOR']
        dt_boottime = data_inicial - timedelta(days=random.randint(2, 10))
        
        estado_servidores[nome_servidor] = {
            'cpu_anterior': random.uniform(10.0, 20.0),
            'disco_percent': random.uniform(40.0, 50.0),
            'ram_total': 64.0, 
            'disco_total': 2000.0,
            'qtd_nucleos': 16,
            'boottime': round(dt_boottime.timestamp(), 2) # Formato amigável para a Lambda
        }
        linhas_por_servidor[nome_servidor] = []

    colunas = [
        "EMPRESA", "REGIAO", "DATACENTER", "ZONA", "SERVIDOR", "CPU", "RAM_TOTAL", "RAM_USADA", "RAM_PERCENT", 
        "DISCO_TOTAL", "DISCO_USADO", "DISCO_PERCENT", "LATENCIA", "PACOTES_ENVIADOS", "PACOTES_RECEBIDOS", 
        "PACOTES_PERDIDOS", "QTD_PR", "PROCESSO1_CPU", "PORCENTAGEM_PROCESSO1_CPU", "PROCESSO2_CPU", 
        "PORCENTAGEM_PROCESSO2_CPU", "PROCESSO3_CPU", "PORCENTAGEM_PROCESSO3_CPU", "PROCESSO1_RAM", 
        "PORCENTAGEM_PROCESSO1_RAM", "PROCESSO2_RAM", "PORCENTAGEM_PROCESSO2_RAM", "PROCESSO3_RAM", 
        "PORCENTAGEM_PROCESSO3_RAM", "QTD_NUCLEOS", "USO_USER", "USO_SISTEM", "BOOTTIME", "DATA_HORA", "DIA_SEMANA"
    ]
    nomes_dias = ["SEGUNDA", "TERCA", "QUARTA", "QUINTA", "SEXTA", "SABADO", "DOMINGO"]

    print(f"\nGerando linha do tempo de {data_inicial.strftime('%d/%m %H:%M')} até {data_final.strftime('%d/%m %H:%M')}...")
    
    data_atual = data_inicial
    idx_servidor = 0

    # Avança no tempo de X em X minutos. A cada avanço, gera dado para UM servidor diferente.
    while data_atual <= data_final:
        # Pega o servidor da vez (Round-Robin)
        s = servidores_db[idx_servidor]
        nome_servidor = s['SERVIDOR']
        est = estado_servidores[nome_servidor]
        
        # Move o índice para o próximo servidor na próxima rodada do loop
        idx_servidor = (idx_servidor + 1) % qtd_servidores

        dia_semana_idx = data_atual.weekday()
        dia_semana_nome = nomes_dias[dia_semana_idx]
        hora = data_atual.hour
        hora_decimal = hora + (data_atual.minute / 60.0) 
        
        em_manutencao = False
        if dia_semana_idx == 1 and 18 <= hora <= 20:
            if hora == 19: em_manutencao = True

        fator_horario = get_fator_horario_suave(hora_decimal)
        fator_dia = 1.5 if dia_semana_idx in [0, 1] else (0.7 if dia_semana_idx == 2 else (2.0 if dia_semana_idx in [3, 4] else 1.1))

        pico_extra = 1.0
        if dia_semana_idx == 4 and (hora >= 17 or hora < 1): pico_extra = 1.3 
        elif dia_semana_idx == 5 and (hora >= 14 or hora < 2): pico_extra = 2.8 
        elif dia_semana_idx == 6 and (14 <= hora <= 23): pico_extra = 1.8 

        if em_manutencao:
            latencia = random.randint(200, 500)
            pacotes_perdidos = random.randint(15, 35)
            disco_percent_atual = random.uniform(40.0, 42.0)
            cpu_alvo = random.uniform(2.0, 8.0)
        else:
            latencia = random.randint(8, 25) + int(est['cpu_anterior'] * 0.1)
            pacotes_perdidos = 0
            incremento_disco = (0.5 / 60) * intervalo_minutos
            disco_percent_atual = est['disco_percent'] + incremento_disco + random.uniform(-0.02, 0.05)
            cpu_alvo = 10.0 * fator_horario * fator_dia * pico_extra
            
        disco_percent_atual = min(max(disco_percent_atual, 5.0), 99.9)
        disco_usado = est['disco_total'] * (disco_percent_atual / 100)
        
        cpu = est['cpu_anterior'] + ((cpu_alvo - est['cpu_anterior']) * 0.3) + random.uniform(-1.5, 1.5)
        cpu = min(max(cpu, 1.0), 100.0) 
        
        # Atualiza a memória de recursos DO SERVIDOR DA VEZ
        est['cpu_anterior'] = cpu
        est['disco_percent'] = disco_percent_atual
        
        uso_user = cpu * random.uniform(0.65, 0.85)
        uso_sistem = cpu - uso_user

        ram_percent = min(35.0 + random.uniform(-2.0, 2.0) + (cpu * 0.4), 98.0)
        ram_usada = est['ram_total'] * (ram_percent / 100)
        
        multiplicador_rede = cpu * 100
        pacotes_enviados = int(multiplicador_rede * random.uniform(0.9, 1.1))
        pacotes_recebidos = int(multiplicador_rede * random.uniform(0.9, 1.1))
        if em_manutencao:
            pacotes_enviados = int(pacotes_enviados * 0.2)
            pacotes_recebidos = int(pacotes_recebidos * 0.2)

        qtd_pr = int(100 + (cpu * 1.5) + random.randint(-10, 10))
        top_procs = random.sample(["steamcmd", "srcds_linux", "mysqld", "nginx", "docker", "java"], 3)

        linha = [
            s['EMPRESA'], s['REGIAO'], s['DATACENTER'], s['ZONA'], nome_servidor,
            round(cpu, 2), round(est['ram_total'], 2), round(ram_usada, 2), round(ram_percent, 2),
            round(est['disco_total'], 2), round(disco_usado, 2), round(disco_percent_atual, 2),
            latencia, pacotes_enviados, pacotes_recebidos, pacotes_perdidos,
            qtd_pr, 
            top_procs[0], round(cpu * random.uniform(0.2, 0.35), 2),
            top_procs[1], round(cpu * random.uniform(0.1, 0.2), 2),
            top_procs[2], round(cpu * random.uniform(0.05, 0.1), 2),
            top_procs[0], round(ram_percent * random.uniform(0.15, 0.25), 2),
            top_procs[1], round(ram_percent * random.uniform(0.1, 0.15), 2),
            top_procs[2], round(ram_percent * random.uniform(0.05, 0.1), 2),
            est['qtd_nucleos'], round(uso_user, 2), round(uso_sistem, 2),
            est['boottime'], data_atual.strftime("%Y-%m-%d %H:%M:%S"), dia_semana_nome
        ]
        
        # Guarda a linha no "bolso" específico desse servidor
        linhas_por_servidor[nome_servidor].append(linha)
        
        # Avança o tempo
        data_atual += timedelta(minutes=intervalo_minutos)

    print("\nSalvando arquivos e enviando para o S3 com delay (evitar gargalo na Lambda)...")
    
    # Após a simulação, grava e envia cada arquivo individualmente
    for s in servidores_db:
        nome_servidor = s['SERVIDOR']
        zona_formatada = s['ZONA'].replace(" ", "_")
        
        nome_arquivo_local = f"{s['EMPRESA']}_{s['DATACENTER']}_{zona_formatada}_{nome_servidor}_dadosBrutos.csv"
        nome_arquivo_s3 = f"raw/{s['EMPRESA']}_{s['DATACENTER']}_{zona_formatada}_{nome_servidor}_dadosBrutos.csv"
        
        with open(nome_arquivo_local, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=';')
            writer.writerow(colunas)
            writer.writerows(linhas_por_servidor[nome_servidor])
            
        print(f" -> Servidor {nome_servidor} gerou {len(linhas_por_servidor[nome_servidor])} linhas intercaladas.")
        upload_file(nome_arquivo_local, bucket_name, nome_arquivo_s3)
        
        # Mantém a pausa de segurança para a Lambda de tratamento processar sem atropelos
        time.sleep(6)

    print("\nConcluído! Todos os servidores foram processados com sucesso.")
    
if __name__ == "__main__":
    main()