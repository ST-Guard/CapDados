import csv
import random
from datetime import datetime, timedelta


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
    print("=== GERADOR DE DADOS (ORGÂNICO) ===")
    
    try:
        dias_simulacao = int(input("Por quantos dias para trás você deseja gerar os dados? "))
        intervalo_minutos = int(input("Qual o intervalo (em minutos) entre cada geração de dados? "))
    except ValueError:
        print("Por favor, digite apenas números inteiros.")
        return

    
    data_final = datetime.now()
    data_inicial = data_final - timedelta(days=dias_simulacao)
    data_atual = data_inicial
    
    
    empresa = "Steam"
    regiao = "Steam Sp"
    datacenter = "ST-SP-01"
    zona = "Zona A"
    servidor = "SERVIDOR-DC01-WEB-05"
    ram_total = 64.0
    disco_total = 2000.0
    qtd_nucleos = 16
    boottime = (data_inicial - timedelta(days=5)).strftime("%Y-%m-%d %H:%M:%S") 
    
    
    disco_percent = random.uniform(40.0, 50.0)
    cpu_anterior = 15.0 
    
    linhas_csv = []
    
    colunas = [
        "EMPRESA", "REGIAO", "DATACENTER", "ZONA", "SERVIDOR", "CPU", "RAM_TOTAL", "RAM_USADA", "RAM_PERCENT", 
        "DISCO_TOTAL", "DISCO_USADO", "DISCO_PERCENT", "LATENCIA", "PACOTES_ENVIADOS", "PACOTES_RECEBIDOS", 
        "PACOTES_PERDIDOS", "QTD_PR", "PROCESSO1_CPU", "PORCENTAGEM_PROCESSO1_CPU", "PROCESSO2_CPU", 
        "PORCENTAGEM_PROCESSO2_CPU", "PROCESSO3_CPU", "PORCENTAGEM_PROCESSO3_CPU", "PROCESSO1_RAM", 
        "PORCENTAGEM_PROCESSO1_RAM", "PROCESSO2_RAM", "PORCENTAGEM_PROCESSO2_RAM", "PROCESSO3_RAM", 
        "PORCENTAGEM_PROCESSO3_RAM", "QTD_NUCLEOS", "USO_USER", "USO_SISTEM", "BOOTTIME", "DATA_HORA", "DIA_SEMANA"
    ]
    
    nomes_dias = ["SEGUNDA", "TERCA", "QUARTA", "QUINTA", "SEXTA", "SABADO", "DOMINGO"]
    
    print("\nGerando dados....")

    while data_atual <= data_final:
        dia_semana_idx = data_atual.weekday()
        dia_semana_nome = nomes_dias[dia_semana_idx]
        hora = data_atual.hour
        hora_decimal = hora + (data_atual.minute / 60.0) 
        
        
        em_manutencao = False
        if dia_semana_idx == 1 and 18 <= hora <= 20:
            if hora == 19: 
                em_manutencao = True

        if em_manutencao:
            latencia = random.randint(200, 500)
            pacotes_perdidos = random.randint(15, 35)
        else:
            
            latencia = random.randint(8, 25) + int(cpu_anterior * 0.1)
            pacotes_perdidos = 0
            
        
        incremento_disco = (0.5 / 60) * intervalo_minutos
        disco_percent += incremento_disco + random.uniform(-0.02, 0.05)
        
        if em_manutencao:
            disco_percent = random.uniform(40.0, 42.0)
            
        disco_percent = min(disco_percent, 99.9)
        disco_usado = disco_total * (disco_percent / 100)
        
        
        fator_horario = get_fator_horario_suave(hora_decimal)

        
        if dia_semana_idx in [0, 1]: fator_dia = 1.5
        elif dia_semana_idx == 2: fator_dia = 0.7
        elif dia_semana_idx in [3, 4]: fator_dia = 2.0
        else: fator_dia = 1.1

        
        pico_extra = 1.0
        if dia_semana_idx == 4 and (hora >= 17 or hora < 1): 
            pico_extra = 1.3 
        elif dia_semana_idx == 5 and (hora >= 14 or hora < 2): 
            pico_extra = 2.8 
        elif dia_semana_idx == 6 and (14 <= hora <= 23): 
            pico_extra = 1.8 
            
        
        cpu_alvo = 10.0 * fator_horario * fator_dia * pico_extra
        
        if em_manutencao:
            cpu_alvo = random.uniform(2.0, 8.0)
            
        
        
        cpu = cpu_anterior + ((cpu_alvo - cpu_anterior) * 0.3) + random.uniform(-1.5, 1.5)
        cpu = min(max(cpu, 1.0), 100.0) 
        cpu_anterior = cpu 
        
        uso_user = cpu * random.uniform(0.65, 0.85)
        uso_sistem = cpu - uso_user

        
        ram_percent_base = 35.0 + random.uniform(-2.0, 2.0)
        ram_percent = ram_percent_base + (cpu * 0.4) 
        ram_percent = min(ram_percent, 98.0)
        ram_usada = ram_total * (ram_percent / 100)
        
        
        multiplicador_rede = cpu * 100
        pacotes_enviados = int(multiplicador_rede * random.uniform(0.9, 1.1))
        pacotes_recebidos = int(multiplicador_rede * random.uniform(0.9, 1.1))
        if em_manutencao:
            pacotes_enviados = int(pacotes_enviados * 0.2)
            pacotes_recebidos = int(pacotes_recebidos * 0.2)

        
        qtd_pr = int(100 + (cpu * 1.5) + random.randint(-10, 10))
        
        processos_nomes = ["steamcmd", "srcds_linux", "mysqld", "nginx", "docker", "java"]
        top_procs = random.sample(processos_nomes, 3)
        
        p1_cpu_pct = cpu * random.uniform(0.2, 0.35)
        p2_cpu_pct = cpu * random.uniform(0.1, 0.2)
        p3_cpu_pct = cpu * random.uniform(0.05, 0.1)
        
        p1_ram_pct = ram_percent * random.uniform(0.15, 0.25)
        p2_ram_pct = ram_percent * random.uniform(0.1, 0.15)
        p3_ram_pct = ram_percent * random.uniform(0.05, 0.1)

        linha = [
            empresa, regiao, datacenter, zona, servidor,
            round(cpu, 2), round(ram_total, 2), round(ram_usada, 2), round(ram_percent, 2),
            round(disco_total, 2), round(disco_usado, 2), round(disco_percent, 2),
            latencia, pacotes_enviados, pacotes_recebidos, pacotes_perdidos,
            qtd_pr, 
            top_procs[0], round(p1_cpu_pct, 2),
            top_procs[1], round(p2_cpu_pct, 2),
            top_procs[2], round(p3_cpu_pct, 2),
            top_procs[0], round(p1_ram_pct, 2),
            top_procs[1], round(p2_ram_pct, 2),
            top_procs[2], round(p3_ram_pct, 2),
            qtd_nucleos, round(uso_user, 2), round(uso_sistem, 2),
            boottime, data_atual.strftime("%Y-%m-%d %H:%M:%S"), dia_semana_nome
        ]
        
        linhas_csv.append(linha)
        data_atual += timedelta(minutes=intervalo_minutos)

    with open("simulacao_servidores.csv", mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(colunas)
        writer.writerows(linhas_csv)

    print(f"\nConcluído! Arquivo simulacao_servidores gerado com sucesso com {len(linhas_csv)} registros.")
    print(f"Último dado gerado corresponde exatamente a: {data_final.strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()