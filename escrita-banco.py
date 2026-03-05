import datetime
import psutil
import pandas as pd
import os

idRegistro = 1
fkServidor = 1 # Resposta do banco
fkComponente = 1 # Resposta do banco
momento = datetime.datetime.now() # Data e horário da máquina

# Verificar fkComponente no banco de dados e saber qual seu tipo
tipoComponente = "VENTOINHA"

if tipoComponente == "CPU":
    valor = psutil.cpu_freq().current # Uso da CPU em MHz

elif tipoComponente == "RAM":
    valor = psutil.virtual_memory().used # Uso da RAM em bytes

elif tipoComponente == 'DISCO':
    valor = psutil.disk_usage('C:/').used # Uso da Disco em bytes

elif tipoComponente == 'REDE':
    valor = psutil.net_if_stats()['Wi-Fi'].speed # Velocidade da rede em MB

elif tipoComponente == 'VENTOINHA':
    valor = psutil.sensors_fans() 

print(valor)

registroAtual = {
    'idRegistro': [idRegistro],
    'fkServidor': [fkServidor],
    'fkComponente': [fkComponente],
    'horarioData': [momento],
    'valor': [valor],
}

    
# print("Registro de ",momento, " armazenado!")

df = pd.DataFrame(registroAtual)
# mode=a para adicionar nova linha e header=not para não adicionar o header novamente
df.to_csv("dadosMonitorados.csv", mode='a', header=not os.path.exists("dadosMonitorados.csv"), index=False)