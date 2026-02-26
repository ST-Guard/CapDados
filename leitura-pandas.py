import pandas as pd
import os

registro = pd.read_csv("dadosMonitorados.csv")

dataHora = pd.to_datetime(registro["horarioData"], dayfirst=True) # Transformando coluna horarioData em tipo datetime
limite = pd.Timestamp.now() - pd.offsets.Minute(5) # Pegando a ultima uma hora com a função .offsets (lida com viradas de hora, dia e ate ano)

registroUltimo10min = registro[dataHora >= limite]

MHzParaGHz = 1 / 1000
bytesParaGB = 1 / 1024 ** 3

if registroUltimo10min.empty:
    print("Nenhum dado encontrado nos últimos 10 minutos")

else:
    print("Nos últimos 10 minutos:")

    mediaCpuUso = registroUltimo10min["cpuUsoFreq"].mean()
    mediaCpuUso *= MHzParaGHz
    print(f"Média de uso em GHz da Cpu: {mediaCpuUso:.2f} GHz")

    mediaRamUso = registroUltimo10min["ramUso"].mean() # Descobrindo a media com a função .mean()
    mediaRamUso *= bytesParaGB
    print(f"Média de uso em GB da memória RAM: {mediaRamUso:.2f} GB")

    mediaRamPorcUso = registroUltimo10min["ramPorcUso"].mean()
    print(f"Média de uso em porcentagem da memória RAM: {mediaRamPorcUso:.2f}%")

    mediaDiscoUso = registroUltimo10min["discoUso"].mean()
    mediaDiscoUso *= bytesParaGB
    print(f"Média de uso em GB do Disco: {mediaDiscoUso:.2f} GB")

    mediaDiscoPorcUso = registroUltimo10min["discoPorcUso"].mean()
    print(f"Média de uso em porcentagem do Disco: {mediaDiscoPorcUso:.2f}%")

    # Criando outro .csv
    registroNovo = {
            'mediaCpuUso': [mediaCpuUso],
            'mediaRamUso': [mediaRamUso],
            'mediaRamPorcUso': [mediaRamPorcUso],
            'mediaDiscoUso': [mediaDiscoUso],
            'mediaDiscoPorcUso': [mediaDiscoPorcUso]
        }

    df = pd.DataFrame(registroNovo)
    df.to_csv("dadosTratados.csv", mode='a', header= os.path.exists("dadosMonitorados.csv"), index=False)