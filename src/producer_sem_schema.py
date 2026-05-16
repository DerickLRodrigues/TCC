import pandas as pd
import json
import random
from confluent_kafka import Producer
import time
import logging

logging.basicConfig(
    filename='auditoria_sem_governanca.log',
    filemode='a', 
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    encoding='utf-8'
)

def delivery_report(err, msg):
    if err is not None:
        pass 

def salvar_relatorio(arquivo_txt, rodada, marco, total, sucesso, bloqueado, duplicado, tempo_inicio):
    """ Calcula as métricas do cenário SEM GOVERNANÇA e grava no TXT """
    tempo_atual = time.time()
    duracao_segundos = tempo_atual - tempo_inicio
    taxa_rejeicao = (bloqueado / total) * 100 if total > 0 else 0
    throughput = total / duracao_segundos if duracao_segundos > 0 else 0

    linhas_relatorio = [
        "==================================================",
        f"CENÁRIO: SEM GOVERNANÇA (SEM SCHEMA_REGISTRY)",
        f"EXECUÇÃO NÚMERO: {rodada} | MARCO: {marco} LINHAS",
        "==================================================",
        f"Total de Transações Lidas: {total}",
        f"Transações Enviadas (Sucesso): {sucesso}",
        f"Transações Barradas (Governança): {bloqueado}",
        f"Transações Ignoradas (Duplicatas): {duplicado}",
        f"Taxa de Rejeição de Anomalias: {taxa_rejeicao:.2f}%",
        f"Tempo Acumulado: {duracao_segundos:.2f} segundos",
        f"Throughput: {throughput:.2f} transações por segundo",
        "==================================================\n"
    ]
    
    conteudo_final = "\n".join(linhas_relatorio)
    
    with open(arquivo_txt, 'a', encoding='utf-8') as f:
        f.write(conteudo_final)

def main():
    topic = "transacoes_financeiras"
    arquivo_resultados = "resultados_benchmark_sem_governanca.txt"

    # Inicializa o arquivo de texto limpo
    with open(arquivo_resultados, 'w', encoding='utf-8') as f:
        f.write("=== INÍCIO DO BENCHMARK - CENÁRIO SEM GOVERNANÇA ===\n")

    # Configuração do Producer Raiz (Sem serializadores complexos)
    producer_conf = {
        'bootstrap.servers': 'localhost:9092'
    }
    producer = Producer(producer_conf)

    print("Carregando o dataset completo na memória (aguarde)...")
    try:
        df = pd.read_csv('../data/credit_card_transactions.csv')
    except FileNotFoundError:
         print("Arquivo CSV não encontrado na pasta data/")
         return

    campos_obrigatorios = ["trans_num", "cc_num", "amt", "merchant"]
    marcos_coleta = [100, 1000, 10000, 100000, 1296675]
    
    total_rodadas = 10
    print(f"Dataset carregado. Iniciando as {total_rodadas} rodadas SEM GOVERNANÇA...")

    for rodada in range(1, total_rodadas + 1):
        print(f" -> Executando rodada {rodada}/{total_rodadas}...")
        
        transacoes_processadas = set()
        tempo_inicio = time.time()
        total_processado = 0
        total_sucesso = 0
        total_bloqueado = 0
        total_duplicado = 0

        for index, row in df.iterrows():
            total_processado += 1
            id_transacao = str(row['trans_num'])

            # REGRA 1: DEDUPLICAÇÃO (Mantida para o teste ser justo)
            if id_transacao in transacoes_processadas:
                total_duplicado += 1
            else:
                transacoes_processadas.add(id_transacao)
                
                # REGRA 2: MASCARAMENTO LGPD (Mantido para o teste ser justo)
                cc_raw = str(int(row['cc_num'])) 
                cc_mascarado = "*" * (len(cc_raw) - 4) + cc_raw[-4:]

                mensagem = {
                    "trans_num": id_transacao,
                    "cc_num": cc_mascarado,
                    "amt": float(row['amt']),
                    "merchant": str(row['merchant']),
                    "trans_date_trans_time": str(row['trans_date_trans_time'])
                }

                # Simulação de quebra de contrato (30% de chance)
                if random.random() < 0.30:
                    campo_removido = random.choice(campos_obrigatorios)
                    del mensagem[campo_removido]

                try:
                    # Serialização manual para JSON string pura antes de injetar na rede
                    payload_bytes = json.dumps(mensagem).encode('utf-8')
                    
                    producer.produce(
                        topic=topic,
                        key=mensagem.get('trans_num', 'id_desconhecido'),
                        value=payload_bytes,
                        on_delivery=delivery_report
                    )
                    producer.poll(0)
                    total_sucesso += 1

                except Exception as e:
                    # Sem o Schema Registry, isso aqui só vai disparar se o broker cair ou estourar a memória
                    logging.error(f"[ERRO REDE/BROKER]: {e}") 
                    total_bloqueado += 1

            if total_processado in marcos_coleta:
                producer.flush() 
                salvar_relatorio(arquivo_resultados, rodada, total_processado, total_processado, total_sucesso, total_bloqueado, total_duplicado, tempo_inicio)

        producer.flush()
        if total_processado not in marcos_coleta:
            salvar_relatorio(arquivo_resultados, rodada, "FINAL", total_processado, total_sucesso, total_bloqueado, total_duplicado, tempo_inicio)

    print(f"\n[SUCESSO] Benchmark sem governança finalizado! Resultados salvos em '{arquivo_resultados}'")

if __name__ == '__main__':
    main()