import pandas as pd
import json
import random
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer
import time
import logging

logging.basicConfig(
    filename='auditoria_governanca.log',
    filemode='a', 
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    encoding='utf-8'
)

def delivery_report(err, msg):
    """ Callback de monitorização: chamado quando a mensagem é entregue ou falha """
    if err is not None:
        print(f" Não foi possível entregar: {err}")
    else:
        pass # Removido o print por linha para não inundar o terminal em execuções longas

def exibir_relatorio(marco, total, sucesso, bloqueado, duplicado, tempo_inicio):
    """ Função auxiliar para calcular e exibir as métricas de forma limpa """
    tempo_atual = time.time()
    duracao_segundos = tempo_atual - tempo_inicio
    taxa_rejeicao = (bloqueado / total) * 100 if total > 0 else 0
    throughput = total / duracao_segundos if duracao_segundos > 0 else 0

    print("\n" + "="*50)
    print(f"RELATÓRIO PARCIAL: MARCO DE {marco} LINHAS")
    print("="*50)
    print(f"Total de Transações Lidas do CSV: {total}")
    print(f"Transações Válidas (Sucesso): {sucesso}")
    print(f"Transações Barradas (Governança): {bloqueado}")
    print(f"Transações Barradas (Duplicatas): {duplicado}")
    print(f"Taxa de Rejeição de Anomalias: {taxa_rejeicao:.2f}%")
    print(f"Tempo Acumulado de Processamento: {duracao_segundos:.2f} segundos")
    print(f"Throughput Geral Até Aqui: {throughput:.2f} transações por segundo")
    print("="*50 + "\n")

def main():
    topic = "transacoes_financeiras"

    # 1. LER O CONTRATO DE DADOS
    try:
        with open('schema.json', 'r') as f:
            schema_str = f.read()
    except FileNotFoundError:
        print("Erro: Arquivo schema.json não encontrado.")
        return

    # 2. Configurar o Cliente do Schema Registry
    schema_registry_conf = {'url': 'http://localhost:8081'}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # 3. Configurar o Serializador JSON
    json_serializer = JSONSerializer(
        schema_str,
        schema_registry_client,
        to_dict=lambda obj, ctx: obj 
    )

    # 4. Configurar o Produtor Kafka
    producer_conf = {
        'bootstrap.servers': 'localhost:9092',
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': json_serializer
    }
    producer = SerializingProducer(producer_conf)

    print("A iniciar a leitura do dataset completo...")
    try:
        # Lendo o arquivo completo (sem restrição de nrows)
        df = pd.read_csv('../data/credit_card_transactions.csv')
    except FileNotFoundError:
         print("Arquivo CSV não encontrado na pasta data/")
         return

    campos_obrigatorios = ["trans_num", "cc_num", "amt", "merchant"]
    transacoes_processadas = set()

    # DEFINIÇÃO DOS MARCOS DE COLETA DO TCC
    marcos_coleta = [100, 1000, 10000, 100000, 1296675]
    
    tempo_inicio = time.time()
    total_processado = 0
    total_sucesso = 0
    total_bloqueado = 0
    total_duplicado = 0

    for index, row in df.iterrows():
        total_processado += 1
        id_transacao = str(row['trans_num'])

        # REGRA DE NEGÓCIO 1: BLOQUEIO DE DUPLICATAS
        if id_transacao in transacoes_processadas:
            total_duplicado += 1
        else:
            transacoes_processadas.add(id_transacao)
            
            # REGRA DE NEGÓCIO 2: CONFORMIDADE LGPD (Mascaramento)
            cc_raw = str(int(row['cc_num'])) 
            cc_mascarado = "*" * (len(cc_raw) - 4) + cc_raw[-4:]

            # Montagem do payload
            mensagem = {
                "trans_num": id_transacao,
                "cc_num": cc_mascarado,
                "amt": float(row['amt']),
                "merchant": str(row['merchant']),
                "trans_date_trans_time": str(row['trans_date_trans_time'])
            }

            # Injeção de aleatoriedade para simular o erro (30% de chance)
            if random.random() < 0.30:
                campo_removido = random.choice(campos_obrigatorios)
                del mensagem[campo_removido]

            try:
                producer.produce(
                    topic=topic,
                    key=mensagem.get('trans_num', 'id_desconhecido'),
                    value=mensagem,
                    on_delivery=delivery_report
                )
                producer.poll(0)
                total_sucesso += 1

            except Exception as e:
                mensagem_erro = f"[BLOQUEIO] Transação {mensagem.get('trans_num', 'N/A')} rejeitada. Motivo: {e}"
                logging.error(f"{mensagem_erro}") 
                total_bloqueado += 1

        # VERIFICAÇÃO AUTOMÁTICA DOS MARCOS
        if total_processado in marcos_coleta:
            # Força o envio de mensagens pendentes na fila para garantir precisão nas métricas de tempo/vazão
            producer.flush() 
            exibir_relatorio(total_processado, total_processado, total_sucesso, total_bloqueado, total_duplicado, tempo_inicio)

    # Garantia final caso o número exato de linhas do arquivo varie ligeiramente do esperado
    producer.flush()
    if total_processado not in marcos_coleta:
        exibir_relatorio("FINAL (Fim do Arquivo)", total_processado, total_sucesso, total_bloqueado, total_duplicado, tempo_inicio)

if __name__ == '__main__':
    main()