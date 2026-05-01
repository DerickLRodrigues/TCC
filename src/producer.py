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
        print(f"Transação validada e entregue no tópico '{msg.topic()}' [Partição: {msg.partition()}]")

def main():
    topic = "transacoes_financeiras"

    # 1. LER O CONTRATO DE DADOS
    try:
        with open('schema.json', 'r') as f:
            schema_str = f.read()
    except FileNotFoundError:
        print("Erro: Arquivo schema.json não encontrado na pasta src/")
        return

    # 2. Configurar o Cliente do Schema Registry
    schema_registry_conf = {'url': 'http://localhost:8081'}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # 3. Configurar o Serializador JSON
    json_serializer = JSONSerializer(
        schema_str,
        schema_registry_client,
        to_dict=lambda obj, ctx: obj  # A mensagem é um dicionário 
    )

    # 4. Configurar o Produtor Kafka
    producer_conf = {
        'bootstrap.servers': 'localhost:9092',
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': json_serializer
    }
    producer = SerializingProducer(producer_conf)

    print("A iniciar a leitura do dataset ...")
    # Ler apenas as X linhas para o teste
    try:
        df = pd.read_csv('../data/credit_card_transactions.csv')
    except FileNotFoundError:
         print("Arquivo CSV não encontrado na pasta data/")
         return

    campos_obrigatorios = ["trans_num", "cc_num", "amt", "merchant"]
    
    # Memória local para evitar transações duplicadas (Simulando uma cache/banco)
    transacoes_processadas = set()

    
    # VARIÁVEIS PARA COLETA DE MÉTRICAS DO TCC
    tempo_inicio = time.time()
    total_processado = 0
    total_sucesso = 0
    total_bloqueado = 0
    total_duplicado = 0

    for index, row in df.iterrows():
        total_processado += 1
        id_transacao = str(row['trans_num'])

        print(f"\nA processar transação ID: {id_transacao}...")

        # REGRA DE NEGÓCIO 1: BLOQUEIO DE DUPLICATAS
        if id_transacao in transacoes_processadas:
            print(f"[DEDUPLICAÇÃO] Transação {id_transacao} bloqueada. Já foi enviada anteriormente.")
            total_duplicado += 1
            continue
        
        transacoes_processadas.add(id_transacao)
        
        # REGRA DE NEGÓCIO 2: CONFORMIDADE LGPD (Mascaramento)
        cc_raw = str(int(row['cc_num'])) 
        cc_mascarado = "*" * (len(cc_raw) - 4) + cc_raw[-4:]

        # Montagem do payload extraindo os dados do CSV
        mensagem = {
            "trans_num": id_transacao,
            "cc_num": cc_mascarado, # Enviando o dado já anonimizado
            "amt": float(row['amt']),
            "merchant": str(row['merchant']),
            "trans_date_trans_time": str(row['trans_date_trans_time'])
        }

        # Injeção de aleatoriedade para simular o erro (30% de chance)
        if random.random() < 0.30:
            campo_removido = random.choice(campos_obrigatorios)
            del mensagem[campo_removido]
            print(f"[SIMULAÇÃO DE ERRO] O campo '{campo_removido}' foi removido de propósito!")

        try:
            # O Serializer analisa o 'value' contra o Schema Registry antes de ir para a rede
            producer.produce(
                topic=topic,
                key=mensagem.get('trans_num', 'id_desconhecido'),
                value=mensagem,
                on_delivery=delivery_report
            )
            producer.poll(0)
            # Se a linha acima não falhar, significa que o Schema validou e passou!
            total_sucesso += 1
            # time.sleep(1) 

        except Exception as e:
            mensagem_erro = f"[BLOQUEIO] Transação {mensagem.get('trans_num', 'N/A')} rejeitada. Motivo: {e}"
            print(f"Error: {mensagem_erro}")
            logging.error(f"{mensagem_erro}") 
            # Se deu exceção, é porque a governança bloqueou a transação
            total_bloqueado += 1

    # Garante que todas as mensagens em fila são enviadas antes de fechar o script
    producer.flush()
    

    # CÁLCULO FINAL DAS MÉTRICAS E EXIBIÇÃO NO TERMINAL
    tempo_fim = time.time()
    duracao_segundos = tempo_fim - tempo_inicio
    
    # Cálculos matemáticos
    taxa_rejeicao = (total_bloqueado / total_processado) * 100 if total_processado > 0 else 0
    throughput = total_processado / duracao_segundos if duracao_segundos > 0 else 0

    print("\n" + "="*50)
    print("RELATÓRIO DE MÉTRICAS PARA O TCC")
    print("="*50)
    print(f"Total de Transações Lidas do CSV: {total_processado}")
    print(f"Transações Válidas (Sucesso): {total_sucesso}")
    print(f"Transações Barradas (Governança): {total_bloqueado}")
    if total_duplicado > 0:
        print(f"Transações Barradas (Duplicatas): {total_duplicado}")
    print(f"Taxa de Rejeição de Anomalias: {taxa_rejeicao:.2f}%")
    print(f"Tempo Total de Processamento: {duracao_segundos:.2f} segundos")
    print(f"Throughput Geral: {throughput:.2f} transações por segundo")
    print("="*50)

if __name__ == '__main__':
    main()