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
    filename='auditoria_governança.log',
    filemode='a', 
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
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
    # Ler apenas as primeiras 50 linhas para o teste, pode ser comentado esse trecho
    try:
        df = pd.read_csv('../data/credit_card_transactions.csv', nrows=50)
    except FileNotFoundError:
         print("Arquivo CSV não encontrado na pasta data/")
         return

    campos_obrigatorios = ["trans_num", "cc_num", "amt", "merchant"]

    campos_obrigatorios = ["trans_num", "cc_num", "amt", "merchant"]
    
    # Memória local para evitar transações duplicadas (Simulando uma cache/banco)
    transacoes_processadas = set()

    for index, row in df.iterrows():
        id_transacao = str(row['trans_num'])

        print(f"\nA processar transação ID: {id_transacao}...")

        # REGRA DE NEGÓCIO 1: BLOQUEIO DE DUPLICATAS
        if id_transacao in transacoes_processadas:
            print(f"[DEDUPLICAÇÃO] Transação {id_transacao} bloqueada. Já foi enviada anteriormente.")
            continue
        transacoes_processadas.add(id_transacao)

        
        # REGRA DE NEGÓCIO 2: CONFORMIDADE LGPD (Mascaramento Shift-Left)
        # Pegamos o número bruto, removemos decimais, e substituímos tudo por '*' exceto os 4 últimos
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
            time.sleep(1)

        except Exception as e:
            mensagem_erro = f"[BLOQUEIO] Transação {mensagem.get('trans_num', 'N/A')} rejeitada. Motivo: {e}"
            print(f"Error: {mensagem_erro}")
            logging.error(f"{mensagem_erro}") 

    # Garante que todas as mensagens em fila são enviadas antes de fechar o script
    producer.flush()
    print("\n Fluxo de ingestão concluído!")

if __name__ == '__main__':
    main()