import pandas as pd
import json
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer
import time

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
    # Ler apenas as primeiras 10 linhas para o teste, pode ser comentado esse trecho
    try:
        df = pd.read_csv('../data/credit_card_transactions.csv', nrows=10)
    except FileNotFoundError:
         print("Arquivo CSV não encontrado na pasta data/")
         return

    for index, row in df.iterrows():
        # Montagem do payload extraindo os dados do CSV
        mensagem = {
            "trans_num": str(row['trans_num']),
            "cc_num": float(row['cc_num']),
           "amt": float(row['amt']),    # podemos qualquer linha para forcar um erro
            "merchant": str(row['merchant']),
            "trans_date_trans_time": str(row['trans_date_trans_time'])
        }

        print(f"\nA processar transação ID: {mensagem['trans_num']}...")

        try:
            # O Serializer analisa o 'value' contra o Schema Registry ANTES de ir para a rede
            producer.produce(
                topic=topic,
                key=mensagem['trans_num'], # o ID da transação como chave de partição
                value=mensagem,
                on_delivery=delivery_report
            )
            # A função poll() força o Kafka a verificar e disparar os callbacks pendentes
            producer.poll(0)
            time.sleep(1) # Pausa de 1 segundo para ver a execução no terminal, pode ser maior ou menor no final

        except Exception as e:
            # Aqui rola os bloqueios por motivo do schema registry (parte da governancia)
            print(f"[BLOQUEIO DE Schema] O Schema Registry rejeitou a transação: {e}")

    # Garante que todas as mensagens em fila são enviadas antes de fechar o script
    producer.flush()
    print("\n Fluxo de ingestão concluído!")

if __name__ == '__main__':
    main()