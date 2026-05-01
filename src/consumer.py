import json
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import StringDeserializer

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

    # 3. Configurar o Desserializador JSON (Garante que vai ler o formato correto)
    json_deserializer = JSONDeserializer(
        schema_str,
        from_dict=lambda obj, ctx: obj
    )

    # 4. Configurar o Consumidor Kafka
    consumer_conf = {
        'bootstrap.servers': 'localhost:9092',
        'key.deserializer': StringDeserializer('utf_8'),
        'value.deserializer': json_deserializer,
        'group.id': 'grupo_analise_risco', # Nome do grupo de consumo
        'auto.offset.reset': 'earliest'    # Começa a ler desde a primeira mensagem disponível
    }
    
    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe([topic])

    print("Consumidor iniciado! Aguardando transações perfeitamente validadas...\n")

    try:
        while True:
            # Fica escutando o tópico continuamente
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            if msg.error():
                print(f" Erro no Kafka: {msg.error()}")
                continue

            # Extrai os dados validados
            chave = msg.key()
            transacao = msg.value()

            print(f"[PROCESSADA] Transação ID: {chave}")
            print(f" Valor: ${transacao['amt']} | Loja: {transacao['merchant']} | Cartão: {transacao['cc_num']}")
            print("-" * 50)

    except KeyboardInterrupt:
        print("\n Encerrando o consumidor...")
    finally:
        consumer.close()

if __name__ == '__main__':
    main()