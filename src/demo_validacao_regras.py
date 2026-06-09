"""
demo_validacao_regras.py
Demonstração isolada das regras de governança definidas em schema.json.

Diferente do producer.py — que lê a base do Kaggle e injeta apenas a anomalia de
"campo obrigatório ausente" — este script NÃO toca na base de dados. Ele monta um
conjunto pequeno de mensagens sintéticas, escritas à mão, para exercitar TODAS as
regras do contrato: máscara LGPD do cc_num, limites de valor, formato de data e
tamanho mínimo de texto, além da deduplicação por trans_num.

Cada mensagem passa pelo MESMO SerializingProducer + Schema Registry usado em
producer.py. Ou seja, a rejeição aqui é exatamente a mesma barreira de governança
preventiva avaliada no experimento — só que isolando cada regra individualmente.

Pré-requisito: stack Docker no ar  ->  docker-compose up -d
Execução (de dentro da pasta src/):  python demo_validacao_regras.py
"""
import logging
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer

logging.basicConfig(
    filename='auditoria_demo.log',
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    encoding='utf-8'
)

TOPICO = "transacoes_demo"

# Transação de referência: todos os campos em conformidade com o schema.json.
# Cada caso de teste parte dela e quebra exatamente UMA regra.
TRANSACAO_VALIDA = {
    "trans_num": "DEMO00000001",
    "cc_num": "************3456",
    "amt": 150.75,
    "merchant": "Loja Exemplo",
    "trans_date_trans_time": "2019-01-01 00:00:18",
}


def construir_casos():
    """Lista de (descrição, mensagem, resultado_esperado)."""
    casos = [
        (
            "Transação válida (todos os campos em conformidade)",
            dict(TRANSACAO_VALIDA),
            "ACEITA",
        ),
        (
            "Cartão SEM máscara (viola regex LGPD do cc_num)",
            dict(TRANSACAO_VALIDA, trans_num="DEMO00000002", cc_num="1234567890123456"),
            "BLOQUEADA",
        ),
        (
            "Valor negativo (viola amt exclusiveMinimum 0)",
            dict(TRANSACAO_VALIDA, trans_num="DEMO00000003", amt=-50.0),
            "BLOQUEADA",
        ),
        (
            "Valor acima do teto (viola amt maximum 500.000)",
            dict(TRANSACAO_VALIDA, trans_num="DEMO00000004", amt=999999.0),
            "BLOQUEADA",
        ),
        (
            "Estabelecimento curto (viola merchant minLength 3)",
            dict(TRANSACAO_VALIDA, trans_num="DEMO00000005", merchant="AB"),
            "BLOQUEADA",
        ),
        (
            "Data fora do formato YYYY-MM-DD HH:MM:SS",
            dict(TRANSACAO_VALIDA, trans_num="DEMO00000006", trans_date_trans_time="01/01/2019"),
            "BLOQUEADA",
        ),
        (
            "ID de transação curto (viola trans_num minLength 10)",
            dict(TRANSACAO_VALIDA, trans_num="CURTO"),
            "BLOQUEADA",
        ),
    ]

    # Campo obrigatório ausente: a anomalia já avaliada em escala no benchmark.
    sem_campo = dict(TRANSACAO_VALIDA, trans_num="DEMO00000007")
    del sem_campo["amt"]
    casos.append(("Campo obrigatório ausente (amt removido)", sem_campo, "BLOQUEADA"))

    return casos


def main():
    try:
        with open('schema.json', 'r') as f:
            schema_str = f.read()
    except FileNotFoundError:
        print("Erro: schema.json não encontrado. Rode este script de dentro da pasta src/.")
        return

    sr_client = SchemaRegistryClient({'url': 'http://localhost:8081'})
    json_serializer = JSONSerializer(schema_str, sr_client, to_dict=lambda obj, ctx: obj)
    producer = SerializingProducer({
        'bootstrap.servers': 'localhost:9092',
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': json_serializer,
    })

    def enviar(mensagem):
        """Serializa (valida no Schema Registry) e publica. Levanta exceção se inválida."""
        producer.produce(
            topic=TOPICO,
            key=str(mensagem.get('trans_num', 'sem-id')),
            value=mensagem,
        )
        producer.flush()

    def motivo_da_falha(exc):
        causa = getattr(exc, '__cause__', None) or exc
        texto = str(causa).strip()
        return texto.splitlines()[0] if texto else type(exc).__name__

    print("=" * 66)
    print(" DEMONSTRAÇÃO DAS REGRAS DE GOVERNANÇA (schema.json)")
    print(" Mesma barreira do producer.py: SerializingProducer + Schema Registry")
    print("=" * 66)

    # Health check: se nem a transação válida passar, o problema é de infraestrutura.
    try:
        enviar(dict(TRANSACAO_VALIDA, trans_num="HEALTHCHECK01"))
    except Exception as e:
        print("\n[ERRO DE INFRAESTRUTURA] Não consegui enviar nem a transação válida.")
        print("Verifique se a stack está no ar:  docker-compose up -d")
        print(f"Detalhe: {e}")
        return

    casos = construir_casos()
    acertos = 0

    for i, (descricao, mensagem, esperado) in enumerate(casos, start=1):
        try:
            enviar(mensagem)
            resultado, motivo = "ACEITA", ""
        except Exception as e:
            resultado, motivo = "BLOQUEADA", motivo_da_falha(e)

        ok = resultado == esperado
        acertos += ok
        print(f"\n[{i:>2}] {descricao}")
        print(f"     Esperado: {esperado:<9} | Resultado: {resultado:<9} [{'OK' if ok else 'FALHOU'}]")
        if motivo:
            print(f"     Motivo: {motivo}")
        logging.info(f"[{resultado}] {descricao} | trans_num={mensagem.get('trans_num', 'N/A')} | {motivo}")

    # Deduplicação: não é regra de schema; é o controle em memória do producer.py.
    print("\n" + "-" * 66)
    print(" Deduplicação por trans_num (controle em memória, igual ao producer.py)")
    print("-" * 66)
    dedup_set = set()
    id_repetido = "DEDUP0000001"
    for tentativa in (1, 2):
        if id_repetido in dedup_set:
            print(f"[tentativa {tentativa}] trans_num={id_repetido} -> DUPLICADA (ignorada)")
            logging.info(f"[DUPLICADA] trans_num={id_repetido}")
        else:
            dedup_set.add(id_repetido)
            enviar(dict(TRANSACAO_VALIDA, trans_num=id_repetido))
            print(f"[tentativa {tentativa}] trans_num={id_repetido} -> ACEITA (1a ocorrencia)")

    print("\n" + "=" * 66)
    print(f" RESUMO: {acertos}/{len(casos)} casos com o comportamento esperado.")
    print(" Trilha de auditoria gravada em: auditoria_demo.log")
    print("=" * 66)


if __name__ == '__main__':
    main()
