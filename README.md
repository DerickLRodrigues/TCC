# Governança de Dados Financeiros em Arquiteturas de Streaming com Kafka

Repositório destinado ao código-fonte e infraestrutura da Prova de Conceito (PoC) desenvolvida para o Trabalho de Conclusão de Curso (TCC) em Sistemas de Informação. 

Este projeto demonstra a implementação de um modelo de **Governança Preventiva na Origem** utilizando Apache Kafka e Confluent Schema Registry para validar, auditar e aplicar o mascaramento (LGPD) em transações financeiras em tempo real.

---

##  Pré-requisitos

Para garantir a reprodutibilidade do experimento, você precisará ter instalado em sua máquina:
* [Docker Desktop](https://www.docker.com/products/docker-desktop/) (com a *engine* em execução).
* [Python 3.x](https://www.python.org/downloads/).

---

##  Passo a Passo para Reprodução

### 1. Provisionamento da Infraestrutura
Com o Docker Desktop aberto, abra o seu terminal na pasta raiz deste projeto e suba os contêineres do Kafka, Zookeeper e Schema Registry em segundo plano:
```bash
docker-compose up -d
```

### 2. Configuração do ambiente virtual
Crie e ative um ambiente virtual para isolar as dependências:
```
python -m venv venv
Ativação no Windows: venv\Scripts\activate
```

### 3. Instalação das dependências
Com o ambiente virtual ativado, instale as bibliotecas necessárias
```
pip install pandas certifi httpx authlib jsonschema fastjsonschema
pip install "confluent-kafka[schema-registry,json]"
```

### 4. Execução dos microsserviços
Para observar a governança em tempo real, execute primeiro o consumidor e depois o produtor.
Abra dois terminais, ambos com o ambiente virtual ativado na pasta src/
Terminal 1 – Consumidor: ``` python consumer.py ```
Terminal 2 – Produtor: ``` python producer.py ```
