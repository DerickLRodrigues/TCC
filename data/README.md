# Base de Dados

O dataset utilizado na PoC **não é versionado neste repositório** por causa do tamanho
(1.296.675 registros, centenas de MB). Por isso, ele precisa ser baixado manualmente e
colocado nesta pasta antes de executar os scripts.

- **Fonte:** [Credit Card Transactions Dataset — Kaggle](https://www.kaggle.com/datasets/priyamchoksi/credit-card-transactions-dataset)
- **Arquivo esperado:** `data/credit_card_transactions.csv`

> O caminho é importante: os scripts em `src/` leem o arquivo em `../data/credit_card_transactions.csv`.
> O arquivo já está listado no `.gitignore`, então não há risco de subi-lo para o GitHub por engano.

---

## Opção 1 — Download manual pelo navegador (mais simples)

1. Acesse a página do dataset: <https://www.kaggle.com/datasets/priyamchoksi/credit-card-transactions-dataset>
2. Faça login (é necessário ter uma conta gratuita no Kaggle) e clique em **Download**.
3. Descompacte o `.zip` baixado. Dentro dele há o arquivo `credit_card_transactions.csv`.
4. Mova o `credit_card_transactions.csv` para a pasta `data/` na raiz deste projeto.

Ao final, a estrutura deve ficar assim:

```
TCC/
├── data/
│   └── credit_card_transactions.csv   <-- arquivo baixado vai aqui
├── src/
└── ...
```

---

## Opção 2 — Download via Kaggle CLI (linha de comando)

1. Instale a CLI: `pip install kaggle`
2. Gere um token de API no Kaggle em **Account → Settings → API → Create New Token**.
   Isso baixa um `kaggle.json`. Coloque-o em:
   - **Linux/Mac:** `~/.kaggle/kaggle.json`
   - **Windows:** `C:\Users\<seu-usuario>\.kaggle\kaggle.json`
3. A partir da raiz do projeto, baixe e descompacte direto na pasta `data/`:

```bash
kaggle datasets download -d priyamchoksi/credit-card-transactions-dataset -p data/ --unzip
```

---

## Verificando o arquivo

Confirme que o arquivo está no lugar certo e com o conteúdo esperado:

```bash
# Linux/Mac
head -n 1 data/credit_card_transactions.csv   # mostra o cabeçalho com as colunas
wc -l data/credit_card_transactions.csv        # deve indicar ~1.296.676 linhas (1 cabeçalho + dados)
```

```powershell
# Windows (PowerShell)
Get-Content data\credit_card_transactions.csv -TotalCount 1
```

O dataset contém diversas colunas, mas a PoC utiliza apenas estas cinco:

| Coluna | Uso na PoC |
| --- | --- |
| `trans_num` | Identificador único da transação (chave e deduplicação) |
| `cc_num` | Número do cartão — é **mascarado** antes do envio (LGPD) |
| `amt` | Valor da transação (validado: maior que 0 e menor que 500.000) |
| `merchant` | Estabelecimento (mínimo de 3 caracteres) |
| `trans_date_trans_time` | Data e hora (formato `YYYY-MM-DD HH:MM:SS`) |
