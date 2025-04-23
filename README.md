# Tarefas Airflow

Integrações de tarefas com Airflow.

## Estrutura do Projeto

```
.
├── commons/
│   ├── test/
│   │   └── test_*.py
│   ├── create_sheets.py
│   └── *.py
│
├── crm-integrations/
│   ├── integration_one/
│   │   ├── .env
│   │   ├── *.py
│   │   ├── requirements.txt
│   │   └── Dockerfile
│   │
│   ├── integration_two/
│   │   ├── .env
│   │   ├── *.py
│   │   ├── requirements.txt
│   │   └── Dockerfile
│   │
│   └── ...
│
├── generic/
│   ├── test/
│   │   └── test_*.py
│   ├── argument_manager.py
│   └── *.py
│
├── crm-integrations/
│   └── *.py
│
├── sql/
│   ├── integration_one
│   │    ├── .env
│   │    └── sheet.sql
│   │
│   └── ...
│
├── .flake8
├── .gitignore
├── Makefile
├── pytest.ini
└── README.md
```

## Como Executar

### Verificações Pré-Execução

Antes de construir e executar qualquer integração, é altamente recomendado verificar a qualidade do código e executar os testes unitários disponíveis. Para isso, execute o comando abaixo na raiz do projeto:

```bash
make
```

Este comando:
- Executa verificações de linting (usando flake8)
- Roda os testes unitários localizados nas pastas commons/test/ e generic/test/
- Garante a confiabilidade dos módulos compartilhados

Certifique-se de que todos os testes passaram antes de prosseguir com a construção da imagem Docker.

### Executando uma Integração

Cada ferramenta vai ser construída e executada independentemente, em Docker.

Exemplo de execução do projeto LEARN WORDS (necessário estar na raiz do projeto):

```bash
# Construção da imagem
docker build --no-cache -t learn-words-mev -f crm-integrations/learn_words/Dockerfile .

# Execução do container
docker run --rm --name learn-words-mev \
  -e API_BASE_URL="https://api.example.com" \
  -e API_CLIENT_ID="client_id" \
  -e API_CLIENT_SECRET="client_secret" \
  -e LW_CLIENT="client_name" \
  learn-words-mev
```

### Geração de SQL

Para gerar o SQL da planilha, basta estar na raiz do projeto e executar:

```bash
python commons/create_sheets.py ./crm-integrations/asaas/
```

A saída será um arquivo `sheet.sql` em `crm-integrations/asaas/sheet.sql`

#### Requisitos de Ambiente

O arquivo `.env` precisa estar na raiz do projeto com a seguinte estrutura:

```bash
TOOL=XX
ENDPOINTS=payments,customers,subscriptions,subscriptions_id_payments
```