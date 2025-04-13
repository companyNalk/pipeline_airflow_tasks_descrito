# Airflow Tasks

Integrações de tarefas com Airflow. 

## Estrutura do Projeto

```
.
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
├── commons/
│   ├── create_sheets.py
│   └── *.py
│
├── generic/
│   ├── argument_manager.py
│   └── *.py
│
├── .flake8
├── .gitignore
├── Makefile
└──  README.md
```

## Como executar

Cada ferramenta vai ser construída e executada independentemente, em Docker. 

### Exemplo de execução o projeto LEARN WORDS (Necessario estar na raiz do projeto):

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
