# Airflow Tasks

Integrações de tarefas com Airflow. 

## Estrutura do Projeto

```
.
├── commons/
│   ├── test
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
│   ├── test
│   │   └── test_*.py
│   ├── argument_manager.py
│   └── *.py
│
├── .flake8
├── .gitignore
├──  Makefile
├──  pytest.ini
└──  README.md
```

## Como executar

Cada ferramenta vai ser construída e executada independentemente, em Docker. 

## Antes de subir o código
Antes de construir e executar qualquer integração, é altamente recomendado verificar a qualidade do código e executar os testes unitários disponíveis. Para isso, execute o comando abaixo na raiz do projeto:
```bash
make
```
Este comando executa verificações de linting (usando flake8) e roda os testes unitários localizados nas pastas commons/test/ e generic/test/. As pastas commons e generic contêm testes unitários implementados para garantir confiabilidade dos módulos compartilhados.

Certifique-se de que todos os testes passaram antes de prosseguir com a construção da imagem Docker.

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
