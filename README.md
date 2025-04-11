# Airflow Tasks

Integrações de tarefas com Airflow. 

## Estrutura do Projeto

```
.
├── integration_one/        # Integração específica
│   ├── .env                # Variáveis de ambiente
│   ├── *.py                # Scripts da integração
│   ├── requirements.txt    # Dependências Python
│   ├── run.sh              # Script executor
│   └── Dockerfile          # Configuração container
│
├── integration_two/        # Outra integração
│   ├── .env                # Variáveis de ambiente
│   ├── *.py                # Scripts da integração
│   ├── requirements.txt    # Dependências Python
│   ├── run.sh              # Script executor
│   └── Dockerfile          # Configuração container
│
├── commons/                # Módulos compartilhados
│   ├── create_sheets.py    # Utilitário para criar planilhas
│   └── *.py                # Outras funções compartilhadas
│
├── settings/
│   └── credentials.json    # Credenciais GCP compartilhadas
│
├── .gitignore              # Arquivos ignorados pelo Git
```