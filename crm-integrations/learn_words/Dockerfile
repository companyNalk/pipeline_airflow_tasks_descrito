FROM python:3.11-slim

WORKDIR /app

# Copia somente o necessário - ajustado para o contexto raiz
COPY crm-integrations/learn_words/main.py .
COPY crm-integrations/learn_words/requirements.txt .

# Copia commons e generic como módulo
COPY commons/ /app/commons/
COPY generic/ /app/generic/

# Instala dependências
RUN pip install --no-cache-dir -r requirements.txt

# Cria diretório para credenciais
RUN mkdir -p /app/credentials

# Comando de execução
CMD ["python", "main.py"]