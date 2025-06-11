FROM python:3.11-slim

WORKDIR /app

# Copia somente o necessário - ajustado para o contexto raiz
COPY crm-integrations/pipedrive_v2/main.py .
COPY crm-integrations/pipedrive_v2/requirements.txt .

# Copia commons e generic como módulo
COPY commons/ /app/commons/
COPY generic/ /app/generic/

# Instala dependências
RUN pip install --no-cache-dir -r requirements.txt

# Cria a pasta output com permissões adequadas
RUN mkdir -p /app/output && chmod 777 /app/output

# Comando de execução
CMD ["python", "main.py"]