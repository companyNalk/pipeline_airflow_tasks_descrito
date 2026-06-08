FROM python:3.11-slim

WORKDIR /app

# Copia o código da integração (main + auth)
COPY crm-integrations/contaazul/main.py .
COPY crm-integrations/contaazul/auth.py .
COPY crm-integrations/contaazul/requirements.txt .

# Copia commons e generic como módulo
COPY commons/ /app/commons/
COPY generic/ /app/generic/

# Instala dependências
RUN pip install --no-cache-dir -r requirements.txt

# Cria a pasta output com permissões adequadas
RUN mkdir -p /app/output && chmod 777 /app/output

# Comando de execução
CMD ["python", "main.py"]
