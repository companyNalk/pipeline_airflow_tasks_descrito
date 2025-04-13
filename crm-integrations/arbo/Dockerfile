FROM python:3.11-slim

WORKDIR /app

# Copia somente o necessário - ajustado para o contexto raiz
COPY crm-integrations/arbo/main.py .
COPY crm-integrations/arbo/requirements.txt .

# Copia commons e generic como módulo
COPY commons/ /app/commons/
COPY generic/ /app/generic/

# Instala dependências
RUN pip install --no-cache-dir -r requirements.txt

# Comando de execução
CMD ["python", "main.py"]