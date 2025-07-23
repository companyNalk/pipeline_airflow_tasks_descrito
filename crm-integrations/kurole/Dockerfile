FROM python:3.11-slim

# Instalar dependências do sistema necessárias
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    gzip \
    && rm -rf /var/lib/apt/lists/*

# Diretório de trabalho
WORKDIR /app

# Copia os arquivos principais
COPY crm-integrations/kurole/main.py .
COPY crm-integrations/kurole/requirements.txt .

# Copia módulos
COPY commons/ /app/commons/
COPY generic/ /app/generic/

# Instala dependências Python
RUN pip install --no-cache-dir -r requirements.txt

# Cria pasta de saída
RUN mkdir -p /app/output && chmod 777 /app/output

# Comando padrão de execução
CMD ["python", "main.py"]
