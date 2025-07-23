FROM python:3.11-slim

# Instalar dependências do sistema necessárias
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    gzip \
    && rm -rf /var/lib/apt/lists/*

# Instalar Docker CLI (caso precise chamar Docker de dentro do container)
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends docker-ce-cli \
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
