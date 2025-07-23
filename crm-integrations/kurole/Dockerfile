FROM python:3.11-slim

# Instalar dependências do sistema necessárias
RUN apt-get update && apt-get install -y \
    # Docker
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    # Utilitários para MySQL
    gzip \
    # Limpeza
    && rm -rf /var/lib/apt/lists/*

# Instalar Docker
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null \
    && apt-get update \
    && apt-get install -y docker-ce-cli \
    && rm -rf /var/lib/apt/lists/*

# Configurar diretório de trabalho
WORKDIR /app

# Copia somente o necessário - ajustado para o contexto raiz
COPY crm-integrations/kurole/main.py .
COPY crm-integrations/kurole/requirements.txt .

# Copia commons e generic como módulo
COPY commons/ /app/commons/
COPY generic/ /app/generic/

# Instala dependências Python
RUN pip install --no-cache-dir -r requirements.txt

# Cria a pasta output com permissões adequadas
RUN mkdir -p /app/output && chmod 777 /app/output

# Comando de execução
CMD ["python", "main.py"]