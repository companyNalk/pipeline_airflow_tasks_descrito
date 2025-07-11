FROM python:3.11-slim

WORKDIR /app

# Instala OpenVPN e dependências necessárias
RUN apt-get update && \
    apt-get install -y openvpn curl procps iproute2 openssl coreutils netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

# Copia o app principal
COPY crm-integrations/protheus/main.py .
COPY crm-integrations/protheus/requirements.txt .

# Copia módulos auxiliares
COPY commons/ /app/commons/
COPY generic/ /app/generic/

# Copia configurações da VPN
#COPY crm-integrations/protheus/vpn-config/ /app/vpn-config/

# Instala dependências Python
RUN pip install --no-cache-dir -r requirements.txt

# Cria a pasta de saída com permissões
RUN mkdir -p /app/output && chmod 777 /app/output

# Copia o script de inicialização
#COPY crm-integrations/protheus/entrypoint.sh /entrypoint.sh
#RUN chmod +x /entrypoint.sh
#
#ENTRYPOINT ["/entrypoint.sh"]

CMD ["python", "main.py"]