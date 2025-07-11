#!/bin/bash

echo "Iniciando configuração da VPN..."

# Cria o diretório vpn-config se não existir
mkdir -p /app/vpn-config

# Verifica se arquivos VPN existem OU se variáveis de ambiente estão definidas
if [ ! -f "/app/vpn-config/conector.ovpn" ] || [ ! -f "/app/vpn-config/pass.txt" ]; then
    echo "Arquivos VPN não encontrados, tentando criar a partir de variáveis de ambiente..."

    # Verifica se variáveis de ambiente estão definidas
    if [ -z "$VPN_OVPN_CONTENT" ] && [ -z "$VPN_OVPN_BASE64" ] || [ -z "$VPN_PASSWORD" ]; then
        echo "ERRO: Nem arquivos VPN nem variáveis de ambiente encontrados"
        echo "Necessário: (VPN_OVPN_CONTENT ou VPN_OVPN_BASE64) e VPN_PASSWORD"
        exit 1
    fi

    # Cria arquivos VPN a partir das variáveis
    echo "Criando arquivos VPN a partir de variáveis de ambiente..."

    # Se tiver base64, decodifica primeiro
    if [ -n "$VPN_OVPN_BASE64" ]; then
        echo "Decodificando VPN_OVPN_BASE64..."
        if ! echo "$VPN_OVPN_BASE64" | base64 -d > /app/vpn-config/conector.ovpn; then
            echo "ERRO: Falha ao decodificar VPN_OVPN_BASE64"
            exit 1
        fi
        echo "✓ Base64 decodificado com sucesso"
    else
        # Usa conteúdo direto se disponível
        echo "Usando VPN_OVPN_CONTENT..."
        echo "$VPN_OVPN_CONTENT" > /app/vpn-config/conector.ovpn
    fi

    # Cria arquivo de senha
    echo "$VPN_PASSWORD" > /app/vpn-config/pass.txt

    echo "✓ Arquivos VPN criados com sucesso!"
fi

# Verifica novamente se arquivos existem
if [ ! -f "/app/vpn-config/conector.ovpn" ] || [ ! -f "/app/vpn-config/pass.txt" ]; then
    echo "ERRO: Arquivos VPN ainda não encontrados"
    exit 1
fi

# Debug: mostra primeiras linhas do arquivo criado
echo "Debug - Primeiras linhas do conector.ovpn:"
head -5 /app/vpn-config/conector.ovpn

PASSWORD=$(cat /app/vpn-config/pass.txt | tr -d '\r\n')
echo "Senha lida: ${#PASSWORD} caracteres"

# Extrai e converte chave privada
echo "Processando chave privada..."
sed -n '/<key>/,/<\/key>/p' /app/vpn-config/conector.ovpn | grep -v '<key>' | grep -v '</key>' > /tmp/key_encrypted.pem

# Verifica se a chave foi extraída
if [ ! -s /tmp/key_encrypted.pem ]; then
    echo "ERRO: Não foi possível extrair a chave privada do arquivo .ovpn"
    echo "Conteúdo do arquivo .ovpn:"
    cat /app/vpn-config/conector.ovpn
    exit 1
fi

# Converte chave
if ! echo "$PASSWORD" | openssl rsa -in /tmp/key_encrypted.pem -out /tmp/key_plain.pem -passin stdin 2>/dev/null; then
    echo "ERRO: Falha na conversão da chave"
    echo "Verificando se a chave está criptografada..."
    openssl rsa -in /tmp/key_encrypted.pem -text -noout 2>&1 || true
    exit 1
fi

# Cria arquivo .ovpn modificado
cp /app/vpn-config/conector.ovpn /tmp/conector_fixed.ovpn
sed -i '/<key>/,/<\/key>/d' /tmp/conector_fixed.ovpn
echo -e "\n<key>" >> /tmp/conector_fixed.ovpn
cat /tmp/key_plain.pem >> /tmp/conector_fixed.ovpn
echo "</key>" >> /tmp/conector_fixed.ovpn

echo "✓ Configuração VPN preparada"

# Inicia OpenVPN
echo "Conectando VPN..."
openvpn --config /tmp/conector_fixed.ovpn --daemon --log /tmp/openvpn.log --verb 1

# Aguarda conexão
sleep 10

# Verifica se OpenVPN está rodando
if ! ps aux | grep -v grep | grep -q openvpn; then
    echo "ERRO: OpenVPN não iniciou"
    echo "Log do OpenVPN:"
    cat /tmp/openvpn.log 2>/dev/null
    exit 1
fi

# Verifica interface TUN
if ! ip addr show | grep -q tun; then
    echo "ERRO: Interface TUN não criada"
    echo "Interfaces disponíveis:"
    ip addr show
    exit 1
fi

echo "✓ VPN conectada - IP: $(ip addr show tun0 | grep 'inet ' | awk '{print $2}' | cut -d'/' -f1 2>/dev/null || echo 'IP não encontrado')"

# Testa conectividade exclusivamente com o servidor
echo "Testando servidor 192.169.0.6..."
for i in {1..10}; do
    if curl -s --max-time 5 http://192.169.0.6:8084/rest/ | grep -q "TOTVS - RESTFul API"; then
        echo "✓ Servidor acessível!"
        break
    fi

    if [ $i -eq 10 ]; then
        echo "ERRO: Servidor não acessível após 10 tentativas"
        echo "Última resposta do servidor:"
        curl -s --max-time 5 http://192.169.0.6:8084/rest/ || echo "Sem resposta"
        exit 1
    fi

    echo "Tentativa $i/10..."
    sleep 3
done

echo "🚀 Iniciando aplicação..."
cd /app
python main.py

echo "🔌 Finalizando VPN..."
killall openvpn 2>/dev/null