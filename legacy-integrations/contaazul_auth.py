"""
Autorizador OAuth 2.0 — ContaAzul API v2 (Authorization Code flow).

Roda UMA vez por cliente, manualmente, para obter o refresh_token de longo prazo.
Depois disso, o extractor (crm-integrations/contaazul) renova o access_token
sozinho a cada execução usando esse refresh_token.

Fluxo:
  1. Monta a URL de autorização e abre no navegador.
  2. Sobe um servidor HTTP local temporário para capturar o `code` no redirect.
  3. Troca o `code` por access_token + refresh_token no token endpoint.
  4. Imprime o refresh_token (para você persistir onde o Airflow injeta secrets).

Pré-requisitos no Portal do Desenvolvedor (https://developers-portal.contaazul.com):
  - Criar o app -> obter CLIENT_ID e CLIENT_SECRET
  - Cadastrar o REDIRECT_URI exatamente igual ao usado aqui
    (default: http://localhost:8765/callback)

Uso:
    export CONTAAZUL_CLIENT_ID=...
    export CONTAAZUL_CLIENT_SECRET=...
    # opcional: export CONTAAZUL_REDIRECT_URI=http://localhost:8765/callback
    python legacy-integrations/contaazul_auth.py
"""

import base64
import os
import secrets
import sys
import threading
import urllib.parse
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer

import requests

# Endpoints OAuth 2.0 da ContaAzul (Cognito-based)
AUTHORIZE_URL = "https://auth.contaazul.com/oauth2/authorize"
TOKEN_URL = "https://auth.contaazul.com/oauth2/token"

# Scope fixo exigido pela API v2
SCOPE = "openid profile aws.cognito.signin.user.admin"

DEFAULT_REDIRECT_URI = "http://localhost:8765/callback"

# Container para passar o resultado do handler HTTP de volta à thread principal
_RESULT = {"code": None, "state": None, "error": None}
_EVENT = threading.Event()


class _CallbackHandler(BaseHTTPRequestHandler):
    """Captura o redirect do OAuth e extrai o `code`."""

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path != urllib.parse.urlparse(_RESULT["redirect_uri"]).path:
            self.send_response(404)
            self.end_headers()
            return

        params = urllib.parse.parse_qs(parsed.query)
        _RESULT["code"] = params.get("code", [None])[0]
        _RESULT["state"] = params.get("state", [None])[0]
        _RESULT["error"] = params.get("error", [None])[0]

        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        if _RESULT["error"]:
            msg = f"Erro na autorizacao: {_RESULT['error']}"
        elif _RESULT["code"]:
            msg = "Autorizacao concluida! Pode fechar esta aba e voltar ao terminal."
        else:
            msg = "Resposta inesperada (sem code)."
        self.wfile.write(f"<html><body><h2>{msg}</h2></body></html>".encode("utf-8"))
        _EVENT.set()

    def log_message(self, *args):  # silencia o log padrão do HTTPServer
        return


def _basic_auth_header(client_id, client_secret):
    raw = f"{client_id}:{client_secret}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("utf-8")


def build_authorize_url(client_id, redirect_uri, state):
    query = urllib.parse.urlencode({
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": SCOPE,
        "state": state,
    })
    return f"{AUTHORIZE_URL}?{query}"


def exchange_code_for_tokens(client_id, client_secret, code, redirect_uri):
    """Troca o authorization code por access_token + refresh_token."""
    headers = {
        "Authorization": _basic_auth_header(client_id, client_secret),
        "Content-Type": "application/x-www-form-urlencoded",
    }
    body = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
    }
    resp = requests.post(TOKEN_URL, headers=headers, data=body, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"Falha ao trocar code por token: {resp.status_code} - {resp.text}")
    return resp.json()


def main():
    client_id = os.getenv("CONTAAZUL_CLIENT_ID")
    client_secret = os.getenv("CONTAAZUL_CLIENT_SECRET")
    redirect_uri = os.getenv("CONTAAZUL_REDIRECT_URI", DEFAULT_REDIRECT_URI)

    if not client_id or not client_secret:
        print("ERRO: defina CONTAAZUL_CLIENT_ID e CONTAAZUL_CLIENT_SECRET no ambiente.")
        sys.exit(1)

    parsed_redirect = urllib.parse.urlparse(redirect_uri)
    host = parsed_redirect.hostname or "localhost"
    port = parsed_redirect.port or 8765
    _RESULT["redirect_uri"] = redirect_uri

    state = secrets.token_urlsafe(16)
    auth_url = build_authorize_url(client_id, redirect_uri, state)

    # Sobe o servidor de callback em background
    server = HTTPServer((host, port), _CallbackHandler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    print("=" * 70)
    print("Abrindo o navegador para autorizacao da ContaAzul...")
    print("Se nao abrir, acesse manualmente a URL abaixo:")
    print(auth_url)
    print("=" * 70)
    webbrowser.open(auth_url)

    # Aguarda o callback (timeout 5 min)
    if not _EVENT.wait(timeout=300):
        print("ERRO: timeout aguardando a autorizacao (5 min).")
        server.shutdown()
        sys.exit(1)

    server.shutdown()

    if _RESULT["error"]:
        print(f"ERRO retornado pela ContaAzul: {_RESULT['error']}")
        sys.exit(1)

    if _RESULT["state"] != state:
        print("ERRO: state divergente (possivel CSRF). Abortando.")
        sys.exit(1)

    if not _RESULT["code"]:
        print("ERRO: nenhum code recebido no callback.")
        sys.exit(1)

    print("Code recebido. Trocando por tokens...")
    tokens = exchange_code_for_tokens(client_id, client_secret, _RESULT["code"], redirect_uri)

    access_token = tokens.get("access_token")
    refresh_token = tokens.get("refresh_token")
    expires_in = tokens.get("expires_in")

    print("=" * 70)
    print("SUCESSO! Tokens obtidos:")
    print(f"  access_token (expira em {expires_in}s): {access_token[:25]}...")
    print(f"  refresh_token (GUARDE COM SEGURANCA):\n\n{refresh_token}\n")
    print("=" * 70)
    print("Proximo passo: persista o refresh_token onde o Airflow injeta secrets")
    print("e passe-o ao extractor via REFRESH_TOKEN.")


if __name__ == "__main__":
    main()
