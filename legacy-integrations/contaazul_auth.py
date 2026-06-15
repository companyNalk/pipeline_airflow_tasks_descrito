"""
Autorizador OAuth 2.0 — ContaAzul API v2 (Authorization Code flow).

Roda UMA vez por cliente, manualmente, para obter o refresh_token de longo prazo.
Depois disso, o extractor (crm-integrations/contaazul) renova o access_token
sozinho a cada execução usando esse refresh_token.

Fluxo em 2 passos (modo MANUAL — o portal da ContaAzul NÃO aceita redirect_uri
localhost, então usamos uma URL pública e capturamos o `code` da barra de endereço):

  PASSO 1 — gerar a URL de autorização:
      export CONTAAZUL_CLIENT_ID=...
      export CONTAAZUL_CLIENT_SECRET=...
      # opcional: export CONTAAZUL_REDIRECT_URI=https://nalk.freedomai.com.br/api/health
      python legacy-integrations/contaazul_auth.py
    -> abre o navegador / imprime a URL. Você loga na conta ContaAzul do CLIENTE,
       autoriza, e o navegador é redirecionado para a redirect_uri com `?code=XXXX`.
       Copie o valor de `code` da barra de endereço.

  PASSO 2 — trocar o code por tokens:
      python legacy-integrations/contaazul_auth.py "COLE_O_CODE_AQUI"
    -> imprime o refresh_token (persista onde o Airflow injeta secrets).

Pré-requisitos no Portal do Desenvolvedor (https://developers.contaazul.com):
  - Criar o app -> obter CLIENT_ID e CLIENT_SECRET
  - Cadastrar o REDIRECT_URI exatamente igual ao usado aqui
    (default: https://nalk.freedomai.com.br/api/health)
"""

import base64
import os
import secrets
import sys
import urllib.parse
import webbrowser

import requests

# Endpoints OAuth 2.0 da ContaAzul — conforme doc oficial (developers.contaazul.com).
# Autorização usa /login (hosted UI); token usa /oauth2/token.
AUTHORIZE_URL = "https://auth.contaazul.com/login"
TOKEN_URL = "https://auth.contaazul.com/oauth2/token"

# Scope fixo exigido pela API v2
SCOPE = "openid profile aws.cognito.signin.user.admin"

# Redirect pública (localhost é rejeitado pelo portal). /api/health = rota real
# do Metabase, sempre 200 -> passa na checagem de reachability do portal.
DEFAULT_REDIRECT_URI = "https://nalk.freedomai.com.br/api/health"

STATE_FILE = "/tmp/contaazul_oauth_state.txt"


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


def _print_code_from_url_hint(redirect_uri):
    print("Apos autorizar, o navegador vai para algo como:")
    print(f"  {redirect_uri}?code=XXXXXXXX&state=YYYY")
    print("Copie SOMENTE o valor de `code` (entre code= e &state).")
    print("A pagina pode mostrar um JSON {\"status\":\"ok\"} ou 404 — tudo bem.\n")


def step1_authorize(client_id, redirect_uri):
    state = secrets.token_urlsafe(16)
    try:
        with open(STATE_FILE, "w") as f:
            f.write(state)
    except Exception:
        pass
    auth_url = build_authorize_url(client_id, redirect_uri, state)
    print("=" * 70)
    print("PASSO 1 — AUTORIZACAO ContaAzul")
    print("=" * 70)
    print("Abrindo o navegador (se nao abrir, copie a URL abaixo):\n")
    print(auth_url + "\n")
    _print_code_from_url_hint(redirect_uri)
    print("Depois rode o PASSO 2:")
    print('  python legacy-integrations/contaazul_auth.py "SEU_CODE_AQUI"')
    print("=" * 70)
    try:
        webbrowser.open(auth_url)
    except Exception:
        pass


def step2_exchange(client_id, client_secret, redirect_uri, code):
    print("Trocando o code por tokens...")
    tokens = exchange_code_for_tokens(client_id, client_secret, code, redirect_uri)
    access_token = tokens.get("access_token")
    refresh_token = tokens.get("refresh_token")
    expires_in = tokens.get("expires_in")
    print("=" * 70)
    print("SUCESSO! Tokens obtidos:")
    if access_token:
        print(f"  access_token (expira em {expires_in}s): {access_token[:25]}...")
    print(f"\n  refresh_token (GUARDE COM SEGURANCA):\n\n{refresh_token}\n")
    print("=" * 70)
    print("Proximo passo: persista o refresh_token na planilha (env do cliente)")
    print("e passe-o ao extractor via REFRESH_TOKEN.")


def main():
    client_id = os.getenv("CONTAAZUL_CLIENT_ID")
    client_secret = os.getenv("CONTAAZUL_CLIENT_SECRET")
    redirect_uri = os.getenv("CONTAAZUL_REDIRECT_URI", DEFAULT_REDIRECT_URI)

    if not client_id:
        print("ERRO: defina CONTAAZUL_CLIENT_ID no ambiente.")
        sys.exit(1)

    # code via argumento (passo 2) ou env CONTAAZUL_CODE
    code = sys.argv[1] if len(sys.argv) > 1 else os.getenv("CONTAAZUL_CODE")

    if not code:
        # PASSO 1: gerar URL de autorizacao
        step1_authorize(client_id, redirect_uri)
        return

    # PASSO 2: trocar code por tokens
    if not client_secret:
        print("ERRO: defina CONTAAZUL_CLIENT_SECRET para o passo 2.")
        sys.exit(1)
    step2_exchange(client_id, client_secret, redirect_uri, code)


if __name__ == "__main__":
    main()
