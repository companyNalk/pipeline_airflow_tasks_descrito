"""
Serviço de callback OAuth 2.0 — ContaAzul API v2 (Authorization Code flow).

Deploy: container FastAPI publicado via Cloudflare Tunnel (mesmo padrão do
app de webhook do RD Station). Sobe no servidor do Metabase com docker-compose.

Rotas:
  GET /health                 -> healthcheck (200) — usado pelo tunnel/monitor
  GET /contaazul/authorize    -> redireciona pro login da ContaAzul (inicia o fluxo)
  GET /contaazul/callback     -> recebe o ?code=, troca por tokens, mostra o
                                 refresh_token e grava em TOKEN_DIR/refresh_token.txt

Fluxo de uso (UMA vez por cliente — o refresh_token do Cognito NÃO rotaciona):
  1. Abrir https://<host>/contaazul/authorize no navegador
  2. Logar na conta ContaAzul do CLIENTE e autorizar
  3. A ContaAzul redireciona pra /contaazul/callback -> a página mostra o refresh_token
  4. Copiar o refresh_token pra planilha do cliente (coluna REFRESH_TOKEN)

Config via env (ver .env.example):
  CONTAAZUL_CLIENT_ID, CONTAAZUL_CLIENT_SECRET  (do app no portal)
  CONTAAZUL_REDIRECT_URI  (DEVE bater EXATAMENTE com o cadastrado no portal)
  CONTAAZUL_SCOPE         (default: openid profile aws.cognito.signin.user.admin)
  TOKEN_DIR              (onde grava o refresh_token; default /data)
"""

import base64
import html
import logging
import os
import secrets

import requests
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("contaazul-callback")

# --- Endpoints OAuth da ContaAzul (doc oficial) ---
AUTHORIZE_URL = "https://auth.contaazul.com/login"
TOKEN_URL = "https://auth.contaazul.com/oauth2/token"

CLIENT_ID = os.getenv("CONTAAZUL_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("CONTAAZUL_CLIENT_SECRET", "")
REDIRECT_URI = os.getenv(
    "CONTAAZUL_REDIRECT_URI",
    "https://webhook.nalk.freedomai.com.br/contaazul/callback",
)
SCOPE = os.getenv("CONTAAZUL_SCOPE", "openid profile aws.cognito.signin.user.admin")
TOKEN_DIR = os.getenv("TOKEN_DIR", "/data")

app = FastAPI(title="ContaAzul OAuth Callback", docs_url=None, redoc_url=None)

# state -> guardado em cookie e validado no retorno (proteção CSRF do OAuth)
_STATE_COOKIE = "ca_oauth_state"


def _basic_auth_header(client_id: str, client_secret: str) -> str:
    raw = f"{client_id}:{client_secret}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("utf-8")


def _build_authorize_url(state: str) -> str:
    import urllib.parse
    query = urllib.parse.urlencode({
        "response_type": "code",
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "scope": SCOPE,
        "state": state,
    })
    return f"{AUTHORIZE_URL}?{query}"


def _exchange_code(code: str) -> dict:
    headers = {
        "Authorization": _basic_auth_header(CLIENT_ID, CLIENT_SECRET),
        "Content-Type": "application/x-www-form-urlencoded",
    }
    body = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": REDIRECT_URI,
    }
    resp = requests.post(TOKEN_URL, headers=headers, data=body, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"{resp.status_code} - {resp.text[:500]}")
    return resp.json()


def _persist_refresh_token(refresh_token: str) -> str:
    """Grava o refresh_token num arquivo no volume montado. Retorna o caminho."""
    try:
        os.makedirs(TOKEN_DIR, exist_ok=True)
        path = os.path.join(TOKEN_DIR, "refresh_token.txt")
        with open(path, "w") as f:
            f.write(refresh_token)
        return path
    except Exception as exc:  # noqa: BLE001 - apenas log, não falhar o fluxo
        logger.warning("Não consegui gravar o refresh_token em disco: %s", exc)
        return "(não gravado em disco — copie da tela)"


@app.get("/health")
def health():
    return JSONResponse({"status": "ok", "service": "contaazul-callback"})


@app.get("/contaazul/authorize")
def authorize():
    if not CLIENT_ID:
        return HTMLResponse(
            "<h3>Configuração incompleta</h3><p>Defina CONTAAZUL_CLIENT_ID no .env.</p>",
            status_code=500,
        )
    state = secrets.token_urlsafe(16)
    resp = RedirectResponse(_build_authorize_url(state), status_code=302)
    resp.set_cookie(_STATE_COOKIE, state, httponly=True, secure=True, samesite="lax")
    return resp


@app.get("/contaazul/callback")
def callback(request: Request, code: str = "", state: str = "", error: str = ""):
    if error:
        return HTMLResponse(f"<h3>Erro retornado pela ContaAzul</h3><pre>{html.escape(error)}</pre>", status_code=400)
    if not code:
        return HTMLResponse(
            "<h3>Sem código de autorização</h3>"
            "<p>Inicie o fluxo por <code>/contaazul/authorize</code>.</p>",
            status_code=400,
        )

    expected_state = request.cookies.get(_STATE_COOKIE)
    if expected_state and state and expected_state != state:
        return HTMLResponse(
            "<h3>State inválido (possível CSRF)</h3>"
            "<p>Refaça o fluxo a partir de <code>/contaazul/authorize</code>.</p>",
            status_code=400,
        )

    try:
        tokens = _exchange_code(code)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Falha ao trocar code por tokens")
        return HTMLResponse(
            f"<h3>Falha ao trocar o code por tokens</h3><pre>{html.escape(str(exc))}</pre>",
            status_code=502,
        )

    refresh_token = tokens.get("refresh_token", "")
    access_token = tokens.get("access_token", "")
    expires_in = tokens.get("expires_in", "?")
    saved_path = _persist_refresh_token(refresh_token) if refresh_token else "(sem refresh_token na resposta)"
    logger.info("Tokens obtidos com sucesso. refresh_token salvo em %s", saved_path)

    page = f"""
    <html><head><meta charset="utf-8"><title>ContaAzul — autorização concluída</title>
    <style>
      body{{font-family:system-ui,Arial,sans-serif;max-width:760px;margin:40px auto;padding:0 16px;color:#1f2937}}
      .ok{{background:#ecfdf5;border:1px solid #10b981;border-radius:8px;padding:12px 16px}}
      code,textarea{{font-family:ui-monospace,Menlo,Consolas,monospace}}
      textarea{{width:100%;height:90px;margin-top:6px;padding:8px;border:1px solid #d1d5db;border-radius:6px}}
      .muted{{color:#6b7280;font-size:14px}}
    </style></head><body>
    <div class="ok"><b>✅ Autorização concluída.</b> access_token válido por {html.escape(str(expires_in))}s.</div>
    <h3>refresh_token (copie para a planilha do cliente, coluna <code>refresh_token</code>)</h3>
    <textarea readonly onclick="this.select()">{html.escape(refresh_token)}</textarea>
    <p class="muted">Também gravado no servidor em: <code>{html.escape(saved_path)}</code></p>
    <p class="muted">O refresh_token do Cognito não rotaciona — esta autorização é feita
    uma única vez por cliente.</p>
    </body></html>
    """
    return HTMLResponse(page)
