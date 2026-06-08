"""
Renovação de access_token da ContaAzul API v2 via refresh_token.

O access_token expira em ~1h, então o extractor chama refresh_access_token()
no início de cada execução para obter um token válido.

ATENÇÃO (a confirmar no portal): alguns provedores OAuth rotacionam o
refresh_token a cada uso. Se a ContaAzul fizer isso, o campo `refresh_token`
abaixo virá preenchido na resposta e PRECISA ser re-persistido onde o Airflow
guarda os secrets — caso contrário o próximo run falhará com refresh_token inválido.
Por isso refresh_access_token() devolve o dict completo, não só o access_token.
"""

import base64
import logging

import requests

TOKEN_URL = "https://auth.contaazul.com/oauth2/token"


def _basic_auth_header(client_id: str, client_secret: str) -> str:
    raw = f"{client_id}:{client_secret}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("utf-8")


def refresh_access_token(client_id: str, client_secret: str, refresh_token: str,
                         logger: logging.Logger = None) -> dict:
    """
    Troca o refresh_token por um novo access_token.

    Retorna o dict completo da resposta do token endpoint. Campos esperados:
      - access_token (str)
      - expires_in (int, segundos)
      - token_type (str)
      - refresh_token (str, OPCIONAL — presente só se for rotativo)
    """
    logger = logger or logging.getLogger(__name__)

    headers = {
        "Authorization": _basic_auth_header(client_id, client_secret),
        "Content-Type": "application/x-www-form-urlencoded",
    }
    body = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }

    logger.info("🔑 Renovando access_token via refresh_token...")
    resp = requests.post(TOKEN_URL, headers=headers, data=body, timeout=30)

    if resp.status_code != 200:
        raise RuntimeError(
            f"Falha ao renovar access_token: {resp.status_code} - {resp.text[:300]}"
        )

    tokens = resp.json()
    if not tokens.get("access_token"):
        raise RuntimeError(f"Resposta sem access_token: {tokens}")

    if tokens.get("refresh_token") and tokens["refresh_token"] != refresh_token:
        logger.warning(
            "⚠️ A ContaAzul retornou um NOVO refresh_token (rotativo). "
            "Re-persista-o nos secrets do Airflow para o próximo run não falhar."
        )

    logger.info("✅ access_token renovado com sucesso")
    return tokens


def build_auth_headers(access_token: str) -> dict:
    """Headers de autenticação para as chamadas à API v2."""
    return {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
