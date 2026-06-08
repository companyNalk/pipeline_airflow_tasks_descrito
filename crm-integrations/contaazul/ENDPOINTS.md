# ContaAzul API v2 — Mapeamento de Endpoints

**Base URL:** `https://api-v2.contaazul.com/v1`
**Autenticação:** OAuth 2.0 Authorization Code — header `Authorization: Bearer {access_token}`
**Token endpoint:** `POST https://auth.contaazul.com/oauth2/token`
**Authorize endpoint:** `https://auth.contaazul.com/oauth2/authorize`
**Scope (fixo):** `openid profile aws.cognito.signin.user.admin`
**Docs:** https://developers.contaazul.com/ (portal exige login; WebFetch retorna 403)

> ⚠️ **API v2 lançada em mar/2025.** A legada (`api.contaazul.com`) está em
> descontinuação. Os IDs de Produtos/Vendas/Pessoas mudaram — de-para via `id_legado`.

---

## Autenticação

### Fluxo (uma vez por cliente) — `legacy-integrations/contaazul_auth.py`
1. Redirect do usuário para `oauth2/authorize` (login + consentimento)
2. Recebe `code` no `redirect_uri`
3. Troca `code` -> `access_token` + `refresh_token` (`grant_type=authorization_code`)

### Renovação (todo run) — `auth.py`
- `POST oauth2/token` com `grant_type=refresh_token`
- Header `Authorization: Basic base64(client_id:client_secret)`
- `access_token` expira em ~1h

> ⚠️ **A confirmar:** se o `refresh_token` é **rotativo** (retornado a cada
> renovação). Se for, precisa ser re-persistido nos secrets a cada run.

---

## Rate Limit
- **600 chamadas/minuto** e **10/segundo** por conta ERP conectada
- Detalhes via headers HTTP na resposta
- `RATE_LIMIT` no `main.py` está em 500/min (margem de segurança)

---

## Paginação
- Parâmetros: **`pagina`** (1-based) + **`tamanho_pagina`**
- (Mudou vs. legada, que usava `page`)
- ⚠️ **A confirmar:** formato exato da resposta (chave da lista de itens e
  metadados de total). O paginador em `main.py` é defensivo: tenta as chaves
  `itens`/`data`/`content`/`registros`/`items` e detecta fim quando uma página
  retorna menos que `tamanho_pagina`.

---

## Endpoints para Extração (GET)

| Domínio | Endpoint | Status |
|---|---|---|
| Clientes/Pessoas | `GET /v1/pessoas` | ✅ confirmado |
| Produtos | `GET /v1/produtos` | ⚠️ inferido — confirmar no portal |
| Vendas | `GET /v1/vendas` | ⚠️ inferido — confirmar no portal |
| Financeiro (eventos: pagar/receber) | `GET /v1/financeiro/eventos-financeiros` | ⚠️ inferido — confirmar no portal |

> Endpoint de criação de evento financeiro a pagar (`createpayablefinancialevent`)
> está documentado no portal, o que sugere a família `/financeiro/eventos-financeiros`.

### Dict do `main.py`
```python
ENDPOINTS = {
    "pessoas":            "pessoas",
    "produtos":           "produtos",
    "vendas":             "vendas",
    "financeiro_eventos": "financeiro/eventos-financeiros",
}
```

---

## Pendências de validação no portal (precisam de login)
1. Paths exatos de **produtos**, **vendas** e **financeiro**
2. Formato da resposta paginada (chave da lista + metadados)
3. `refresh_token` rotativo? (sim/não)
4. `redirect_uri` cadastrado no app (default usado: `http://localhost:8765/callback`)
5. Filtros de data disponíveis (para extração incremental futura)

---

## Tabelas BigQuery (ver `sheet.sql`)
- `contaazul.pessoas` -> gold `vendas.contaazul_pessoas_gold`
- `contaazul.produtos` -> gold `vendas.contaazul_produtos_gold`
- `contaazul.vendas` -> gold `vendas.contaazul_vendas_gold`
- `contaazul.financeiro_eventos` -> gold `vendas.contaazul_financeiro_eventos_gold`
