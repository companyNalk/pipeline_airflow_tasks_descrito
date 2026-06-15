# ContaAzul — Serviço de Callback OAuth 2.0

Serviço FastAPI que recebe o `code` do OAuth da ContaAzul, troca por
`refresh_token` e o exibe/grava. Publicado via **Cloudflare Tunnel** (mesmo
padrão do app de webhook do RD Station), rodando no servidor do Metabase.

> O `refresh_token` do Cognito **não rotaciona** → a autorização é feita
> **uma única vez por cliente**. O extractor (`crm-integrations/contaazul`)
> usa esse `refresh_token` (gravado na planilha do cliente) pra renovar o
> `access_token` a cada execução.

## Pré-requisito (BLOQUEIO ATUAL)

Precisa do `client_id` + `client_secret` de um app criado no Portal do
Desenvolvedor ContaAzul. Hoje a criação do app está falhando no backend deles
(erro de provisionamento no Cognito). Este serviço fica **pronto pra usar**
assim que as credenciais existirem.

## Deploy (servidor do Metabase)

1. **Criar o tunnel no Cloudflare** (Zero Trust → Networks → Tunnels → Create):
   - Crie um tunnel novo, copie o **token**.
   - Em **Public Hostname**, adicione:
     - Subdomain/domain: `webhook.nalk.freedomai.com.br` (ou o que preferir)
     - Service: `http://contaazul-callback:8010`
   - O hostname escolhido define o `CONTAAZUL_REDIRECT_URI`
     (`https://<hostname>/contaazul/callback`).

2. **Configurar o `.env`:**
   ```bash
   cp .env.example .env
   # preencher CLIENT_ID, CLIENT_SECRET, REDIRECT_URI (= hostname acima), TUNNEL_TOKEN
   ```

3. **Subir:**
   ```bash
   docker compose up -d --build
   docker compose logs -f contaazul-callback
   ```

4. **Sanity check:**
   ```bash
   curl -s https://webhook.nalk.freedomai.com.br/contaazul/callback   # 400 "sem code" = no ar
   ```

## Autorizar um cliente (uma vez)

1. Abrir no navegador: `https://webhook.nalk.freedomai.com.br/contaazul/authorize`
2. Logar na conta **ContaAzul do cliente** e autorizar.
3. A página de retorno mostra o **`refresh_token`** (e grava em `./data/refresh_token.txt`).
4. Copiar o `refresh_token` pra **planilha do cliente** (coluna `refresh_token`).

## Rotas

| Rota | Função |
|------|--------|
| `GET /health` | healthcheck (200) |
| `GET /contaazul/authorize` | inicia o fluxo (redireciona pro login ContaAzul) |
| `GET /contaazul/callback` | recebe `?code=`, troca por tokens, mostra/grava o refresh_token |

## Segurança

- O `.env` (com `client_secret` e `TUNNEL_TOKEN`) **não vai pro git** (ver `.gitignore`).
- O `refresh_token` tem poder de ação na conta do cliente — trate como senha.
