# iClin API — Mapeamento de Endpoints

**Doc oficial:** https://iclin.com.br/web/inter/apicli_spo.php
**Base URL:** `https://iclin.com.br/web/inter` (a classe vira segmento do path)
**Autenticação:** headers `app-api-user: {usuario}` + `app-api-key: {chave}`
**Método:** `POST` exclusivamente, corpo **form-encoded** (PHP `$_POST`), UTF-8
**Escopo desta integração:** SOMENTE LEITURA

> ⚠️ **Não confundir:** iClin (`iclin.com.br`, agendamento) ≠ iClinic (`iclinic.com.br`).

---

## Variáveis de ambiente (aba `iclin` da planilha)

| Chave | Obrigatória | Default | Descrição |
|---|---|---|---|
| `API_USER` | ✅ | — | Credencial `app-api-user` |
| `API_KEY` | ✅ | — | Credencial `app-api-key` |
| `PROJECT_ID` | ✅ | — | Projeto GCP (`inbv-nalk`) |
| `CRM_TYPE` | ✅ | — | Dataset destino → `iclin` |
| `GOOGLE_APPLICATION_CREDENTIALS` | ✅ | — | Credencial GCS |
| `API_BASE_URL` | — | `https://iclin.com.br/web/inter` | Base da API |
| `LOOKBACK_DAYS` | — | `365` | Dias de atendimentos a varrer (dia-a-dia) |
| `FETCH_DETAILS` | — | `1` | `1` busca detalhe/serviços/cliente por atendimento; `0` grava só a lista base |

---

## Classes e métodos

### `Agendas` — base `{BASE}/Agendas/`
| Método | Params | Lido? | Uso |
|---|---|---|---|
| `listar_unid` | cod_unid | ✅ | unidades da clínica |
| `listar_age` | cod_unid, nage, aol_permite | ✅ | agendas (iteramos por unidade) |
| `listar_conv` | **nage\*** | ✅ | convênios da agenda (iteramos por agenda) |
| `listar_hr_livres` | nage*, quant_hr | ❌ | horários livres (tempo real, não-BI) |
| `listar_hr_cpf` | cpf* | ❌ | bookings por CPF (não serve p/ varredura) |
| `mostrar_hr` | nhr* | ❌ | detalhe de horário |
| `listar_paol` / `ocupar_paol` | — | ❌ | pronto-atendimento online |
| `ocupar_hr` | vários* | ❌ | **escrita** — fora de escopo (read-only) |

### `Atend` — base `{BASE}/Atend/`
| Método | Params | Lido? | Uso |
|---|---|---|---|
| `listar_atend_data` | **data\*** (dd-mm-yyyy), cod_unid, cod_prof | ✅ | ⭐ atendimentos por dia (varredura) |
| `mostrar_atend` | nat* | ✅ | detalhe do atendimento (mesclado no registro) |
| `listar_serv_atend` | nat* | ✅ | serviços do atendimento (1 linha/serviço) |
| `mostrar_cli` | ncli* | ✅ | dados do cliente (derivado dos atendimentos) |
| `listar_cli_cpf` | cpf* | ❌ | clientes por CPF (não usado) |

---

## Estratégia de extração

1. **Dimensões:** `listar_unid` → para cada unidade `listar_age` → para cada agenda (`nage`) `listar_conv`.
2. **Fatos:** varre `LOOKBACK_DAYS` dias, 1 `listar_atend_data(data)` por dia (a API
   **não aceita range** — só data única). Dos atendimentos derivamos `nat` e `ncli`:
   - `mostrar_atend(nat)` → detalhe (mesclado em `atendimentos`)
   - `listar_serv_atend(nat)` → `atendimento_servicos`
   - `mostrar_cli(ncli)` → `clientes`
3. Detalhamento é paralelizado (`MAX_WORKERS`) e pode ser desligado com `FETCH_DETAILS=0`.

## Tabelas geradas (dataset `iclin`)
`unidades`, `agendas`, `convenios`, `atendimentos`, `atendimento_servicos`, `clientes`
(+ versões `*_gold` em `vendas`, ver `sheet.sql`).

---

## Pendências de validação (teste com credenciais reais)
1. **Formato da resposta** — é JSON? Qual a chave da lista? `_extract_items` tenta
   `dados/data/result/retorno/lista/itens/items/registros/rows` e fallback p/ dict único.
2. **Nomes reais dos campos id** — assumimos `nat`/`ncli`/`nage`/`cod_unid`;
   `_first_key` tenta variações. Confirmar e fixar.
3. **Corpo da requisição** — assumimos form-encoded (`data=`). Se a API exigir JSON,
   trocar para `json_data=` no helper `post()`.
4. **`listar_unid`/`listar_age` sem parâmetros** retornam tudo? (senão, precisamos de
   um `cod_unid` semente).
5. **Rate limit real** (ajustar `RATE_LIMIT`, hoje 90 req/min) e custo do backfill
   (LOOKBACK_DAYS dias × N atendimentos × 2-3 chamadas de detalhe).
6. **Sem endpoint financeiro/faturamento** exposto — receita não é coletável por aqui.
