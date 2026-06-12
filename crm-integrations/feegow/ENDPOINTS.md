# Feegow API v1 — Mapeamento de Endpoints

**Base URL:** `https://api.feegow.com/v1/api`
**Autenticação:** token estático no header `x-access-token: {token}`
**Como obter o token:** o usuário master da licença libera na interface do Feegow Clinic
**Formato:** JSON (`Content-Type: application/json`)
**Docs:** https://docs.feegow.com/

> Domínio: gestão de clínicas/saúde. Auth = padrão do `asaas` (token no header),
> sem OAuth/refresh.

---

## Variáveis de ambiente (aba `feegow` da planilha)

| Chave | Obrigatória | Default | Descrição |
|---|---|---|---|
| `API_ACCESS_TOKEN` | ✅ | — | Token `x-access-token` |
| `PROJECT_ID` | ✅ | — | Projeto GCP |
| `CRM_TYPE` | ✅ | — | Dataset destino → `feegow` |
| `GOOGLE_APPLICATION_CREDENTIALS` | ✅ | — | Credencial GCS |
| `API_BASE_URL` | — | `https://api.feegow.com/v1/api` | Base da API |
| `LOOKBACK_DAYS` | — | `365` | Janela (dias) p/ dados por data |
| `PATIENT_STRATEGY` | — | `date` | `date` ou `appointments` (ver abaixo) |

---

## Paginação
- Parâmetros: **`start`** (registro inicial, default 0) + **`offset`** (tamanho da
  página, default 50).
- O extractor pagina incrementando `start += offset` e para quando uma página
  retorna menos que `offset` itens.
- ⚠️ **Assunção:** `offset` = tamanho da página (não "pular N"). A doc é
  ambígua — confirmar no teste real. Há guarda contra paginação que não avança
  (1º item repetido) e contra loop infinito (`MAX_PAGES_GUARD`).

## Datas
- Filtros `data_start` / `data_end` no formato **`DD-MM-YYYY`**.
- Calculados por run a partir de `LOOKBACK_DAYS`.

## Rate limit
- **Não documentado.** Extractor começa conservador em **120 req/min** (`RATE_LIMIT`).

---

## Endpoints extraídos

### Cadastros simples (sem filtro obrigatório — 1 chamada) ✅ validado
| Tabela | Endpoint | Método |
|---|---|---|
| `profissionais` | `professional/list` | GET |
| `procedimentos` | `procedures/list` | GET |
| `especialidades` | `specialties/list` | GET |
| `convenios` | `insurance/list` | GET |
| `unidades` | `company/list-unity` | GET |
| `status` | `appoints/status` | GET |
| `motivos` | `appoints/motives` | GET |

**Dimensão `status` (id → rótulo) — validada na licença 36514 (12 status):**
`1` Marcado - não confirmado · `2` Em atendimento · `3` Atendido · `4` Aguardando ·
`5` Chamando · `6` **Não compareceu (no-show)** · `7` Marcado - confirmado ·
`11` Desmarcado pelo paciente · `15` Remarcado · `22` Cancelado pelo profissional ·
`33` Em espera · `208` Aguardando pagamento.
Join: `agendamentos.status_id` → `status.id`. **`motivos`** (`{id, motivo}`): 1=Solicitado pelo Paciente, 2=pelo Profissional, 3=pela Clínica.

### Agendamentos — `appoints/search` (janela FATIADA) ✅ validado
| Tabela | Endpoint | Obrigatório | Observação |
|---|---|---|---|
| `agendamentos` | `appoints/search` | `data_start` + `data_end` (DD-MM-YYYY) | janela máx. entre 90 e 180 dias → fatiar em `APPOINTS_WINDOW_DAYS` (60). **HTTP 409 = período sem agendamentos** (tratado como vazio). Retorna a janela inteira numa resposta (a guarda de "paginação não avança" do `fetch_all_pages` cobre isso). |

### Pacientes ✅ validado
- **`PATIENT_STRATEGY=list` (default)** → **`patient/list`** paginado por **`limit`+`offset`**
  (offset = registros a pular; 500/página). Pega TODOS os pacientes da clínica
  (inclusive sem agenda) em ~N/500 chamadas, **sem rate limit**. Campos enxutos:
  `patient_id, nome, nome_social, nascimento, bairro, tabela_id, sexo_id, email,
  celular, criado_em, alterado_em, programa_de_saude`. ⚠️ `patient/list` ignora
  `start`/`offset`-do-outro-padrão e o campo `total` é o tamanho da página (500),
  não o total real — paginar por `offset += 500` até página < 500.
- `PATIENT_STRATEGY=appointments` → deriva `paciente_id` dos agendamentos e busca
  1 a 1 (`patient/search?paciente_id=X` → 200, campos ricos com endereço/documento).
  ⚠️ 1 request/paciente → lento e **estoura o rate limit** (caso real: 6.357 pacientes).
- `PATIENT_STRATEGY=date`: **não funciona** — `patient/search` exige `paciente_id`
  ou `paciente_cpf` (422: *"O campo paciente id é obrigatório quando paciente cpf
  não está presente"*). Mantido só por compat.

### Financeiro READ-ONLY — VALIDADO na licença 36514 (12/06/2026)

**⚠️ Datas em ISO `YYYY-MM-DD`** (todo o resto da API é DD-MM-YYYY!). Dois envelopes:
`financial/*` → `{success, content:[...]}`; `core/financial/*` → `{data:[...]}` paginado (page + perPage/limit, "version 3.0").

**Transacionais** `financial/*` (janela ISO; nomes de param **inconsistentes** entre endpoints):
| Tabela | Endpoint | Params | Obs |
|---|---|---|---|
| `financeiro_vendas` | `financial/list-sales` | `date_start`,`date_end`,**`unidade_id`** | varre todas as unidades. `{invoice_id,timestamp,amount,type}` |
| `financeiro_repasses` | `financial/list-medical-transfer` | `data_start`,`data_end` | `{funcao,valor,associacao_id,conta_id,criado_em,situacao}` |

**Dimensões** `financial/*` (sem params): `financeiro_fornecedores` = `financial/list-suppliers`; `financeiro_bandeiras_cartao` = `financial/credit-card-flags`.

**`core/financial/*`** (paginado page+perPage/limit, lista em `data`):
| Tabela | Endpoint |
|---|---|
| `financeiro_plano_contas` | `core/financial/base/financial-category` |
| `financeiro_centro_custo` | `core/financial/base/cost-center` |
| `financeiro_conta_corrente` | `core/financial/base/current-accounts` |
| `financeiro_produtos` | `core/financial/base/product/list` |
| `financeiro_estoque` | `core/financial/base/product/position` |
| `financeiro_vouchers` | `core/financial/voucher/list` |

**FORA (deliberado):** `financial/list-invoice` (exige `tipo_transacao` C/D/T + estrutura aninhada `{detalhes,pagamentos,itens}` — não encaixa no pipeline flat; revenue já vem de `list-sales`). `financial/dmed` (per-CPF, fiscal). Endpoints de escrita (create/pay/remove/voucher edit etc.) ignorados (read-only).

⚠️ **Lição (12/06):** `422 {"success":false,"cod_erro":0,"message":""}` = **rota INEXISTENTE** (idêntico a `financial/banana`), NÃO falta de permissão. Os paths `sales-list`/`list-accounts`/`list-providers`/`chart-accounts` etc. (chutes/resumo impreciso) **não existem**. Sempre validar path real pela doc HTML crua de docs.feegow.com (tem as URLs embutidas) — o resumo via WebFetch erra os nomes.

---

## Validação com token real (licença 36514) — 2026-06-10
1. ✅ `/patient/search` **não** aceita só data → exige `paciente_id`/`cpf` → default = `appointments`.
2. ✅ `appoints/search` ignora `start`/`offset` e devolve a janela inteira; **janela >~90d → 409**. Fix: fatiar em 60d + tratar 409 como vazio.
3. ✅ `/financial/*` → 422 (módulo não habilitado) → tornados opcionais.
4. ℹ️ Rate limit real não testado a fundo; `RATE_LIMIT=120/min` mantido. Backfill de 365d derivou ~6.3k pacientes (1 request cada) — considerar reduzir `LOOKBACK_DAYS` em runs incrementais.
5. ✅ Chave da lista = `content` (confirmado). Resposta tem envelope `{"success":true,"content":[...]}`.
6. 📊 Run real: 8 profissionais, 60 procedimentos, 24 convênios, 1 unidade, 1 especialidade, ~10k agendamentos, ~6.3k pacientes únicos.

---

## Endpoints mapeados mas NÃO extraídos (disponíveis se necessário)
- Agenda: `appoints/available-schedule` (`appoints/status` e `appoints/motives` agora SÃO extraídos)
- Bloqueios: `lock/list`
- Prontuários/Laudos: `laudos/*`
- Relatórios: `reports/list`, `reports/generate`
- Funcionários: `employee/list`
- Cartão Benefício: base `https://cartao-beneficios.feegow.com/external/`
