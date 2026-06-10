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

### Cadastros simples (sem filtro obrigatório — 1 chamada)
| Tabela | Endpoint | Método |
|---|---|---|
| `profissionais` | `professional/list` | GET |
| `procedimentos` | `procedures/list` | GET |
| `especialidades` | `specialties/list` | GET |
| `convenios` | `insurance/list` | GET |
| `unidades` | `company/list-unity` | GET |
| `financeiro_contas` | `financial/accounts` | GET |
| `financeiro_fornecedores` | `financial/suppliers` | GET |

### Por janela de data (paginados)
| Tabela | Endpoint | Método | Obrigatório |
|---|---|---|---|
| `agendamentos` | `appoints/search` | GET | `data_start` + `data_end` |
| `financeiro_faturas` | `financial/invoice` | GET | filtros de data |

### Pacientes (estratégia configurável — `PATIENT_STRATEGY`)
| Estratégia | Como funciona | Trade-off |
|---|---|---|
| `date` (default) | `patient/search` por `data_start`/`data_end`, paginado | ⚠️ a doc lista data como *opcional* e exige "≥1 filtro" — pode rejeitar busca só por data |
| `appointments` | deriva `paciente_id` dos agendamentos e busca 1 a 1 (`patient/search?paciente_id=X`) | cobre só quem tem agenda; perde pacientes sem agendamento |

> Plano: usar `date`; se a API rejeitar, trocar `PATIENT_STRATEGY=appointments`.

---

## Pendências de validação (teste com token real)
1. `/patient/search` aceita busca **só por data**? (define a estratégia padrão)
2. Semântica real de `start`/`offset` (tamanho de página vs. skip)
3. Paginação dos `/financial/*` (a doc não detalha)
4. Rate limit real (ajustar `RATE_LIMIT`)
5. Janela de data adequada por cliente (`LOOKBACK_DAYS`)
6. Formato exato da resposta (chave da lista — assumido `content`)

---

## Endpoints mapeados mas NÃO extraídos (disponíveis se necessário)
- Agenda: `appoints/available-schedule`, `appoints/status`, `appoints/motives`
- Bloqueios: `lock/list`
- Prontuários/Laudos: `laudos/*`
- Relatórios: `reports/list`, `reports/generate`
- Funcionários: `employee/list`
- Cartão Benefício: base `https://cartao-beneficios.feegow.com/external/`
