# Asaas API — Mapeamento de Endpoints

**Base URL Produção:** `https://api.asaas.com`
**Base URL Sandbox:** `https://api-sandbox.asaas.com`
**Autenticação:** Header `access_token: $ASAAS_API_KEY`
**Docs:** https://docs.asaas.com/reference/comece-por-aqui

---

## Paginação (padrão para todos os GET de listagem)

Todos os endpoints de listagem usam `offset` / `limit`:

```
?offset=0&limit=100
```

Resposta padrão:
```json
{
  "object": "list",
  "hasMore": true,
  "totalCount": 1500,
  "limit": 100,
  "offset": 0,
  "data": [...]
}
```

- `limit` máximo: **100**
- Iterar enquanto `hasMore == true`, incrementando `offset += limit`

---

## Endpoints para Extração (GET)

### 1. Clientes

| Método | Path | Descrição |
|--------|------|-----------|
| GET | `/v3/customers` | Listar clientes |

**Filtros disponíveis:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `offset` | int | Posição inicial (default: 0) |
| `limit` | int | Itens por página, max 100 (default: 10) |
| `name` | string | Filtrar por nome |
| `email` | string | Filtrar por email |
| `cpfCnpj` | string | Filtrar por CPF/CNPJ |
| `groupName` | string | Filtrar por grupo |
| `externalReference` | string | Filtrar por referência externa |

---

### 2. Cobranças (Payments)

| Método | Path | Descrição |
|--------|------|-----------|
| GET | `/v3/payments` | Listar cobranças |
| GET | `/v3/payments/{id}` | Recuperar cobrança única |

**Filtros disponíveis (listagem):**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `offset` | int | Posição inicial |
| `limit` | int | Itens por página, max 100 |
| `status` | string | PENDING, RECEIVED, CONFIRMED, OVERDUE, REFUNDED, etc. |
| `customer` | string | ID do cliente no Asaas |
| `subscription` | string | ID da assinatura |
| `installment` | string | ID do parcelamento |
| `externalReference` | string | Referência externa |
| `billingType` | string | BOLETO, CREDIT_CARD, PIX |
| `dueDate[ge]` / `dueDate[le]` | date | Filtro por vencimento |
| `paymentDate[ge]` / `paymentDate[le]` | date | Filtro por data de pagamento |
| `estimatedCreditDate[ge]` / `estimatedCreditDate[le]` | date | Filtro por data estimada de crédito |

---

### 3. Assinaturas (Subscriptions)

| Método | Path | Descrição |
|--------|------|-----------|
| GET | `/v3/subscriptions` | Listar assinaturas |
| GET | `/v3/subscriptions/{id}/payments` | Listar cobranças de uma assinatura |

**Filtros disponíveis (listagem):**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `offset` | int | Posição inicial |
| `limit` | int | Itens por página, max 100 |
| `customer` | string | ID do cliente |
| `status` | string | Status da assinatura |
| `externalReference` | string | Referência externa |

**Endpoint dependente:** `/v3/subscriptions/{id}/payments`
- Depende da listagem de assinaturas — usar o campo `id` de cada assinatura

---

### 4. Parcelamentos (Installments)

| Método | Path | Descrição |
|--------|------|-----------|
| GET | `/v3/installments` | Listar parcelamentos |

**Filtros disponíveis:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `offset` | int | Posição inicial |
| `limit` | int | Itens por página, max 100 |
| `status` | string | Status do parcelamento |
| `customer` | string | ID do cliente |
| `dateCreatedFrom` | date | Data de criação (início) |
| `dateCreatedTo` | date | Data de criação (fim) |

---

### 5. Links de Pagamento (Payment Links)

| Método | Path | Descrição |
|--------|------|-----------|
| GET | `/v3/paymentLinks` | Listar links de pagamento |

**Filtros disponíveis:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `offset` | int | Posição inicial |
| `limit` | int | Itens por página, max 100 |

---

### 6. Transferências (Transfers)

| Método | Path | Descrição |
|--------|------|-----------|
| GET | `/v3/transfers` | Listar transferências |

**Filtros disponíveis:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `offset` | int | Posição inicial |
| `limit` | int | Itens por página, max 100 |

---

### 7. Financeiro

| Método | Path | Descrição |
|--------|------|-----------|
| GET | `/v3/finance/balance` | Saldo da conta |
| GET | `/v3/financialTransactions` | Extrato financeiro |

**Filtros do extrato:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `offset` | int | Posição inicial |
| `limit` | int | Itens por página, max 100 |
| `startDate` | date | Data início |
| `finishDate` | date | Data fim |

---

## Resumo para o ENDPOINTS dict (padrão do workspace)

```python
ENDPOINTS = {
    "customers":             "/v3/customers",
    "payments":              "/v3/payments",
    "subscriptions":         "/v3/subscriptions",
    "installments":          "/v3/installments",
    "payment_links":         "/v3/paymentLinks",
    "transfers":             "/v3/transfers",
    "financial_transactions": "/v3/financialTransactions",
    "balance":               "/v3/finance/balance",
}

DEPENDENT_ENDPOINTS = {
    "subscription_payments": {
        "endpoint": "/v3/subscriptions/{id}/payments",
        "parent": "subscriptions",
        "id_field": "id",
    },
}
```

---

## Enums de referência

**billingType:** `UNDEFINED`, `BOLETO`, `CREDIT_CARD`, `PIX`

**Payment status:** `PENDING`, `RECEIVED`, `CONFIRMED`, `OVERDUE`, `REFUNDED`, `RECEIVED_IN_CASH`, `REFUND_REQUESTED`, `REFUND_IN_PROGRESS`, `CHARGEBACK_REQUESTED`, `CHARGEBACK_DISPUTE`, `AWAITING_CHARGEBACK_REVERSAL`, `DUNNING_REQUESTED`, `DUNNING_RECEIVED`, `AWAITING_RISK_ANALYSIS`

**Subscription cycle:** `MONTHLY`, `QUARTERLY`, `SEMI_ANNUALLY`, `YEARLY`