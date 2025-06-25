CREATE OR REPLACE TABLE `nalk-app-demo.vendas.clickup_clientes_gold_v2` AS
WITH base_dados AS (
  SELECT
    -- Campos básicos
    Task_ID,
    Task_Custom_ID,
    Task_Name,

    -- Assignee com tratamento (remoção de [])
    TRIM(REGEXP_REPLACE(Assignee, r'^\[|\]$', '')) AS Assignee,

    -- Priority e Status
    Priority,
    Status,

    -- Produtos Vendidos com tratamento (remoção de [])
    TRIM(REGEXP_REPLACE(Produtos_Vendidos__labels_, r'^\[|\]$', '')) AS Produtos_Vendidos__labels_,

    -- Campos de data com formato YYYY-M-DD, HH:MM (com hora)
    CASE
      WHEN Due_Date IS NOT NULL
           AND LOWER(CAST(Due_Date AS STRING)) NOT IN ('nan', '', 'null')
           AND LENGTH(TRIM(CAST(Due_Date AS STRING))) > 0
      THEN
        CASE
          WHEN CONTAINS_SUBSTR(CAST(Due_Date AS STRING), ',') THEN
            FORMAT_DATETIME('%Y-%-m-%-d, %H:%M',
              DATETIME(
                PARSE_DATE('%A, %B %d %Y',
                  REGEXP_REPLACE(CAST(Due_Date AS STRING), r'(\d+)(st|nd|rd|th)', r'\1')),
                TIME(12, 13, 0)
              ))
          ELSE NULL
        END
      ELSE NULL
    END AS Due_Date,

    CASE
      WHEN Start_Date IS NOT NULL
           AND LOWER(CAST(Start_Date AS STRING)) NOT IN ('nan', '', 'null')
           AND LENGTH(TRIM(CAST(Start_Date AS STRING))) > 0
      THEN
        CASE
          WHEN CONTAINS_SUBSTR(CAST(Start_Date AS STRING), ',') THEN
            FORMAT_DATETIME('%Y-%-m-%-d, %H:%M',
              DATETIME(
                PARSE_DATE('%A, %B %d %Y',
                  REGEXP_REPLACE(CAST(Start_Date AS STRING), r'(\d+)(st|nd|rd|th)', r'\1')),
                TIME(12, 13, 0)
              ))
          ELSE NULL
        END
      ELSE NULL
    END AS Start_Date,

    -- Campos de data com formato YYYY-M-DD (apenas data)
    CASE
      WHEN Date_Created IS NOT NULL
           AND LOWER(CAST(Date_Created AS STRING)) NOT IN ('nan', '', 'null')
           AND LENGTH(TRIM(CAST(Date_Created AS STRING))) > 0
      THEN
        CASE
          WHEN CONTAINS_SUBSTR(CAST(Date_Created AS STRING), ',') THEN
            FORMAT_DATE('%Y-%-m-%-d',
              PARSE_DATE('%A, %B %d %Y',
                REGEXP_REPLACE(
                  REGEXP_EXTRACT(CAST(Date_Created AS STRING), r'^[^,]+, [^,]+'),
                  r'(\d+)(st|nd|rd|th)', r'\1')))
          ELSE NULL
        END
      ELSE NULL
    END AS Date_Created,

    CASE
      WHEN Date_Done IS NOT NULL
           AND LOWER(CAST(Date_Done AS STRING)) NOT IN ('nan', '', 'null')
           AND LENGTH(TRIM(CAST(Date_Done AS STRING))) > 0
      THEN
        CASE
          WHEN CONTAINS_SUBSTR(CAST(Date_Done AS STRING), ',') THEN
            FORMAT_DATE('%Y-%-m-%-d',
              PARSE_DATE('%A, %B %d %Y',
                REGEXP_REPLACE(
                  REGEXP_EXTRACT(CAST(Date_Done AS STRING), r'^[^,]+, [^,]+'),
                  r'(\d+)(st|nd|rd|th)', r'\1')))
          ELSE NULL
        END
      ELSE NULL
    END AS Date_Done,

    CASE
      WHEN Date_Updated IS NOT NULL
           AND LOWER(CAST(Date_Updated AS STRING)) NOT IN ('nan', '', 'null')
           AND LENGTH(TRIM(CAST(Date_Updated AS STRING))) > 0
      THEN
        CASE
          WHEN CONTAINS_SUBSTR(CAST(Date_Updated AS STRING), ',') THEN
            FORMAT_DATE('%Y-%-m-%-d',
              PARSE_DATE('%A, %B %d %Y',
                REGEXP_REPLACE(
                  REGEXP_EXTRACT(CAST(Date_Updated AS STRING), r'^[^,]+, [^,]+'),
                  r'(\d+)(st|nd|rd|th)', r'\1')))
          ELSE NULL
        END
      ELSE NULL
    END AS Date_Updated,

    CASE
      WHEN Data___Contrato__date_ IS NOT NULL
           AND LOWER(CAST(Data___Contrato__date_ AS STRING)) NOT IN ('nan', '', 'null')
           AND LENGTH(TRIM(CAST(Data___Contrato__date_ AS STRING))) > 0
      THEN
        CASE
          WHEN CONTAINS_SUBSTR(CAST(Data___Contrato__date_ AS STRING), ',') THEN
            FORMAT_DATE('%Y-%-m-%-d',
              PARSE_DATE('%A, %B %d %Y',
                REGEXP_REPLACE(CAST(Data___Contrato__date_ AS STRING), r'(\d+)(st|nd|rd|th)', r'\1')))
          ELSE NULL
        END
      ELSE NULL
    END AS Data___Contrato__date_,

    CASE
      WHEN Data___Kickoff__date_ IS NOT NULL
           AND LOWER(CAST(Data___Kickoff__date_ AS STRING)) NOT IN ('nan', '', 'null')
           AND LENGTH(TRIM(CAST(Data___Kickoff__date_ AS STRING))) > 0
      THEN
        CASE
          WHEN CONTAINS_SUBSTR(CAST(Data___Kickoff__date_ AS STRING), ',') THEN
            FORMAT_DATE('%Y-%-m-%-d',
              PARSE_DATE('%A, %B %d %Y',
                REGEXP_REPLACE(CAST(Data___Kickoff__date_ AS STRING), r'(\d+)(st|nd|rd|th)', r'\1')))
          ELSE NULL
        END
      ELSE NULL
    END AS Data___Kickoff__date_,

    CASE
      WHEN Data___Entrega__Produto___date_ IS NOT NULL
           AND LOWER(CAST(Data___Entrega__Produto___date_ AS STRING)) NOT IN ('nan', '', 'null')
           AND LENGTH(TRIM(CAST(Data___Entrega__Produto___date_ AS STRING))) > 0
      THEN
        CASE
          WHEN CONTAINS_SUBSTR(CAST(Data___Entrega__Produto___date_ AS STRING), ',') THEN
            FORMAT_DATE('%Y-%-m-%-d',
              PARSE_DATE('%A, %B %d %Y',
                REGEXP_REPLACE(CAST(Data___Entrega__Produto___date_ AS STRING), r'(\d+)(st|nd|rd|th)', r'\1')))
          ELSE NULL
        END
      ELSE NULL
    END AS Data___Entrega__Produto___date_,

    CASE
      WHEN Data___Churn__date_ IS NOT NULL
           AND LOWER(CAST(Data___Churn__date_ AS STRING)) NOT IN ('nan', '', 'null')
           AND LENGTH(TRIM(CAST(Data___Churn__date_ AS STRING))) > 0
      THEN
        CASE
          WHEN CONTAINS_SUBSTR(CAST(Data___Churn__date_ AS STRING), ',') THEN
            FORMAT_DATE('%Y-%-m-%-d',
              PARSE_DATE('%A, %B %d %Y',
                REGEXP_REPLACE(CAST(Data___Churn__date_ AS STRING), r'(\d+)(st|nd|rd|th)', r'\1')))
          ELSE NULL
        END
      ELSE NULL
    END AS Data___Churn__date_,

    CASE
      WHEN Data___1___pagamento__date_ IS NOT NULL
           AND LOWER(CAST(Data___1___pagamento__date_ AS STRING)) NOT IN ('nan', '', 'null')
           AND LENGTH(TRIM(CAST(Data___1___pagamento__date_ AS STRING))) > 0
      THEN
        CASE
          WHEN CONTAINS_SUBSTR(CAST(Data___1___pagamento__date_ AS STRING), ',') THEN
            FORMAT_DATE('%Y-%-m-%-d',
              PARSE_DATE('%A, %B %d %Y',
                REGEXP_REPLACE(CAST(Data___1___pagamento__date_ AS STRING), r'(\d+)(st|nd|rd|th)', r'\1')))
          ELSE NULL
        END
      ELSE NULL
    END AS Data___1___pagamento__date_,

    CASE
      WHEN Data___Negocia____o__date_ IS NOT NULL
           AND LOWER(CAST(Data___Negocia____o__date_ AS STRING)) NOT IN ('nan', '', 'null')
           AND LENGTH(TRIM(CAST(Data___Negocia____o__date_ AS STRING))) > 0
      THEN
        CASE
          WHEN CONTAINS_SUBSTR(CAST(Data___Negocia____o__date_ AS STRING), ',') THEN
            FORMAT_DATE('%Y-%-m-%-d',
              PARSE_DATE('%A, %B %d %Y',
                REGEXP_REPLACE(CAST(Data___Negocia____o__date_ AS STRING), r'(\d+)(st|nd|rd|th)', r'\1')))
          ELSE NULL
        END
      ELSE NULL
    END AS Data___Negocia____o__date_,

    CASE
      WHEN Inadimplentes___Previs__o_de_Retorno__date_ IS NOT NULL
           AND LOWER(CAST(Inadimplentes___Previs__o_de_Retorno__date_ AS STRING)) NOT IN ('nan', '', 'null')
           AND LENGTH(TRIM(CAST(Inadimplentes___Previs__o_de_Retorno__date_ AS STRING))) > 0
      THEN
        CASE
          WHEN CONTAINS_SUBSTR(CAST(Inadimplentes___Previs__o_de_Retorno__date_ AS STRING), ',') THEN
            FORMAT_DATE('%Y-%-m-%-d',
              PARSE_DATE('%A, %B %d %Y',
                REGEXP_REPLACE(CAST(Inadimplentes___Previs__o_de_Retorno__date_ AS STRING), r'(\d+)(st|nd|rd|th)', r'\1')))
          ELSE NULL
        END
      ELSE NULL
    END AS Inadimplentes___Previs__o_de_Retorno__date_,

    CASE
      WHEN Inadimpl__ncia___Data_inicial__date_ IS NOT NULL
           AND LOWER(CAST(Inadimpl__ncia___Data_inicial__date_ AS STRING)) NOT IN ('nan', '', 'null')
           AND LENGTH(TRIM(CAST(Inadimpl__ncia___Data_inicial__date_ AS STRING))) > 0
      THEN
        CASE
          WHEN CONTAINS_SUBSTR(CAST(Inadimpl__ncia___Data_inicial__date_ AS STRING), ',') THEN
            FORMAT_DATE('%Y-%-m-%-d',
              PARSE_DATE('%A, %B %d %Y',
                REGEXP_REPLACE(CAST(Inadimpl__ncia___Data_inicial__date_ AS STRING), r'(\d+)(st|nd|rd|th)', r'\1')))
          ELSE NULL
        END
      ELSE NULL
    END AS Inadimpl__ncia___Data_inicial__date_,

    -- Campos de progresso e localização
    Progresso__automatic_progress_,
    Endere__o__location_,

    -- URLs e contatos
    Site_da_Empresa__url_,
    Respons__vel_pelo_Projeto__short_text_,
    Telefone_do_Respons__vel__phone_,
    Email__email_,

    -- Ferramentas e labels
    Ferramentas__labels_,
    Suporte___Ferramenta__labels_,

    -- Textos e observações
    Expectativa_do_Cliente__text_,
    Observa____es_sobre_o_Cliente__text_,
    Objetivos_de_Neg__cio_do_Cliente_com_o_Data_Nalk__text_,
    Principais_Desafios__text_,
    KPIs_Priorit__rios__text_,
    Sobre_a_Empresa__text_,
    Qual_dor_o_cliente_quer_resolver_com_o_Data_Nalk___short_text_,

    -- Drop downs e seletores
    Closer__drop_down_,
    Modelo_de_Neg__cio__drop_down_,
    Segmento_de_Mercado__drop_down_,
    Origem__drop_down_,
    Plano__drop_down_,
    Usu__rios__drop_down_,
    Etapa__drop_down_,
    IA__drop_down_,
    Respons__vel___Eng__Dados__drop_down_,
    Metas__drop_down_,
    Inadimpl__ncia___Motivo__drop_down_,
    Cancelamento___Motivo__drop_down_,
    Cliente_Estrat__gico__drop_down_,
    Prioridade___Entregas__drop_down_,
    Possui_ferramenta_nova___drop_down_,
    Atua____o__drop_down_,
    Prioridade___Ordem__drop_down_,
    Inadimpl__ncia___Risco__drop_down_,
    Inadimpl__ncia___Atraso__drop_down_,
    Inadimpl__ncia___Isen____o__drop_down_,
    Inadimpl__ncia___Extrajudicial__drop_down_,
    Inadimpl__ncia___Desconto__drop_down_,
    Inadimpl__ncia___Devolu____o__drop_down_,
    Parceiro__drop_down_,
    Em_uma_escala_de_0_a_10__como_voc___avalia_a_sua_experi__ncia_conosco___drop_down_,
    Tipo__drop_down_,
    Tipo_de_Visualiza____o__drop_down_,

    -- URLs e links
    Link_da_Oportunidade_no_CRM__url_,
    Link_da_Negocia____o_no_Tldv__url_,

    -- Attachments
    Proposta__attachment_,

    -- Campos de texto curto
    Cargo_do_Respons__vel_pelo_Projeto__short_text_,
    Empresa__short_text_,
    Raz__o_Social__short_text_,

    -- Labels especiais
    Quais_ferramentas_de_IA_da_Nalk_voc___gostaria_de_usar_para_capta____o_de_leads___labels_,

    -- Campos de moeda (currency)
    Valor___Setup__currency_,
    MRR__currency_,
    Faturamento_da_Empresa__currency_,
    Inadimpl__ncia___Valor__currency_,
    Atraso___Valor__currency_,
    Isen____o___Valor__currency_,
    Devolu____o___Valor__currency_,
    Extrajudicial___Valor__currency_,
    Desconto___Valor__currency_,

    -- Campos numéricos
    Tempo_de_Entrega__Dias___number_,
    CNPJ__number_,
    ID_da_Oportunidade__number_,
    Ferramentas___Novas__number_,
    CNPJ___Contrato__number_,
    Pagamento___Dia__number_,

    -- Campos de fórmula
    Tempo_Restante___Entrega__formula_,
    Data___Entrega__Contrato___formula_,

    -- Campos de meses 2025 (Jan/25 a Dez/25)
    Jan_25__drop_down_,
    Fev_25__drop_down_,
    Mar_25__drop_down_,
    Abr_25__drop_down_,
    Mai_25__drop_down_,
    Jun_25__drop_down_,
    Jul_25__drop_down_,
    Ago_25__drop_down_,
    Set_25__drop_down_,
    Out_25__drop_down_,
    Nov_25__drop_down_,
    Dez_25__drop_down_

  FROM `nalk-app-demo.clickup.clientes`
),

-- ferramentas_principais AS (
--   SELECT
--     *,  -- Todos os campos da base_dados
--     TRIM(ferramenta_individual) AS Ferramenta,
--     'Ferramentas_Principais' AS Tipo_Campo
--   FROM base_dados,
--   UNNEST(SPLIT(REGEXP_REPLACE(Ferramentas__labels_, r'^\[|\]$', ''), ',')) AS ferramenta_individual
--   WHERE
--     Ferramentas__labels_ IS NOT NULL
--     AND LENGTH(TRIM(Ferramentas__labels_)) > 0
--     AND TRIM(Ferramentas__labels_) NOT IN ('[]', '', 'nan', 'null')
--     AND TRIM(ferramenta_individual) != ''
-- ),

ferramentas_suporte AS (
  SELECT
    *,  -- Todos os campos da base_dados
    TRIM(ferramenta_individual) AS Ferramenta,
    'Ferramentas_Suporte' AS Tipo_Campo
  FROM base_dados,
  UNNEST(SPLIT(REGEXP_REPLACE(Suporte___Ferramenta__labels_, r'^\[|\]$', ''), ',')) AS ferramenta_individual
  WHERE
    Suporte___Ferramenta__labels_ IS NOT NULL
    AND LENGTH(TRIM(Suporte___Ferramenta__labels_)) > 0
    AND TRIM(Suporte___Ferramenta__labels_) NOT IN ('[]', '', 'nan', 'null')
    AND TRIM(ferramenta_individual) != ''
)

-- Union das duas fontes mantendo TODOS os campos
-- SELECT * FROM ferramentas_principais
-- UNION ALL
SELECT * FROM ferramentas_suporte
ORDER BY Task_ID, Tipo_Campo, Ferramenta;