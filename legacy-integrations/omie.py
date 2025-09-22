"""
Omie module for data extraction functions.
This module contains functions specific to the Contact2Sale integration.
"""

from core import gcs


def run(customer):
    import requests, pandas as pd, time, os, random, logging
    from google.cloud import storage
    from datetime import datetime
    import pathlib

    API_KEY = customer['api_key']
    API_SECRET = customer['api_secret']
    BUCKET_NAME = customer['bucket_name']
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler('omie_api.log'), logging.StreamHandler()])
    logger = logging.getLogger()

    # === FUNÇÕES AUXILIARES ===
    def fazer_requisicao_com_retry(url, payload, tentativas_max=8, tempo_espera=15, timeout=30, session=None):
        """Faz requisição para API Omie com retry automático"""
        use_session = session if session else requests
        for tentativa in range(tentativas_max):
            try:
                time.sleep(0.25)  # ~240 req/min
                response = use_session.post(url, json=payload, headers={'Content-type': 'application/json'},
                                            timeout=timeout)
                if response.status_code == 500:
                    delay = min(tempo_espera * (1.5 ** tentativa), 120) * random.uniform(0.75, 1.25)
                    logger.error(f"Erro 500. Tentativa {tentativa + 1}/{tentativas_max}. Aguardando {delay:.2f}s...")
                    time.sleep(delay)
                    continue
                response.raise_for_status()
                return response
            except requests.exceptions.HTTPError as e:
                if getattr(e.response, 'status_code', 0) == 429:
                    time.sleep(30 + random.uniform(0, 15))
                    continue
                logger.error(f"Erro HTTP: {e}")
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                delay = min(tempo_espera * (1.5 ** tentativa), 120) * random.uniform(0.75, 1.25)
                logger.warning(
                    f"Timeout/conexão: {e}. Tentativa {tentativa + 1}/{tentativas_max}. Aguardando {delay:.2f}s...")
                if tentativa < tentativas_max - 1:
                    time.sleep(delay)
                    continue
            except Exception as e:
                logger.error(f"Erro: {e}")
                if tentativa < tentativas_max - 1:
                    time.sleep(tempo_espera)
                    continue
        logger.error("Falha após todas tentativas.")
        return None

    def upload_to_gcs(df, gcs_path, sep='|'):
        """Upload de DataFrame para o Google Cloud Storage"""
        if df.empty:
            logger.warning("DataFrame vazio, upload cancelado.")
            return False
        try:
            csv_data = df.to_csv(sep=sep, index=False, quoting=1)
            storage_client = storage.Client(project=customer['project_id'])
            bucket = storage_client.bucket(BUCKET_NAME)

            # Upload do arquivo principal
            blob = bucket.blob(gcs_path)
            blob.upload_from_string(csv_data, content_type='text/csv')
            logger.info(f"Arquivo enviado: gs://{BUCKET_NAME}/{gcs_path}")

            return True
        except Exception as e:
            logger.error(f"Erro ao fazer upload: {e}")
            raise

    # === COLETA: CATEGORIAS ===
    def extrair_dados_aninhados_categorias(categoria):
        """Extrai campos aninhados da categoria"""
        dados_dre = categoria.get('dadosDRE', {})
        return {
            "dadosDRE_codigo": dados_dre.get('codigo', ''),
            "dadosDRE_descricao": dados_dre.get('descricao', ''),
            "dadosDRE_nivel": dados_dre.get('nivel', ''),
            "dadosDRE_ordem": dados_dre.get('ordem', ''),
            "dadosDRE_tipo": dados_dre.get('tipo', '')
        }

    def coletar_categorias():
        """Coleta categorias da API Omie"""
        URL = "https://app.omie.com.br/api/v1/geral/categorias/"
        ENDPOINT_NAME = "listar_categorias"
        pagina, resultados = 1, []
        tentativas_sem_dados = 0
        inicio = datetime.now()

        logger.info("Iniciando coleta de categorias...")
        while tentativas_sem_dados < 10:
            payload = {
                "call": "ListarCategorias",
                "app_key": API_KEY, "app_secret": API_SECRET,
                "param": [{"pagina": pagina, "registros_por_pagina": 500}]
            }

            response = fazer_requisicao_com_retry(URL, payload)
            if not response:
                tentativas_sem_dados += 1
                logger.warning(f"Sem resposta na página {pagina}. Tentativa sem dados {tentativas_sem_dados}/10")
                pagina += 1
                continue

            dados = response.json()
            categorias = dados.get('categoria_cadastro', [])
            if not categorias:
                tentativas_sem_dados += 1
                logger.warning(f"Nenhuma categoria na página {pagina}. Tentativa sem dados {tentativas_sem_dados}/10")
                pagina += 1
                continue

            tentativas_sem_dados = 0  # Reseta contador ao encontrar dados
            for categoria in categorias:
                categoria.update(extrair_dados_aninhados_categorias(categoria))
                resultados.append(categoria)

            logger.info(f"Página {pagina}: +{len(categorias)} categorias. Total: {len(resultados)}")
            if pagina >= dados.get('total_de_paginas', 1):
                break
            pagina += 1

        if not resultados:
            logger.warning("Nenhuma categoria coletada.")
            return

        df = pd.DataFrame(resultados)
        colunas_ordenadas = ['codigo', 'descricao', 'categoria_superior', 'descricao_padrao', 'dadosDRE_codigo',
                             'dadosDRE_descricao', 'dadosDRE_nivel', 'dadosDRE_ordem', 'dadosDRE_tipo']
        colunas_existentes = [col for col in colunas_ordenadas if col in df.columns]
        if colunas_existentes:
            df = df[colunas_existentes]
        upload_to_gcs(df, f"{ENDPOINT_NAME}/categorias_omie.csv")
        logger.info(f"Coleta de categorias finalizada em {datetime.now() - inicio}")

    # === COLETA: PEDIDOS ===
    def extrair_campos_aninhados_pedidos(p):
        """Extrai campos aninhados do pedido"""
        campos = {}

        # Processa cabecalho
        for k, v in p.get("cabecalho", {}).items():
            campos[f"cabecalho_{k}"] = v

        # Processa detalhes do pedido
        if p.get("det"):
            det = p["det"][0]
            # Campos de ide
            for k, v in det.get("ide", {}).items():
                campos[f"ide_{k}"] = v
            # Campos de imposto
            for tipo, dados in det.get("imposto", {}).items():
                if isinstance(dados, dict):
                    for k, v in dados.items():
                        campos[f"imposto_{tipo}_{k}"] = v
            # Outros campos
            for campo, prefixo in [("inf_adic", "inf_adic_"), ("produto", "produto_")]:
                for k, v in det.get(campo, {}).items():
                    campos[f"{prefixo}{k}"] = v
            # Campos especiais
            for c in ['combustivel', 'observacao', 'rastreabilidade', 'tributavel']:
                if c in det:
                    campos[f"det_{c}"] = "" if isinstance(det[c], dict) and not det[c] else det[c]

        # Campos restantes
        if p.get("lista_parcelas", {}).get("parcela"):
            for k, v in p["lista_parcelas"]["parcela"][0].items():
                campos[f"parcela_{k}"] = v

        for campo, prefixo in [
            ("frete", "frete_"),
            ("infoCadastro", "infoCadastro_"),
            ("total_pedido", "total_")
        ]:
            for k, v in p.get(campo, {}).items():
                campos[f"{prefixo}{k}"] = v

        # Info adicionais
        for k, v in p.get("informacoes_adicionais", {}).items():
            if isinstance(v, dict):
                for sub_k, sub_v in v.items():
                    campos[f"infoAdicionais_{k}_{sub_k}"] = sub_v
            else:
                campos[f"infoAdicionais_{k}"] = v

        return campos

    def coletar_pedidos():
        """Coleta pedidos tipo N da API Omie"""
        URL = "https://app.omie.com.br/api/v1/produtos/pedido/"
        ENDPOINT_NAME = "listar_produtos_pedidos"
        inicio = datetime.now()
        ids_coletados, resultados = set(), []
        tentativas_sem_dados = 0

        with requests.Session() as session:
            session.headers.update({'Content-type': 'application/json'})
            logger.info("Iniciando coleta de pedidos tipo N")
            pagina = 1
            total_ids = 0
            paginas_com_erro = []

            while tentativas_sem_dados < 10:
                payload = {
                    "call": "ListarPedidos",
                    "app_key": API_KEY, "app_secret": API_SECRET,
                    "param": [{
                        "pagina": pagina,
                        "registros_por_pagina": 200,
                        "apenas_importado_api": "N"
                    }]
                }

                resp = fazer_requisicao_com_retry(URL, payload, session=session)
                if not resp:
                    tentativas_sem_dados += 1
                    paginas_com_erro.append(pagina)
                    logger.warning(f"Sem resposta na página {pagina}. Tentativa sem dados {tentativas_sem_dados}/10")
                    pagina += 1  # CRÍTICO: sempre incrementar a página
                    if len(paginas_com_erro) > 10:
                        break
                    continue

                try:
                    dados = resp.json()
                    pedidos = dados.get('pedido_venda_produto', [])
                    if not pedidos:
                        tentativas_sem_dados += 1
                        logger.warning(
                            f"Nenhum pedido na página {pagina}. Tentativa sem dados {tentativas_sem_dados}/10")
                        pagina += 1  # CRÍTICO: sempre incrementar a página
                        continue

                    tentativas_sem_dados = 0  # Reseta contador ao encontrar dados
                    novos_pedidos = 0
                    for p in pedidos:
                        try:
                            # Verifica duplicatas
                            id_pedido = p.get('cabecalho', {}).get('codigo_pedido')
                            if id_pedido and id_pedido in ids_coletados:
                                continue
                            if id_pedido:
                                ids_coletados.add(id_pedido)

                            # Processa e adiciona
                            pedido_processado = p.copy()
                            campos_aninhados = extrair_campos_aninhados_pedidos(p)
                            pedido_processado.update(campos_aninhados)

                            # Remove campos originais para evitar duplicação
                            for campo in ["cabecalho", "det", "lista_parcelas", "frete", "infoCadastro",
                                          "informacoes_adicionais", "total_pedido"]:
                                if campo in pedido_processado:
                                    del pedido_processado[campo]

                            resultados.append(pedido_processado)
                            novos_pedidos += 1
                        except Exception as e:
                            logger.error(f"Erro ao processar pedido: {e}")

                    total_ids += novos_pedidos
                    logger.info(f"Página {pagina}: +{novos_pedidos} pedidos. Total: {total_ids}")

                    # Verifica se chegou ao fim das páginas
                    total_paginas = dados.get('total_de_paginas', 0)
                    if total_paginas > 0 and pagina >= total_paginas:
                        logger.info(f"Fim das páginas alcançado: {pagina}/{total_paginas}")
                        break

                    # Se retornou menos registros que o solicitado, provavelmente é a última página
                    if len(pedidos) < 200:
                        logger.info(f"Última página detectada (registros: {len(pedidos)})")
                        break

                    pagina += 1

                except Exception as e:
                    logger.error(f"Erro na página {pagina}: {e}")
                    paginas_com_erro.append(pagina)
                    pagina += 1  # CRÍTICO: sempre incrementar a página

            # Recupera páginas com erro (limitando a 5 tentativas)
            if paginas_com_erro and resultados:  # Só tenta recuperar se já coletou algo
                logger.warning(f"Tentando recuperar {len(paginas_com_erro)} páginas com erro")
                for pag_erro in paginas_com_erro[:5]:  # Limita a 5 páginas para evitar loops
                    try:
                        payload = {
                            "call": "ListarPedidos",
                            "app_key": API_KEY, "app_secret": API_SECRET,
                            "param": [{
                                "pagina": pag_erro,
                                "registros_por_pagina": 200,
                                "apenas_importado_api": "N"
                            }]
                        }

                        resp = fazer_requisicao_com_retry(URL, payload, session=session, timeout=60)
                        if not resp:
                            continue

                        dados = resp.json()
                        pedidos = dados.get('pedido_venda_produto', [])
                        recuperados = 0

                        for p in pedidos:
                            id_pedido = p.get('cabecalho', {}).get('codigo_pedido')
                            if id_pedido and id_pedido in ids_coletados:
                                continue
                            if id_pedido:
                                ids_coletados.add(id_pedido)

                            pedido_processado = p.copy()
                            pedido_processado.update(extrair_campos_aninhados_pedidos(p))
                            for campo in ["cabecalho", "det", "lista_parcelas", "frete", "infoCadastro",
                                          "informacoes_adicionais", "total_pedido"]:
                                if campo in pedido_processado:
                                    del pedido_processado[campo]

                            resultados.append(pedido_processado)
                            recuperados += 1

                        logger.info(f"Recuperados +{recuperados} pedidos da página {pag_erro}")
                    except Exception as e:
                        logger.error(f"Erro ao recuperar página {pag_erro}: {e}")
                        continue  # Não quebra o processo por erro de recuperação

        if not resultados:
            logger.warning("Nenhum pedido coletado.")
            return

        df = pd.DataFrame(resultados)
        upload_to_gcs(df, f"{ENDPOINT_NAME}/pedidos_omie.csv", sep='|')
        logger.info(f"Coleta de pedidos finalizada em {datetime.now() - inicio}. Total: {len(resultados)}")

    # === FUNÇÕES DE COLETA: OUTROS ENDPOINTS ===
    def coletar_vendedores():
        """Coleta vendedores da API Omie"""
        URL = "https://app.omie.com.br/api/v1/geral/vendedores/"
        ENDPOINT_NAME = "vendedores"
        pagina, resultados = 1, []
        tentativas_sem_dados = 0
        inicio = datetime.now()

        logger.info("Iniciando coleta de vendedores...")
        while tentativas_sem_dados < 10:
            payload = {
                "call": "ListarVendedores",
                "app_key": API_KEY, "app_secret": API_SECRET,
                "param": [{"pagina": pagina, "registros_por_pagina": 100, "apenas_importado_api": "N"}]
            }

            response = fazer_requisicao_com_retry(URL, payload)
            if not response:
                tentativas_sem_dados += 1
                logger.warning(f"Sem resposta na página {pagina}. Tentativa sem dados {tentativas_sem_dados}/10")
                pagina += 1
                continue

            dados = response.json()
            vendedores = dados.get('cadastro', [])
            if not vendedores:
                tentativas_sem_dados += 1
                logger.warning(f"Nenhum vendedor na página {pagina}. Tentativa sem dados {tentativas_sem_dados}/10")
                pagina += 1
                continue

            tentativas_sem_dados = 0  # Reseta contador ao encontrar dados
            resultados.extend(vendedores)
            logger.info(f"Página {pagina}: +{len(vendedores)} vendedores. Total: {len(resultados)}")

            if pagina >= dados.get('total_de_paginas', 1):
                break
            pagina += 1

        if resultados:
            df = pd.DataFrame(resultados)
            upload_to_gcs(df, f"{ENDPOINT_NAME}/vendedores.csv")
            logger.info(f"Coleta de vendedores finalizada em {datetime.now() - inicio}")

    def coletar_produtos():
        """Coleta produtos da API Omie"""
        URL = "https://app.omie.com.br/api/v1/geral/produtos/"
        ENDPOINT_NAME = "listar_produtos"
        pagina, resultados = 1, []
        tentativas_sem_dados = 0
        inicio = datetime.now()

        logger.info("Iniciando coleta de produtos...")
        while tentativas_sem_dados < 10:
            payload = {
                "call": "ListarProdutos",
                "app_key": API_KEY, "app_secret": API_SECRET,
                "param": [{
                    "pagina": pagina,
                    "registros_por_pagina": 500,
                    "apenas_importado_api": "N",
                    "filtrar_apenas_omiepdv": "N"
                }]
            }

            response = fazer_requisicao_com_retry(URL, payload)
            if not response:
                tentativas_sem_dados += 1
                logger.warning(f"Sem resposta na página {pagina}. Tentativa sem dados {tentativas_sem_dados}/10")
                pagina += 1
                continue

            dados = response.json()
            produtos = dados.get('produto_servico_cadastro', [])
            if not produtos:
                tentativas_sem_dados += 1
                logger.warning(f"Nenhum produto na página {pagina}. Tentativa sem dados {tentativas_sem_dados}/10")
                pagina += 1
                continue

            tentativas_sem_dados = 0  # Reseta contador ao encontrar dados
            resultados.extend(produtos)
            if len(resultados) % 500 == 0:
                logger.info(f"Total coletado: {len(resultados)}")

            if pagina >= dados.get('total_de_paginas', 1):
                break
            pagina += 1

        if resultados:
            df = pd.DataFrame(resultados)
            upload_to_gcs(df, f"{ENDPOINT_NAME}/produtos.csv")
            logger.info(f"Coleta de produtos finalizada em {datetime.now() - inicio}")

    def coletar_motivos_devolucao():
        """Coleta motivos de devolução"""
        URL = "https://app.omie.com.br/api/v1/geral/motivodevolucao/"
        ENDPOINT_NAME = "listar_motivo_devolucao"
        inicio = datetime.now()
        tentativas_sem_dados = 0

        logger.info("Iniciando coleta de motivos de devolução...")
        while tentativas_sem_dados < 10:
            payload = {
                "call": "ListarMotivosDevol",
                "app_key": API_KEY, "app_secret": API_SECRET,
                "param": [{"nPagina": 1, "nRegPorPagina": 100}]
            }

            response = fazer_requisicao_com_retry(URL, payload)
            if not response:
                tentativas_sem_dados += 1
                logger.warning(f"Sem resposta. Tentativa sem dados {tentativas_sem_dados}/10")
                continue

            dados = response.json()
            motivos = dados.get('listaMotivo', [])
            if not motivos:
                tentativas_sem_dados += 1
                logger.warning(f"Nenhum motivo de devolução. Tentativa sem dados {tentativas_sem_dados}/10")
                continue

            tentativas_sem_dados = 0  # Reseta contador ao encontrar dados
            df = pd.DataFrame(motivos)
            upload_to_gcs(df, f"{ENDPOINT_NAME}/motivos_devolucao.csv")
            logger.info(f"Coleta finalizada em {datetime.now() - inicio}. Total: {len(motivos)}")
            break

    def coletar_etapas_pedido():
        """Coleta etapas de pedido"""
        URL = "https://app.omie.com.br/api/v1/produtos/pedidoetapas/"
        ENDPOINT_NAME = "listar_pedidos_etapas"
        pagina, resultados = 1, []
        tentativas_sem_dados = 0
        inicio = datetime.now()

        logger.info("Iniciando coleta de etapas de pedido...")
        while tentativas_sem_dados < 10:
            payload = {
                "call": "ListarEtapasPedido",
                "app_key": API_KEY, "app_secret": API_SECRET,
                "param": [{"nPagina": pagina, "nRegPorPagina": 500}]
            }

            response = fazer_requisicao_com_retry(URL, payload)
            if not response:
                tentativas_sem_dados += 1
                logger.warning(f"Sem resposta na página {pagina}. Tentativa sem dados {tentativas_sem_dados}/10")
                pagina += 1
                continue

            dados = response.json()
            etapas = dados.get('etapasPedido', [])
            if not etapas:
                tentativas_sem_dados += 1
                logger.warning(f"Nenhuma etapa na página {pagina}. Tentativa sem dados {tentativas_sem_dados}/10")
                pagina += 1
                continue

            tentativas_sem_dados = 0  # Reseta contador ao encontrar dados
            resultados.extend(etapas)
            if len(resultados) % 500 == 0:
                logger.info(f"Total coletado: {len(resultados)}")

            if pagina >= dados.get('nTotPaginas', 1):
                break
            pagina += 1

        if resultados:
            df = pd.DataFrame(resultados)
            upload_to_gcs(df, f"{ENDPOINT_NAME}/etapas_pedidos.csv")
            logger.info(f"Coleta finalizada em {datetime.now() - inicio}")

    def coletar_etapas_faturamento():
        """Coleta etapas de faturamento"""
        URL = "https://app.omie.com.br/api/v1/produtos/etapafat/"
        ENDPOINT_NAME = "etapafat"
        pagina, resultados = 1, []
        tentativas_sem_dados = 0
        inicio = datetime.now()

        logger.info("Iniciando coleta de etapas de faturamento...")
        while tentativas_sem_dados < 10:
            payload = {
                "call": "ListarEtapasFaturamento",
                "app_key": API_KEY, "app_secret": API_SECRET,
                "param": [{"pagina": pagina, "registros_por_pagina": 100}]
            }

            response = fazer_requisicao_com_retry(URL, payload)
            if not response:
                tentativas_sem_dados += 1
                logger.warning(f"Sem resposta na página {pagina}. Tentativa sem dados {tentativas_sem_dados}/10")
                pagina += 1
                continue

            dados = response.json()
            etapas = dados.get('cadastros', [])
            if not etapas:
                tentativas_sem_dados += 1
                logger.warning(f"Nenhuma etapa na página {pagina}. Tentativa sem dados {tentativas_sem_dados}/10")
                pagina += 1
                continue

            tentativas_sem_dados = 0  # Reseta contador ao encontrar dados
            resultados.extend(etapas)
            logger.info(f"Página {pagina}: +{len(etapas)} etapas. Total: {len(resultados)}")

            if pagina >= dados.get('total_de_paginas', 1):
                break
            pagina += 1

        if resultados:
            df = pd.DataFrame(resultados)
            upload_to_gcs(df, f"{ENDPOINT_NAME}/etapas_faturamento.csv")
            logger.info(f"Coleta finalizada em {datetime.now() - inicio}")

    def coletar_pedidos_venda(c_etapa="50"):
        """Coleta pedidos de venda por etapa"""
        URL = "https://app.omie.com.br/api/v1/produtos/pedidovendafat/"
        ENDPOINT_NAME = "pedidos_vendas"
        inicio = datetime.now()

        logger.info(f"Iniciando coleta de pedidos de venda (etapa {c_etapa})...")
        payload = {
            "call": "ObterPedidosVenda",
            "app_key": API_KEY, "app_secret": API_SECRET,
            "param": [{"cEtapa": c_etapa}]
        }

        response = fazer_requisicao_com_retry(URL, payload, timeout=60)
        if not response:
            logger.error(f"Falha ao obter pedidos de venda (etapa {c_etapa}).")
            return

        dados = response.json()
        pedidos = dados.get('listaPedidosVenda', [])

        if pedidos:
            df = pd.DataFrame(pedidos)
            upload_to_gcs(df, f"{ENDPOINT_NAME}/pedidos_venda_etapa_{c_etapa}.csv")
            logger.info(f"Coleta finalizada em {datetime.now() - inicio}. Total: {len(pedidos)}")

    def coletar_resumo_vendas(data_inicio="01/01/2024", data_fim="22/09/2025"):
        """Coleta resumo de vendas da API Omie"""
        URL = "https://app.omie.com.br/api/v1/produtos/vendas-resumo/"
        ENDPOINT_NAME = "vendas_resumo"
        inicio = datetime.now()

        logger.info(f"Iniciando coleta de resumo de vendas ({data_inicio} a {data_fim})...")

        payload = {
            "call": "ObterResumoProdutos",
            "app_key": API_KEY,
            "app_secret": API_SECRET,
            "param": [{
                "dDataInicio": data_inicio,
                "dDataFim": data_fim,
                "lApenasResumo": True
            }]
        }

        response = fazer_requisicao_com_retry(URL, payload, timeout=60)
        if not response:
            logger.error("Falha ao obter resumo de vendas.")
            return

        dados = response.json()

        # Processar dados aninhados para estrutura tabular
        resultados = []

        # Dados principais do período
        resultado_base = {
            "data_inicio": dados.get("dDataInicio", ""),
            "data_fim": dados.get("dDataFim", ""),
            "data_coleta": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        # Processar pedidoVenda se existir
        if dados.get("pedidoVenda"):
            pedido_venda = dados["pedidoVenda"].copy()
            pedido_venda.update(resultado_base)
            pedido_venda["tipo_resumo"] = "pedido_venda"

            # Processar faturarHoje se existir
            if pedido_venda.get("faturarHoje"):
                for k, v in pedido_venda["faturarHoje"].items():
                    pedido_venda[f"faturar_hoje_{k}"] = v
                del pedido_venda["faturarHoje"]

            resultados.append(pedido_venda)

        # Processar faturamentoResumo se existir
        if dados.get("faturamentoResumo"):
            faturamento_resumo = dados["faturamentoResumo"].copy()
            faturamento_resumo.update(resultado_base)
            faturamento_resumo["tipo_resumo"] = "faturamento_resumo"
            resultados.append(faturamento_resumo)

        # Processar outros painéis se existirem (mesmo que sejam null, registrar)
        paineis = [
            "painelNfeVenda", "painelCteVenda", "painelCfeSat",
            "painelNfce", "painelCupom", "faturamentoCupomResumo", "propostaVenda"
        ]

        for painel in paineis:
            if painel in dados:
                painel_data = dados[painel] if dados[painel] else {}
                if isinstance(painel_data, dict):
                    painel_data.update(resultado_base)
                    painel_data["tipo_resumo"] = painel
                    resultados.append(painel_data)
                else:
                    # Se for null ou outro tipo, criar registro básico
                    registro_painel = resultado_base.copy()
                    registro_painel["tipo_resumo"] = painel
                    registro_painel["status"] = "null" if dados[painel] is None else str(dados[painel])
                    resultados.append(registro_painel)

        if resultados:
            df = pd.DataFrame(resultados)

            # Definir ordem das colunas principais
            colunas_principais = ["data_inicio", "data_fim", "data_coleta", "tipo_resumo"]
            colunas_existentes = [col for col in colunas_principais if col in df.columns]
            outras_colunas = [col for col in df.columns if col not in colunas_principais]
            colunas_ordenadas = colunas_existentes + sorted(outras_colunas)

            df = df[colunas_ordenadas]
            upload_to_gcs(df, f"{ENDPOINT_NAME}/resumo_vendas.csv")
            logger.info(
                f"Coleta de resumo de vendas finalizada em {datetime.now() - inicio}. Total de registros: {len(resultados)}")
        else:
            logger.warning("Nenhum dado de resumo de vendas encontrado.")

    # === FUNÇÃO EXECUTOR E MAIN ===
    def executar_todas_coletas():
        """Executa todas as coletas sequencialmente"""
        inicio_geral = datetime.now()
        logger.info("Iniciando execução automática de todas as coletas")
        print("\n===== INICIANDO COLETAS OMIE =====")

        coletas = [
            {"nome": "Categorias", "func": coletar_categorias},
            {"nome": "Pedidos (tipo N)", "func": coletar_pedidos},
            {"nome": "Vendedores", "func": coletar_vendedores},
            {"nome": "Produtos", "func": coletar_produtos},
            {"nome": "Motivos de Devolução", "func": coletar_motivos_devolucao},
            {"nome": "Etapas de Pedido", "func": coletar_etapas_pedido},
            {"nome": "Etapas de Faturamento", "func": coletar_etapas_faturamento},
            {"nome": "Pedidos de Venda (etapa 50)", "func": lambda: coletar_pedidos_venda("50")},
            {"nome": "Resumo de Vendas", "func": coletar_resumo_vendas}
        ]

        for i, coleta in enumerate(coletas, 1):
            try:
                print(f"\n[{i}/{len(coletas)}] Iniciando coleta de {coleta['nome']}...")
                coleta["func"]()
                print(f"✓ Coleta de {coleta['nome']} finalizada!")
            except Exception as e:
                print(f"✗ ERRO na coleta de {coleta['nome']}: {e}")
                logger.error(f"Erro na coleta {i} ({coleta['nome']}): {e}")

        tempo_total = datetime.now() - inicio_geral
        logger.info(f"Todas as coletas finalizadas em {tempo_total}")
        print(f"\n===== COLETAS FINALIZADAS =====")
        print(f"Tempo total: {tempo_total}")

    """Função principal de entrada"""
    try:
        executar_todas_coletas()
    except KeyboardInterrupt:
        print("\nOperação interrompida pelo usuário.")
        logger.warning("Execução interrompida pelo usuário")
    except Exception as e:
        logger.critical(f"Erro fatal: {e}")
        raise


def get_extraction_tasks():
    """
    Get the list of data extraction tasks for Moskit.

    Returns:
        list: List of task configurations
    """
    return [
        {
            'task_id': 'run',
            'python_callable': run
        }
    ]
