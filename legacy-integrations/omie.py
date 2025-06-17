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
        inicio = datetime.now()

        logger.info("Iniciando coleta de categorias...")
        while True:
            payload = {
                "call": "ListarCategorias",
                "app_key": API_KEY, "app_secret": API_SECRET,
                "param": [{"pagina": pagina, "registros_por_pagina": 500}]
            }

            response = fazer_requisicao_com_retry(URL, payload)
            if not response:
                pagina += 1
                continue

            dados = response.json()
            categorias = dados.get('categoria_cadastro', [])
            if not categorias: break

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

        with requests.Session() as session:
            session.headers.update({'Content-type': 'application/json'})
            logger.info("Iniciando coleta de pedidos tipo N")
            pagina, total_ids, paginas_com_erro = 1, 0, []

            while True:
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
                    paginas_com_erro.append(pagina)
                    pagina += 1
                    if len(paginas_com_erro) > 10: break
                    continue

                try:
                    dados = resp.json()
                    pedidos = dados.get('pedido_venda_produto', [])
                    if not pedidos: break

                    novos_pedidos = 0
                    for p in pedidos:
                        try:
                            # Verifica duplicatas
                            id_pedido = p.get('cabecalho', {}).get('codigo_pedido')
                            if id_pedido and id_pedido in ids_coletados: continue
                            if id_pedido: ids_coletados.add(id_pedido)

                            # Processa e adiciona
                            pedido_processado = p.copy()
                            campos_aninhados = extrair_campos_aninhados_pedidos(p)
                            pedido_processado.update(campos_aninhados)

                            # Remove campos originais para evitar duplicação
                            for campo in ["cabecalho", "det", "lista_parcelas", "frete", "infoCadastro",
                                          "informacoes_adicionais", "total_pedido"]:
                                if campo in pedido_processado: del pedido_processado[campo]

                            resultados.append(pedido_processado)
                            novos_pedidos += 1
                        except Exception as e:
                            logger.error(f"Erro ao processar pedido: {e}")

                    total_ids += novos_pedidos
                    logger.info(f"Página {pagina}: +{novos_pedidos} pedidos. Total: {total_ids}")

                    if len(pedidos) < 200: break
                    pagina += 1

                except Exception as e:
                    logger.error(f"Erro na página {pagina}: {e}")
                    paginas_com_erro.append(pagina)
                    pagina += 1

            # Recupera páginas com erro
            if paginas_com_erro:
                logger.warning(f"Tentando recuperar {len(paginas_com_erro)} páginas com erro")
                for pag_erro in paginas_com_erro[:5]:
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
                        if not resp: continue

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
                        raise

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
        inicio = datetime.now()

        logger.info("Iniciando coleta de vendedores...")
        while True:
            payload = {
                "call": "ListarVendedores",
                "app_key": API_KEY, "app_secret": API_SECRET,
                "param": [{"pagina": pagina, "registros_por_pagina": 100, "apenas_importado_api": "N"}]
            }

            response = fazer_requisicao_com_retry(URL, payload)
            if not response:
                pagina += 1
                continue

            dados = response.json()
            vendedores = dados.get('cadastro', [])
            if not vendedores:
                break

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
        inicio = datetime.now()

        logger.info("Iniciando coleta de produtos...")
        while True:
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
                pagina += 1
                continue

            dados = response.json()
            produtos = dados.get('produto_servico_cadastro', [])
            if not produtos:
                break

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

        logger.info("Iniciando coleta de motivos de devolução...")
        payload = {
            "call": "ListarMotivosDevol",
            "app_key": API_KEY, "app_secret": API_SECRET,
            "param": [{"nPagina": 1, "nRegPorPagina": 100}]
        }

        response = fazer_requisicao_com_retry(URL, payload)
        if not response:
            logger.error("Falha ao coletar motivos de devolução.")
            return

        dados = response.json()
        motivos = dados.get('listaMotivo', [])

        if motivos:
            df = pd.DataFrame(motivos)
            upload_to_gcs(df, f"{ENDPOINT_NAME}/motivos_devolucao.csv")
            logger.info(f"Coleta finalizada em {datetime.now() - inicio}. Total: {len(motivos)}")

    def coletar_etapas_pedido():
        """Coleta etapas de pedido"""
        URL = "https://app.omie.com.br/api/v1/produtos/pedidoetapas/"
        ENDPOINT_NAME = "listar_pedidos_etapas"
        pagina, resultados = 1, []
        inicio = datetime.now()

        logger.info("Iniciando coleta de etapas de pedido...")
        while True:
            payload = {
                "call": "ListarEtapasPedido",
                "app_key": API_KEY, "app_secret": API_SECRET,
                "param": [{"nPagina": pagina, "nRegPorPagina": 500}]
            }

            response = fazer_requisicao_com_retry(URL, payload)
            if not response:
                pagina += 1
                continue

            dados = response.json()
            etapas = dados.get('etapasPedido', [])
            if not etapas:
                break

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
        inicio = datetime.now()

        logger.info("Iniciando coleta de etapas de faturamento...")
        while True:
            payload = {
                "call": "ListarEtapasFaturamento",
                "app_key": API_KEY, "app_secret": API_SECRET,
                "param": [{"pagina": pagina, "registros_por_pagina": 100}]
            }

            response = fazer_requisicao_com_retry(URL, payload)
            if not response:
                pagina += 1
                continue

            dados = response.json()
            etapas = dados.get('cadastros', [])
            if not etapas:
                break

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
            # {"nome": "Etapas de Pedido", "func": coletar_etapas_pedido},
            {"nome": "Etapas de Faturamento", "func": coletar_etapas_faturamento},
            {"nome": "Pedidos de Venda (etapa 50)", "func": lambda: coletar_pedidos_venda("50")}
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
