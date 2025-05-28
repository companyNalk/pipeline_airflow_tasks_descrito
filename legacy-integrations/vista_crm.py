"""
VistaCRM module for data extraction functions.
This module contains functions specific to the VistaCRM integration.
"""
import os

from core import gcs


def run(customer):
    import json
    import logging
    import random
    import re
    import time
    import unicodedata
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from datetime import datetime
    from typing import Dict, List, Tuple
    import pathlib

    import pandas as pd
    import requests
    from google.cloud import storage

    # Variáveis globais
    TOKEN = customer["token"]
    EMPRESA = customer["empresa"]
    BUCKET_NAME = customer["bucket_name"]
    FOLDERS = {
        "pipelines": "listar_pipelines",
        "etapas": "etapas_pipelines",
        "negocios": "negocios",
        "campos_clientes": "campos_clientes",
        "clientes": "clientes",
        "campos_imoveis": "campos_imoveis",
        "imoveis": "imoveis"
    }
    FILES = {
        "pipelines": "listar_pipelines.csv",
        "etapas": "etapas_pipelines.csv",
        "negocios": "negocios.csv",
        "campos_clientes": "campos_clientes.csv",
        "clientes": "clientes.csv",
        "campos_imoveis": "campos_imoveis.csv",
        "imoveis": "imoveis.csv"
    }
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH
    MAX_RETRIES, MAX_WORKERS, RECORDS_PER_PAGE = 5, 5, 50

    # Configuração de logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(), logging.FileHandler('vista_host_collector.log')]
    )
    logger = logging.getLogger('vista_host_collector')

    class VistaHostCollector:
        """Classe principal para coleta de dados da API VistaHost"""

        def __init__(self):
            self.session = requests.Session()
            self.session.headers.update({'Accept': 'application/json', 'Content-Type': 'application/json'})
            self.total_collected = {
                'pipelines': 0, 'etapas': 0, 'negocios': 0,
                'campos_clientes': 0, 'clientes': 0, 'campos_imoveis': 0, 'imoveis': 0
            }
            self.total_failed = {
                'pipelines': 0, 'etapas': 0, 'negocios': 0,
                'campos_clientes': 0, 'clientes': 0, 'campos_imoveis': 0, 'imoveis': 0
            }
            self.start_time = time.time()
            self.failed_requests = {
                'pipelines': [], 'etapas': [], 'negocios': [],
                'campos_clientes': [], 'clientes': [], 'campos_imoveis': [], 'imoveis': []
            }
            self.processed_urls = set()  # Para evitar duplicação de logs
            self.init_gcs_client()

        def init_gcs_client(self):
            """Inicializa cliente do Google Cloud Storage"""
            try:
                storage_client = storage.Client(project=customer['project_id'])
                self.bucket = storage_client.bucket(BUCKET_NAME)
                logger.info(f"Cliente GCS inicializado com sucesso para o bucket: {BUCKET_NAME}")
            except Exception as e:
                logger.error(f"Erro ao inicializar cliente GCS: {str(e)}")
                raise RuntimeError(f"Erro ao inicializar cliente GCS: {str(e)}")

        def normalize_column_name(self, name: str) -> str:
            """Normaliza nomes de colunas: remove acentos, converte para lowercase e substitui espaços por underlines"""
            normalized = unicodedata.normalize('NFKD', str(name)).encode('ASCII', 'ignore').decode('utf-8').lower()
            normalized = re.sub(r'[^a-z0-9]', '_', normalized)
            return re.sub(r'_+', '_', normalized).strip('_')

        def flatten_json(self, json_obj: Dict, prefix: str = '') -> Dict:
            """Transforma JSON aninhado em formato plano"""
            flattened = {}
            for key, value in json_obj.items():
                new_key = f"{prefix}_{key}" if prefix else key
                if isinstance(value, dict):
                    flattened.update(self.flatten_json(value, new_key))
                elif isinstance(value, list):
                    for i, item in enumerate(value):
                        if isinstance(item, dict):
                            flattened.update(self.flatten_json(item, f"{new_key}_{i}"))
                        else:
                            flattened[f"{new_key}_{i}"] = item
                else:
                    flattened[new_key] = value
            return flattened

        def make_request(self, url: str, resource_type: str, retries: int = 0) -> Dict:
            """Faz uma requisição à API com estratégia de backoff exponencial"""
            try:
                response = self.session.get(url, timeout=30)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429 or response.status_code >= 500:
                    if retries < MAX_RETRIES:
                        wait_time = (2 ** retries) + random.uniform(0, 1)
                        logger.info(f"Limitação da API para {resource_type}. Aguardando {wait_time:.2f}s")
                        time.sleep(wait_time)
                        return self.make_request(url, resource_type, retries + 1)
                    else:
                        logger.error(f"Falha após {MAX_RETRIES} tentativas: {resource_type}")
                        self.total_failed[resource_type] += 1
                        return {"error": f"Falha após {MAX_RETRIES} tentativas", "url": url}
                else:
                    logger.error(f"Erro na requisição para {resource_type}. Status: {response.status_code}")
                    if retries < MAX_RETRIES:
                        time.sleep((2 ** retries) + random.uniform(0, 1))
                        return self.make_request(url, resource_type, retries + 1)
                    self.total_failed[resource_type] += 1
                    return {"error": f"Erro {response.status_code}", "url": url}
            except Exception as e:
                logger.error(f"Exceção ao fazer requisição para {resource_type}: {str(e)}")
                if retries < MAX_RETRIES:
                    time.sleep((2 ** retries) + random.uniform(0, 1))
                    return self.make_request(url, resource_type, retries + 1)
                self.total_failed[resource_type] += 1
                return {"error": str(e), "url": url}

        def extract_data_from_response(self, response_data: Dict, resource_type: str) -> List[Dict]:
            """Extrai dados da resposta da API baseado no tipo de recurso"""
            if "error" in response_data:
                return []

            if resource_type in ['pipelines', 'negocios', 'clientes', 'imoveis']:
                numeric_keys = [k for k in response_data.keys() if k.isdigit()]
                return [response_data[k] for k in numeric_keys]
            elif resource_type in ['etapas', 'campos_clientes', 'campos_imoveis']:
                if any(field in response_data for field in ['CodigoPipe', 'NomePipe', 'NomeEtapa']):
                    return [response_data]

                numeric_keys = [k for k in response_data.keys() if k.isdigit()]
                data = [response_data[k] for k in numeric_keys] if numeric_keys else []
                if not data and 'dados' in response_data:
                    return response_data['dados']
                return data
            return []

        def get_page_data(self, base_url: str, page: int, resource_type: str, pipe_code: str = None) -> Tuple[
            List[Dict], int, bool]:
            """Obtém os dados de uma página específica - Retorna: (dados, próxima_página, tem_erro)"""
            try:
                url = base_url
                # Montar URL para paginação
                if 'paginacao' in base_url:
                    start_idx = base_url.find('pesquisa=') + 9
                    pesquisa_obj = json.loads(base_url[start_idx:])
                    if 'paginacao' in pesquisa_obj:
                        pesquisa_obj['paginacao']['pagina'] = page
                    url = base_url[:start_idx] + json.dumps(pesquisa_obj)

                # Criar um ID único para a requisição
                req_id = f"{resource_type}_{pipe_code}_{page}" if pipe_code else f"{resource_type}_{page}"

                # Verifica se já processamos esta requisição antes
                if req_id in self.processed_urls:
                    logger.warning(f"Requisição duplicada evitada: {req_id}")
                    return [], 0, True

                self.processed_urls.add(req_id)

                # Fazer a requisição
                response_data = self.make_request(url, resource_type)

                if "error" in response_data:
                    self.failed_requests[resource_type].append(url)
                    return [], 0, True

                data = self.extract_data_from_response(response_data, resource_type)

                # Verifica se há mais páginas
                next_page = 0
                if resource_type in ['pipelines', 'negocios', 'clientes', 'imoveis']:
                    next_page = page + 1 if len(data) >= RECORDS_PER_PAGE else 0
                    pipe_info = f"Pipe {pipe_code} | " if pipe_code else ""
                    print(
                        f"{resource_type} | {pipe_info}Pág {page} | +{len(data)} | T:{self.total_collected[resource_type] + len(data)} | F:{self.total_failed[resource_type]}")
                elif resource_type in ['etapas', 'campos_clientes', 'campos_imoveis']:
                    pipe_info = f"Pipe {pipe_code} | " if pipe_code else ""
                    print(
                        f"{resource_type} | {pipe_info}+{len(data)} | T:{self.total_collected[resource_type] + len(data)} | F:{self.total_failed[resource_type]}")

                return data, next_page, False
            except Exception as e:
                logger.error(f"Erro ao processar página {page} de {resource_type}: {str(e)}")
                self.failed_requests[resource_type].append(base_url)
                self.total_failed[resource_type] += 1
                return [], 0, True

        def collect_resource_with_pagination(self, base_url: str, resource_type: str,
                                             pipe_code: str = None) -> pd.DataFrame:
            """Coleta todos os dados de um recurso com paginação"""
            first_page_data, next_page, has_error = self.get_page_data(base_url, 1, resource_type, pipe_code)
            all_data = first_page_data
            self.total_collected[resource_type] += len(first_page_data)

            # Coleta páginas adicionais enquanto houver dados
            current_page = 1
            while next_page > 0:
                current_page = next_page
                page_data, next_page, has_error = self.get_page_data(base_url, current_page, resource_type, pipe_code)
                if has_error or not page_data:
                    break
                all_data.extend(page_data)
                self.total_collected[resource_type] += len(page_data)

            # Reprocessa falhas, se houver
            if self.failed_requests[resource_type]:
                self.reprocess_failed_requests(resource_type, all_data)

            # Converte para DataFrame e normaliza
            if all_data:
                df = pd.DataFrame([self.flatten_json(item) for item in all_data])
                df.columns = [self.normalize_column_name(col) for col in df.columns]
                return df

            return pd.DataFrame()

        def reprocess_failed_requests(self, resource_type: str, all_data: List[Dict]):
            """Reprocessa requisições que falharam"""
            if not self.failed_requests[resource_type]:
                return

            failed_count = len(self.failed_requests[resource_type])
            print(f"REPROCESSANDO | {resource_type} | {failed_count} pendentes")
            failed_urls = self.failed_requests[resource_type].copy()
            self.failed_requests[resource_type] = []

            with ThreadPoolExecutor(max_workers=max(1, MAX_WORKERS // 2)) as executor:
                futures = {executor.submit(self.make_request, url, resource_type): url for url in failed_urls}
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if "error" not in result:
                            data = self.extract_data_from_response(result, resource_type)
                            all_data.extend(data)
                            self.total_collected[resource_type] += len(data)
                            self.total_failed[resource_type] -= 1
                    except Exception as e:
                        logger.error(f"Exceção no reprocessamento para {resource_type}: {str(e)}")

        def save_to_gcs(self, df: pd.DataFrame, resource_type: str):
            """Salva DataFrame como CSV no Google Cloud Storage"""
            if df.empty:
                logger.warning(f"DataFrame vazio, não será salvo: {resource_type}")
                return

            try:
                folder = FOLDERS[resource_type]
                filename = FILES[resource_type]
                blob_path = f"{folder}/{filename}"
                blob = self.bucket.blob(blob_path)
                blob.upload_from_string(df.to_csv(sep=';', index=False), content_type='text/csv')
                print(f"SALVO | {resource_type} | {len(df)} registros | gs://{BUCKET_NAME}/{blob_path}")
            except Exception as e:
                logger.error(f"Erro ao salvar arquivo {resource_type}: {str(e)}")
                print(f"ERRO | {resource_type} | falha ao salvar")
                raise

        def get_api_url(self, resource_type: str, codigo_pipe: str = "1") -> str:
            """Retorna a URL da API baseado no tipo de recurso"""
            base_url = "http://bridgeco-rest.vistahost.com.br"

            if resource_type == "pipelines":
                pesquisa = json.dumps({
                    "fields": ["Codigo", "Nome", "Empresa"],
                    "paginacao": {"pagina": 1, "quantidade": RECORDS_PER_PAGE}
                })
                return f"{base_url}/pipes/listar?key={TOKEN}&empresa={EMPRESA}&pesquisa={pesquisa}"

            elif resource_type == "etapas":
                pesquisa = json.dumps({
                    "fields": ["CodigoPipe", "NomePipe", "NomeEtapa", "Ordem", "DiasPrevisto", "UnidadePrevisto"],
                    "order": {"Ordem": "asc"}
                })
                return f"{base_url}/pipes/etapas?key={TOKEN}&empresa={EMPRESA}&codigo_pipe={codigo_pipe}&pesquisa={pesquisa}"

            elif resource_type == "negocios":
                pesquisa = json.dumps({
                    "fields": [
                        "Codigo", "NomeNegocio", "Status", "DataInicial", "DataFinal",
                        "ValorNegocio", "PrevisaoFechamento", "VeiculoCaptacao",
                        "CodigoMotivoPerda", "MotivoPerda", "ObservacaoPerda",
                        "CodigoPipe", "EtapaAtual", "NomeEtapa", "CodigoCliente",
                        "CodigoImovel", "FotoCliente", "NomeCliente", "EmailCliente",
                        "TelefoneCliente", "CelularCliente"
                    ],
                    "paginacao": {"pagina": 1, "quantidade": RECORDS_PER_PAGE}
                })
                return f"{base_url}/negocios/listar?key={TOKEN}&empresa={EMPRESA}&codigo_pipe={codigo_pipe}&pesquisa={pesquisa}"

            # Novos endpoints
            elif resource_type == "campos_clientes":
                pesquisa = json.dumps({
                    "paginacao": {"pagina": 1, "quantidade": RECORDS_PER_PAGE}
                })
                return f"{base_url}/clientes/listarcampos?key={TOKEN}&empresa={EMPRESA}&pesquisa={pesquisa}"

            elif resource_type == "clientes":
                pesquisa = json.dumps({
                    "paginacao": {"pagina": 1, "quantidade": RECORDS_PER_PAGE},
                    "fields": [
                        "DataCadastro", "Codigo", "TipoPessoa", "Nome", "Proprietario",
                        "Cliente", "Status", "VeiculoCaptacao", "FerramentaCaptacao",
                        "CampanhaImportacao", "DataImportacao", "Corretor",
                        {"historicos": ["Data", "Codigoimovel", "Referenciaimovel",
                                        "VeiculodeCaptacao", "Imovel", "DataRetorno",
                                        "Aceitacao", "Lead", "ValorProposta", "Statusvisita"]},
                        {"CorretorCliente": ["Nome"]}
                    ]
                })
                return f"{base_url}/clientes/listar?key={TOKEN}&empresa={EMPRESA}&pesquisa={pesquisa}"

            elif resource_type == "campos_imoveis":
                pesquisa = json.dumps({
                    "paginacao": {"pagina": 1, "quantidade": RECORDS_PER_PAGE}
                })
                return f"{base_url}/imoveis/listarcampos?key={TOKEN}&empresa={EMPRESA}&pesquisa={pesquisa}"

            elif resource_type == "imoveis":
                pesquisa = json.dumps({
                    "paginacao": {"pagina": 1, "quantidade": RECORDS_PER_PAGE},
                    "fields": [
                        "DataCadastro", "DataAtualizacao", "CodigoEmpreendimento",
                        "Matricula", "Title", "Construtora", "Incorporadora",
                        "Empreendimento", "NomeCondominio", "Proprietario",
                        "AdministradoraCondominio", "Regiao", "UF", "TipoImovel",
                        "CategoriaImovel", "Finalidade", "ValorLocacao", "ValorVenda",
                        "Agenciador", "CorretorNome", "Situacao", "Status"
                    ]
                })
                return f"{base_url}/imoveis/listar?key={TOKEN}&empresa={EMPRESA}&pesquisa={pesquisa}"

            return ""

        def collect_data(self, resource_type: str, codigo_pipe: str = "1") -> pd.DataFrame:
            """Coleta dados do tipo especificado"""
            print(f"INICIANDO | {resource_type.upper()}")
            url = self.get_api_url(resource_type, codigo_pipe)
            return self.collect_resource_with_pagination(url, resource_type, codigo_pipe)

        def collect_etapas_for_pipeline(self, codigo_pipe: str) -> List[Dict]:
            """Coleta etapas para um pipeline específico"""
            url = self.get_api_url("etapas", str(codigo_pipe))
            etapas_data, _, has_error = self.get_page_data(url, 1, 'etapas', codigo_pipe)
            return etapas_data if not has_error else []

        def collect_etapas(self, pipelines_df: pd.DataFrame) -> pd.DataFrame:
            """Coleta etapas de todos os pipelines em paralelo"""
            print("INICIANDO | ETAPAS DE PIPELINE")
            all_etapas = []

            if pipelines_df.empty:
                etapas_data = self.collect_etapas_for_pipeline("1")
                if etapas_data:
                    all_etapas.extend(etapas_data)
                    self.total_collected['etapas'] += len(etapas_data)
            else:
                # Identifica a coluna de código
                codigo_col = next((col for col in pipelines_df.columns if 'codigo' in col), None)
                if not codigo_col:
                    logger.error("Coluna 'codigo' não encontrada no DataFrame de pipelines")
                    return pd.DataFrame()

                # Coleta etapas para cada pipeline em paralelo
                pipeline_codes = pipelines_df[codigo_col].astype(str).tolist()
                print(f"etapas | {len(pipeline_codes)} pipelines")

                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = {executor.submit(self.collect_etapas_for_pipeline, code): code for code in pipeline_codes}
                    for future in as_completed(futures):
                        try:
                            etapas_data = future.result()
                            all_etapas.extend(etapas_data)
                            self.total_collected['etapas'] += len(etapas_data)
                        except Exception as e:
                            logger.error(f"Erro ao coletar etapas: {str(e)}")
                            self.total_failed['etapas'] += 1

            # Reprocessa falhas e converte para DataFrame
            if self.failed_requests['etapas']:
                self.reprocess_failed_requests('etapas', all_etapas)

            if all_etapas:
                df = pd.DataFrame([self.flatten_json(item) for item in all_etapas])
                df.columns = [self.normalize_column_name(col) for col in df.columns]
                print(f"CONCLUÍDO | etapas | Total {len(df)} | Falhas {self.total_failed['etapas']}")
                return df

            return pd.DataFrame()

        def collect_negocios_for_pipelines(self, pipelines_df: pd.DataFrame) -> pd.DataFrame:
            """Coleta negócios para todos os pipelines em paralelo"""
            codigo_col = next((col for col in pipelines_df.columns if 'codigo' in col), None)

            if not codigo_col:
                return self.collect_data("negocios", "1")

            # Cria um DataFrame para cada pipeline e depois concatena
            pipeline_codes = pipelines_df[codigo_col].astype(str).tolist()
            print(f"negocios | {len(pipeline_codes)} pipelines")

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(self.collect_data, "negocios", code): code for code in pipeline_codes}
                pipeline_dfs = []

                for future in as_completed(futures):
                    try:
                        df = future.result()
                        if not df.empty:
                            pipeline_dfs.append(df)
                    except Exception as e:
                        logger.error(f"Erro ao coletar negócios: {str(e)}")
                        self.total_failed['negocios'] += 1

            if pipeline_dfs:
                result_df = pd.concat(pipeline_dfs, ignore_index=True)
                print(f"CONCLUÍDO | negocios | Total {len(result_df)} | Falhas {self.total_failed['negocios']}")
                return result_df
            else:
                print(f"CONCLUÍDO | negocios | Total 0 | Falhas {self.total_failed['negocios']}")
                return pd.DataFrame()

        def run(self):
            """Executa a coleta completa de dados"""
            try:
                print(f"INICIANDO | VistaHost | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

                # Coleta pipelines
                pipelines_df = self.collect_data("pipelines")
                self.save_to_gcs(pipelines_df, "pipelines")

                # Coleta etapas
                etapas_df = self.collect_etapas(pipelines_df)
                self.save_to_gcs(etapas_df, "etapas")

                # Coleta negócios para todos os pipelines em paralelo
                if not pipelines_df.empty:
                    negocios_df = self.collect_negocios_for_pipelines(pipelines_df)
                else:
                    negocios_df = self.collect_data("negocios")
                self.save_to_gcs(negocios_df, "negocios")

                # Novos endpoints
                # Coleta campos de clientes
                campos_clientes_df = self.collect_data("campos_clientes")
                self.save_to_gcs(campos_clientes_df, "campos_clientes")

                # Coleta clientes
                clientes_df = self.collect_data("clientes")
                self.save_to_gcs(clientes_df, "clientes")

                # Coleta campos de imóveis
                campos_imoveis_df = self.collect_data("campos_imoveis")
                self.save_to_gcs(campos_imoveis_df, "campos_imoveis")

                # Coleta imóveis
                imoveis_df = self.collect_data("imoveis")
                self.save_to_gcs(imoveis_df, "imoveis")

            except Exception as e:
                logger.error(f"Erro durante a execução: {str(e)}")
                print(f"ERRO | Execução geral | {str(e)}")
                raise RuntimeError(f"ERRO | Execução geral | {str(e)}")
            finally:
                # Exibe resumo
                self.print_summary()

        def print_summary(self):
            """Exibe resumo da execução"""
            execution_time = time.time() - self.start_time
            hours, remainder = divmod(execution_time, 3600)
            minutes, seconds = divmod(remainder, 60)

            print(f"RESUMO | {int(hours)}h{int(minutes)}m{int(seconds)}s | "
                  f"P:{self.total_collected['pipelines']} "
                  f"E:{self.total_collected['etapas']} "
                  f"N:{self.total_collected['negocios']} "
                  f"CC:{self.total_collected['campos_clientes']} "
                  f"C:{self.total_collected['clientes']} "
                  f"CI:{self.total_collected['campos_imoveis']} "
                  f"I:{self.total_collected['imoveis']} | "
                  f"Falhas:{sum(self.total_failed.values())}")

    VistaHostCollector().run()


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
