"""
VistaCRM module for data extraction functions.
This module contains functions specific to the VistaCRM integration.
"""

from core import gcs


def extract_customers(customer):
    import json
    import logging
    import pandas as pd
    import random
    import re
    import requests
    import time
    import unicodedata
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from datetime import datetime
    from google.cloud import storage
    from typing import Dict, List, Tuple
    import os
    import pathlib

    # Variáveis globais
    BASE_URL = customer['api_base_url']
    TOKEN = customer['api_token']
    EMPRESA = customer['empresa']
    BUCKET_NAME = customer['bucket_name']
    PAYLOAD_ENDPOINT_CLIENTES = dict(customer['payload_endpoint_clientes'])
    FOLDER = "tabela_clientes"
    FILENAME = "clientes.csv"
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    MAX_RETRIES, MAX_WORKERS, RECORDS_PER_PAGE = 5, 5, 50

    # Configuração de logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(), logging.FileHandler('clientes_collector.log')]
    )
    logger = logging.getLogger('clientes_collector')

    class ClientesCollector:
        """Classe para coleta de dados da Tabela de Clientes da API VistaHost"""

        def __init__(self):
            self.session = requests.Session()
            self.session.headers.update({'Accept': 'application/json', 'Content-Type': 'application/json'})
            self.total_collected = 0
            self.total_failed = 0
            self.total_pages = 0
            self.start_time = time.time()
            self.failed_requests = []
            self.processed_urls = set()
            self.cliente_types = {}
            self.init_gcs_client()

        def init_gcs_client(self):
            """Inicializa cliente do Google Cloud Storage"""
            try:
                self.storage_client = storage.Client(project=customer['project_id'])
                self.bucket = self.storage_client.bucket(BUCKET_NAME)
                logger.info(f"Cliente GCS inicializado com sucesso para o bucket: {BUCKET_NAME}")
            except Exception as e:
                logger.error(f"Erro ao inicializar cliente GCS: {str(e)}")
                raise

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

        def make_request(self, url: str, retries: int = 0) -> Dict:
            """Faz uma requisição à API com estratégia de backoff exponencial"""
            try:
                response = self.session.get(url, timeout=30)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429 or response.status_code >= 500:
                    if retries < MAX_RETRIES:
                        wait_time = (2 ** retries) + random.uniform(0, 1)
                        logger.info(f"Limitação da API. Aguardando {wait_time:.2f}s")
                        time.sleep(wait_time)
                        return self.make_request(url, retries + 1)
                    else:
                        logger.error(f"Falha após {MAX_RETRIES} tentativas")
                        self.total_failed += 1
                        return {"error": f"Falha após {MAX_RETRIES} tentativas", "url": url}
                else:
                    logger.error(f"Erro na requisição. Status: {response.status_code}")
                    if response.status_code == 401:
                        print('ERRO NA AUTENTICACAO/MONTAGEM DA URL DINAMICA')
                        raise
                    if retries < MAX_RETRIES:
                        time.sleep((2 ** retries) + random.uniform(0, 1))
                        return self.make_request(url, retries + 1)
                    self.total_failed += 1
                    return {"error": f"Erro {response.status_code}", "url": url}
            except Exception as e:
                logger.error(f"Exceção ao fazer requisição: {str(e)}")
                if retries < MAX_RETRIES:
                    time.sleep((2 ** retries) + random.uniform(0, 1))
                    return self.make_request(url, retries + 1)
                self.total_failed += 1
                return {"error": str(e), "url": url}

        def extract_data_from_response(self, response_data: Dict) -> List[Dict]:
            """Extrai dados da resposta da API"""
            if "error" in response_data:
                return []

            # Para o endpoint de clientes, os dados estão em chaves numéricas
            numeric_keys = [k for k in response_data.keys() if k.isdigit()]
            return [response_data[k] for k in numeric_keys]

        def get_page_data(self, base_url: str, page: int) -> Tuple[List[Dict], int, bool]:
            """Obtém os dados de uma página específica - Retorna: (dados, próxima_página, tem_erro)"""
            try:
                # Montar URL para paginação
                start_idx = base_url.find('pesquisa=') + 9
                pesquisa_obj = json.loads(base_url[start_idx:])
                pesquisa_obj['paginacao']['pagina'] = page
                url = base_url[:start_idx] + json.dumps(pesquisa_obj)

                # Criar um ID único para a requisição
                req_id = f"clientes_{page}"

                # Verifica se já processamos esta requisição antes
                if req_id in self.processed_urls:
                    logger.warning(f"Requisição duplicada evitada: {req_id}")
                    return [], 0, True

                self.processed_urls.add(req_id)

                # Fazer a requisição
                response_data = self.make_request(url)

                if "error" in response_data:
                    self.failed_requests.append(url)
                    return [], 0, True

                # Extrai dados da resposta
                data = self.extract_data_from_response(response_data)

                # Atualiza contador de páginas
                self.total_pages += 1

                next_page = page + 1 if len(data) > 0 else 0

                # Print detalhado para acompanhamento da paginação
                print(
                    f"CLIENTES | Pág {page} | +{len(data)} | T:{self.total_collected + len(data)} | P:{self.total_pages} | F:{self.total_failed}")

                return data, next_page, False
            except Exception as e:
                logger.error(f"Erro ao processar página {page}: {str(e)}")
                self.failed_requests.append(base_url)
                self.total_failed += 1
                return [], 0, True

        def collect_clientes_with_pagination(self, base_url: str) -> pd.DataFrame:
            """Coleta todos os dados de clientes com paginação"""
            print("INICIANDO | TABELA DE CLIENTES")

            first_page_data, next_page, has_error = self.get_page_data(base_url, 1)
            all_data = first_page_data
            self.total_collected += len(first_page_data)

            # Coleta páginas adicionais enquanto houver dados
            current_page = 1
            while next_page > 0:
                current_page = next_page
                page_data, next_page, has_error = self.get_page_data(base_url, current_page)
                if has_error or not page_data:
                    logger.warning(f"Parada na paginação devido a erro ou dados vazios na página {current_page}")
                    break
                all_data.extend(page_data)
                self.total_collected += len(page_data)

                # Evita loop infinito
                if current_page > 10000:
                    logger.warning(f"Limite de segurança atingido na página {current_page}")
                    break

            # Reprocessa falhas
            if self.failed_requests:
                self.reprocess_failed_requests(all_data)

            # Converte para DataFrame e normaliza
            if all_data:
                df = pd.DataFrame([self.flatten_json(item) for item in all_data])
                df.columns = [self.normalize_column_name(col) for col in df.columns]
                print(
                    f"CONCLUÍDO | CLIENTES | Total {len(df)} | Páginas {self.total_pages} | Falhas {self.total_failed}")
                return df

            print(f"CONCLUÍDO | CLIENTES | Total 0 | Páginas {self.total_pages} | Falhas {self.total_failed}")
            return pd.DataFrame()

        def reprocess_failed_requests(self, all_data: List[Dict]):
            """Reprocessa requisições que falharam"""
            if not self.failed_requests:
                return

            failed_count = len(self.failed_requests)
            print(f"REPROCESSANDO | CLIENTES | {failed_count} pendentes")
            failed_urls = self.failed_requests.copy()
            self.failed_requests = []

            with ThreadPoolExecutor(max_workers=max(1, MAX_WORKERS // 2)) as executor:
                futures = {executor.submit(self.make_request, url): url for url in failed_urls}
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if "error" not in result:
                            data = self.extract_data_from_response(result)
                            all_data.extend(data)
                            self.total_collected += len(data)
                            self.total_failed -= 1
                    except Exception as e:
                        logger.error(f"Exceção no reprocessamento: {str(e)}")

        def save_to_gcs(self, df: pd.DataFrame):
            """Salva DataFrame como CSV no Google Cloud Storage"""
            if df.empty:
                logger.warning("DataFrame vazio, não será salvo")
                return

            try:
                blob_path = f"{FOLDER}/{FILENAME}"
                blob = self.bucket.blob(blob_path)
                blob.upload_from_string(df.to_csv(sep=';', index=False), content_type='text/csv')
                print(f"SALVO | CLIENTES | {len(df)} registros | gs://{BUCKET_NAME}/{blob_path}")
            except Exception as e:
                logger.error(f"Erro ao salvar arquivo: {str(e)}")
                print("ERRO | CLIENTES | falha ao salvar")
                raise

        def get_api_url(self) -> str:
            """Retorna a URL da API para a tabela de clientes"""
            pesquisa = json.dumps(PAYLOAD_ENDPOINT_CLIENTES)
            return f"{BASE_URL}/clientes/listar?key={TOKEN}&empresa={EMPRESA}&pesquisa={pesquisa}"

        def generate_cliente_report(self, df: pd.DataFrame):
            """Gera um relatório específico sobre os dados de clientes coletados"""
            if df.empty:
                return

            print(f"\n{'=' * 70}")
            print("RELATÓRIO DE CLIENTES")
            print(f"{'=' * 70}")

            # Estatísticas básicas
            print(f"Total de clientes: {len(df)}")

            # Análise por tipo de pessoa (se a coluna existir)
            tipo_cols = [col for col in df.columns if 'tipo' in col.lower() and 'pessoa' in col.lower()]
            if tipo_cols:
                tipo_col = tipo_cols[0]
                print(f"\nDistribuição por Tipo de Pessoa:")
                tipo_counts = df[tipo_col].value_counts()
                for tipo, count in tipo_counts.items():
                    percentage = (count / len(df)) * 100
                    tipo_name = "Pessoa Física" if str(tipo).lower() in ['f', 'pf',
                                                                         'fisica'] else "Pessoa Jurídica" if str(
                        tipo).lower() in ['j', 'pj', 'juridica'] else str(tipo)
                    print(f"  {tipo_name}: {count} ({percentage:.1f}%)")

            # Análise por status (se a coluna existir)
            status_cols = [col for col in df.columns if 'status' in col.lower()]
            if status_cols:
                status_col = status_cols[0]
                print(f"\nDistribuição por Status:")
                status_counts = df[status_col].value_counts()
                for status, count in status_counts.head(10).items():  # Top 10
                    percentage = (count / len(df)) * 100
                    print(f"  {status}: {count} ({percentage:.1f}%)")

            # Análise por UF (se a coluna existir)
            uf_cols = [col for col in df.columns if 'uf' in col.lower() and 'residencial' in col.lower()]
            if uf_cols:
                uf_col = uf_cols[0]
                print(f"\nDistribuição por UF (Top 10):")
                uf_counts = df[uf_col].value_counts()
                for uf, count in uf_counts.head(10).items():
                    percentage = (count / len(df)) * 100
                    print(f"  {uf}: {count} ({percentage:.1f}%)")

            # Análise por cidade (se a coluna existir)
            cidade_cols = [col for col in df.columns if 'cidade' in col.lower() and 'residencial' in col.lower()]
            if cidade_cols:
                cidade_col = cidade_cols[0]
                print(f"\nDistribuição por Cidade (Top 10):")
                cidade_counts = df[cidade_col].value_counts()
                for cidade, count in cidade_counts.head(10).items():
                    percentage = (count / len(df)) * 100
                    print(f"  {cidade}: {count} ({percentage:.1f}%)")

            # Análise de profissões (se a coluna existir)
            prof_cols = [col for col in df.columns if 'profissao' in col.lower()]
            if prof_cols:
                prof_col = prof_cols[0]
                print(f"\nProfissões mais comuns (Top 10):")
                prof_counts = df[prof_col].value_counts()
                for prof, count in prof_counts.head(10).items():
                    percentage = (count / len(df)) * 100
                    if pd.notna(prof) and str(prof).strip():
                        print(f"  {prof}: {count} ({percentage:.1f}%)")

            # Análise por veículo de captação (se a coluna existir)
            veiculo_cols = [col for col in df.columns if 'veiculo' in col.lower() and 'captacao' in col.lower()]
            if veiculo_cols:
                veiculo_col = veiculo_cols[0]
                print(f"\nVeículos de Captação (Top 10):")
                veiculo_counts = df[veiculo_col].value_counts()
                for veiculo, count in veiculo_counts.head(10).items():
                    percentage = (count / len(df)) * 100
                    if pd.notna(veiculo) and str(veiculo).strip():
                        print(f"  {veiculo}: {count} ({percentage:.1f}%)")

            # Análise de completude dos dados
            print(f"\nCompletude dos Dados (Top 15 campos com mais dados):")
            completude = {}
            for col in df.columns:
                non_null_count = df[col].notna().sum()
                completude[col] = (non_null_count / len(df)) * 100

            sorted_completude = sorted(completude.items(), key=lambda x: x[1], reverse=True)
            for col, percentage in sorted_completude[:15]:
                print(f"  {col}: {percentage:.1f}%")

            print(f"{'=' * 70}\n")

        def validate_cliente_data(self, df: pd.DataFrame):
            """Valida a qualidade dos dados de clientes"""
            if df.empty:
                return

            print(f"\n{'=' * 70}")
            print("VALIDAÇÃO DE QUALIDADE DOS DADOS")
            print(f"{'=' * 70}")

            # Validação de CPFs/CNPJs
            cpf_cols = [col for col in df.columns if 'cpf' in col.lower() or 'cnpj' in col.lower()]
            if cpf_cols:
                cpf_col = cpf_cols[0]
                cpf_preenchidos = df[cpf_col].notna().sum()
                print(f"CPFs/CNPJs preenchidos: {cpf_preenchidos} ({(cpf_preenchidos / len(df) * 100):.1f}%)")

            # Validação de emails
            email_cols = [col for col in df.columns if 'email' in col.lower()]
            if email_cols:
                for email_col in email_cols[:3]:  # Máximo 3 campos de email
                    email_preenchidos = df[email_col].notna().sum()
                    print(f"Emails {email_col}: {email_preenchidos} ({(email_preenchidos / len(df) * 100):.1f}%)")

            # Validação de telefones
            tel_cols = [col for col in df.columns if
                        any(term in col.lower() for term in ['fone', 'celular', 'telefone'])]
            if tel_cols:
                tel_preenchidos = 0
                for tel_col in tel_cols[:3]:  # Máximo 3 campos de telefone
                    count = df[tel_col].notna().sum()
                    tel_preenchidos += count
                    print(f"Telefones {tel_col}: {count} ({(count / len(df) * 100):.1f}%)")

            # Validação de endereços
            end_cols = [col for col in df.columns if 'endereco' in col.lower()]
            if end_cols:
                for end_col in end_cols[:2]:  # Máximo 2 campos de endereço
                    end_preenchidos = df[end_col].notna().sum()
                    print(f"Endereços {end_col}: {end_preenchidos} ({(end_preenchidos / len(df) * 100):.1f}%)")

            print(f"{'=' * 70}\n")

        def run(self):
            """Executa a coleta completa de dados da tabela de clientes"""
            try:
                print(f"INICIANDO | ClientesCollector | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

                # Coleta dados de clientes
                url = self.get_api_url()
                clientes_df = self.collect_clientes_with_pagination(url)

                # Gera relatórios específicos de clientes
                self.generate_cliente_report(clientes_df)
                self.validate_cliente_data(clientes_df)

                # Salva os dados coletados
                self.save_to_gcs(clientes_df)
            except Exception as e:
                logger.error(f"Erro durante a execução: {str(e)}")
                print(f"ERRO | Execução geral | {str(e)}")
                raise
            finally:
                # Exibe resumo
                self.print_summary()

        def print_summary(self):
            """Exibe resumo da execução"""
            execution_time = time.time() - self.start_time
            hours, remainder = divmod(execution_time, 3600)
            minutes, seconds = divmod(remainder, 60)

            print(f"\n{'=' * 80}")
            print(f"RESUMO DA EXECUÇÃO - TABELA DE CLIENTES")
            print(f"{'=' * 80}")
            print(f"Tempo de execução: {int(hours)}h{int(minutes)}m{int(seconds)}s")
            print(f"Total de registros coletados: {self.total_collected}")
            print(f"Total de páginas processadas: {self.total_pages}")
            print(f"Total de falhas: {self.total_failed}")
            print(
                f"Taxa de sucesso: {((self.total_collected / (self.total_collected + self.total_failed)) * 100):.2f}%" if (
                                                                                                                                      self.total_collected + self.total_failed) > 0 else "N/A")
            print(
                f"Registros por página (média): {(self.total_collected / self.total_pages):.1f}" if self.total_pages > 0 else "N/A")
            print(f"Total de campos coletados: 67 campos + CorretorCliente")
            print(f"{'=' * 80}\n")

    # START
    print(f"INICIANDO | ClientesCollector | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    collector = ClientesCollector()
    collector.run()


def extract_realtor(customer):
    import logging
    import pandas as pd
    import random
    import re
    import requests
    import time
    import unicodedata
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from datetime import datetime
    from google.cloud import storage
    from typing import Dict, List, Tuple
    import os
    import pathlib

    # Variáveis globais
    BASE_URL = customer['api_base_url']
    TOKEN = customer['api_token']
    EMPRESA = customer['empresa']
    BUCKET_NAME = customer['bucket_name']
    FOLDER = "tabela_corretores"
    FILENAME = "corretores.csv"
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    MAX_RETRIES, MAX_WORKERS, RECORDS_PER_PAGE = 5, 5, 50

    # Configuração de logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(), logging.FileHandler('corretores_collector.log')]
    )
    logger = logging.getLogger('corretores_collector')

    class CorretoresCollector:
        """Classe para coleta de dados da Tabela de Corretores da API VistaHost"""

        def __init__(self):
            self.session = requests.Session()
            self.session.headers.update({'Accept': 'application/json', 'Content-Type': 'application/json'})
            self.total_collected = 0
            self.total_failed = 0
            self.total_pages = 0
            self.start_time = time.time()
            self.failed_requests = []
            self.processed_urls = set()  # Para evitar duplicação de logs
            self.corretor_stats = {}  # Para análise de estatísticas de corretores
            self.init_gcs_client()

        def init_gcs_client(self):
            """Inicializa cliente do Google Cloud Storage"""
            try:
                self.storage_client = storage.Client(project=customer['project_id'])
                self.bucket = self.storage_client.bucket(BUCKET_NAME)
                logger.info(f"Cliente GCS inicializado com sucesso para o bucket: {BUCKET_NAME}")
            except Exception as e:
                logger.error(f"Erro ao inicializar cliente GCS: {str(e)}")
                raise

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

        def make_request(self, url: str, retries: int = 0) -> Dict:
            """Faz uma requisição à API com estratégia de backoff exponencial"""
            try:
                response = self.session.get(url, timeout=30)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429 or response.status_code >= 500:
                    if retries < MAX_RETRIES:
                        wait_time = (2 ** retries) + random.uniform(0, 1)
                        logger.info(f"Limitação da API. Aguardando {wait_time:.2f}s")
                        time.sleep(wait_time)
                        return self.make_request(url, retries + 1)
                    else:
                        logger.error(f"Falha após {MAX_RETRIES} tentativas")
                        self.total_failed += 1
                        return {"error": f"Falha após {MAX_RETRIES} tentativas", "url": url}
                else:
                    logger.error(f"Erro na requisição. Status: {response.status_code}")
                    if response.status_code == 401:
                        print('ERRO NA AUTENTICACAO/MONTAGEM DA URL DINAMICA')
                        raise
                    if retries < MAX_RETRIES:
                        time.sleep((2 ** retries) + random.uniform(0, 1))
                        return self.make_request(url, retries + 1)
                    self.total_failed += 1
                    return {"error": f"Erro {response.status_code}", "url": url}
            except Exception as e:
                logger.error(f"Exceção ao fazer requisição: {str(e)}")
                if retries < MAX_RETRIES:
                    time.sleep((2 ** retries) + random.uniform(0, 1))
                    return self.make_request(url, retries + 1)
                self.total_failed += 1
                return {"error": str(e), "url": url}

        def extract_data_from_response(self, response_data: Dict) -> List[Dict]:
            """Extrai dados da resposta da API"""
            if "error" in response_data:
                return []

            # Log da estrutura da resposta para debug
            logger.info(
                f"Estrutura da resposta: {list(response_data.keys()) if isinstance(response_data, dict) else type(response_data)}")

            # Para o endpoint de corretores, verifica diferentes estruturas possíveis
            if isinstance(response_data, list):
                logger.info(f"Resposta é uma lista com {len(response_data)} itens")
                return response_data
            elif isinstance(response_data, dict):
                # Primeiro, verifica todas as chaves disponíveis
                logger.info(f"Chaves na resposta: {list(response_data.keys())}")

                # Verifica se há chaves numéricas (padrão comum da API)
                numeric_keys = [k for k in response_data.keys() if k.isdigit()]
                if numeric_keys:
                    logger.info(f"Encontradas {len(numeric_keys)} chaves numéricas")
                    return [response_data[k] for k in numeric_keys]

                # Verifica possíveis chaves que podem conter os dados
                possible_data_keys = ['data', 'dados', 'corretores', 'items', 'results', 'list']
                for key in possible_data_keys:
                    if key in response_data:
                        data = response_data[key]
                        logger.info(f"Dados encontrados na chave '{key}': {type(data)}")
                        if isinstance(data, list):
                            logger.info(f"Lista com {len(data)} itens")
                            return data
                        elif isinstance(data, dict):
                            # Se é um dict, pode ser um único corretor
                            return [data]

                # Se não encontrou nas chaves específicas, verifica se o próprio response_data é um corretor
                # Busca por campos típicos de corretor
                corretor_fields = ['nome', 'codigo', 'id', 'creci', 'email', 'cpf', 'telefone', 'celular']
                if any(field in response_data for field in corretor_fields):
                    logger.info("Resposta parece ser um único corretor")
                    return [response_data]

                # Se nada funcionou, log da resposta completa (primeiros 500 chars)
                response_str = str(response_data)[:500]
                logger.warning(f"Estrutura não reconhecida. Primeiro trecho da resposta: {response_str}")

            logger.warning("Nenhum dado extraído da resposta")
            return []

        def get_page_data(self, base_url: str, page: int) -> Tuple[List[Dict], int, bool]:
            """Obtém os dados de uma página específica - Retorna: (dados, próxima_página, tem_erro)"""
            try:
                # Para o endpoint de corretores, a paginação é feita com o parâmetro 'page'
                url = f"{base_url.split('&page=')[0]}&page={page}"

                # Log da URL sendo chamada
                logger.info(f"Chamando URL: {url}")

                # Criar um ID único para a requisição
                req_id = f"corretores_{page}"

                # Verifica se já processamos esta requisição antes
                if req_id in self.processed_urls:
                    logger.warning(f"Requisição duplicada evitada: {req_id}")
                    return [], 0, True

                self.processed_urls.add(req_id)

                # Fazer a requisição
                response_data = self.make_request(url)

                if "error" in response_data:
                    logger.error(f"Erro na resposta: {response_data}")
                    self.failed_requests.append(url)
                    return [], 0, True

                # Log do tipo de resposta recebida
                logger.info(f"Tipo de resposta recebida: {type(response_data)}")

                # Extrai dados da resposta
                data = self.extract_data_from_response(response_data)

                # Log dos dados extraídos
                logger.info(f"Dados extraídos: {len(data)} registros")
                if len(data) > 0 and isinstance(data[0], dict):
                    logger.info(f"Campos do primeiro registro: {list(data[0].keys())}")

                # Atualiza contador de páginas
                self.total_pages += 1

                # Para corretores, verifica se há mais páginas baseado no número de registros
                # Se retornou dados, tenta a próxima página
                next_page = page + 1 if len(data) > 0 else 0

                # Print detalhado para acompanhamento da paginação
                print(
                    f"CORRETORES | Pág {page} | +{len(data)} | T:{self.total_collected + len(data)} | P:{self.total_pages} | F:{self.total_failed}")

                return data, next_page, False
            except Exception as e:
                logger.error(f"Erro ao processar página {page}: {str(e)}")
                self.failed_requests.append(base_url)
                self.total_failed += 1
                return [], 0, True

        def collect_corretores_with_pagination(self, base_url: str) -> pd.DataFrame:
            """Coleta todos os dados de corretores com paginação"""
            print("INICIANDO | TABELA DE CORRETORES")

            first_page_data, next_page, has_error = self.get_page_data(base_url, 1)
            all_data = first_page_data
            self.total_collected += len(first_page_data)

            # Coleta páginas adicionais enquanto houver dados
            current_page = 1
            consecutive_empty_pages = 0
            max_empty_pages = 3  # Para após 3 páginas vazias consecutivas

            while next_page > 0 and consecutive_empty_pages < max_empty_pages:
                current_page = next_page
                page_data, next_page, has_error = self.get_page_data(base_url, current_page)

                if has_error:
                    logger.warning(f"Erro na página {current_page}")
                    consecutive_empty_pages += 1
                    continue

                if not page_data:
                    consecutive_empty_pages += 1
                    logger.info(f"Página {current_page} vazia ({consecutive_empty_pages}/{max_empty_pages})")
                    # Continua tentando até atingir o limite de páginas vazias
                    if consecutive_empty_pages < max_empty_pages:
                        continue
                    else:
                        logger.warning(f"Parando após {max_empty_pages} páginas vazias consecutivas")
                        break
                else:
                    consecutive_empty_pages = 0  # Reset do contador se encontrou dados
                    all_data.extend(page_data)
                    self.total_collected += len(page_data)

                # Evita loop infinito - limite de segurança
                if current_page > 500:  # Limite de segurança para corretores
                    logger.warning(f"Limite de segurança atingido na página {current_page}")
                    break

            # Reprocessa falhas, se houver
            if self.failed_requests:
                self.reprocess_failed_requests(all_data)

            # Converte para DataFrame e normaliza
            if all_data:
                df = pd.DataFrame([self.flatten_json(item) for item in all_data])
                df.columns = [self.normalize_column_name(col) for col in df.columns]
                print(
                    f"CONCLUÍDO | CORRETORES | Total {len(df)} | Páginas {self.total_pages} | Falhas {self.total_failed}")
                return df

            print(f"CONCLUÍDO | CORRETORES | Total 0 | Páginas {self.total_pages} | Falhas {self.total_failed}")
            return pd.DataFrame()

        def reprocess_failed_requests(self, all_data: List[Dict]):
            """Reprocessa requisições que falharam"""
            if not self.failed_requests:
                return

            failed_count = len(self.failed_requests)
            print(f"REPROCESSANDO | CORRETORES | {failed_count} pendentes")
            failed_urls = self.failed_requests.copy()
            self.failed_requests = []

            with ThreadPoolExecutor(max_workers=max(1, MAX_WORKERS // 2)) as executor:
                futures = {executor.submit(self.make_request, url): url for url in failed_urls}
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if "error" not in result:
                            data = self.extract_data_from_response(result)
                            all_data.extend(data)
                            self.total_collected += len(data)
                            self.total_failed -= 1
                    except Exception as e:
                        logger.error(f"Exceção no reprocessamento: {str(e)}")

        def save_to_gcs(self, df: pd.DataFrame):
            """Salva DataFrame como CSV no Google Cloud Storage"""
            if df.empty:
                logger.warning("DataFrame vazio, não será salvo")
                return

            try:
                blob_path = f"{FOLDER}/{FILENAME}"
                blob = self.bucket.blob(blob_path)
                blob.upload_from_string(df.to_csv(sep=';', index=False), content_type='text/csv')
                print(f"SALVO | CORRETORES | {len(df)} registros | gs://{BUCKET_NAME}/{blob_path}")
            except Exception as e:
                logger.error(f"Erro ao salvar arquivo: {str(e)}")
                print("ERRO | CORRETORES | falha ao salvar")
                raise

        def get_api_url(self) -> str:
            """Retorna a URL da API para a tabela de corretores"""
            return f"{BASE_URL}/corretores/listar?key={TOKEN}&empresa={EMPRESA}&page=1"

        def generate_corretor_report(self, df: pd.DataFrame):
            """Gera um relatório específico sobre os dados de corretores coletados"""
            if df.empty:
                return

            print(f"\n{'=' * 70}")
            print("RELATÓRIO DE CORRETORES")
            print(f"{'=' * 70}")

            # Estatísticas básicas
            print(f"Total de corretores: {len(df)}")

            # Análise por status (se a coluna existir)
            status_cols = [col for col in df.columns if 'status' in col.lower()]
            if status_cols:
                status_col = status_cols[0]
                print(f"\nDistribuição por Status:")
                status_counts = df[status_col].value_counts()
                for status, count in status_counts.items():
                    percentage = (count / len(df)) * 100
                    print(f"  {status}: {count} ({percentage:.1f}%)")

            # Análise por tipo de corretor (se a coluna existir)
            tipo_cols = [col for col in df.columns if 'tipo' in col.lower()]
            if tipo_cols:
                tipo_col = tipo_cols[0]
                print(f"\nDistribuição por Tipo:")
                tipo_counts = df[tipo_col].value_counts()
                for tipo, count in tipo_counts.items():
                    percentage = (count / len(df)) * 100
                    print(f"  {tipo}: {count} ({percentage:.1f}%)")

            # Análise de CRECI (se a coluna existir)
            creci_cols = [col for col in df.columns if 'creci' in col.lower()]
            if creci_cols:
                creci_col = creci_cols[0]
                creci_preenchidos = df[creci_col].notna().sum()
                print(f"\nCRECI preenchidos: {creci_preenchidos} ({(creci_preenchidos / len(df) * 100):.1f}%)")

            # Análise por equipe/supervisor (se a coluna existir)
            supervisor_cols = [col for col in df.columns if
                               any(term in col.lower() for term in ['supervisor', 'equipe', 'gerente', 'lider'])]
            if supervisor_cols:
                supervisor_col = supervisor_cols[0]
                print(f"\nDistribuição por {supervisor_col} (Top 10):")
                supervisor_counts = df[supervisor_col].value_counts()
                for supervisor, count in supervisor_counts.head(10).items():
                    percentage = (count / len(df)) * 100
                    if pd.notna(supervisor) and str(supervisor).strip():
                        print(f"  {supervisor}: {count} ({percentage:.1f}%)")

            # Análise de contatos
            email_cols = [col for col in df.columns if 'email' in col.lower()]
            if email_cols:
                email_col = email_cols[0]
                email_preenchidos = df[email_col].notna().sum()
                print(f"\nEmails preenchidos: {email_preenchidos} ({(email_preenchidos / len(df) * 100):.1f}%)")

            tel_cols = [col for col in df.columns if
                        any(term in col.lower() for term in ['fone', 'celular', 'telefone'])]
            if tel_cols:
                tel_col = tel_cols[0]
                tel_preenchidos = df[tel_col].notna().sum()
                print(f"Telefones preenchidos: {tel_preenchidos} ({(tel_preenchidos / len(df) * 100):.1f}%)")

            # Análise de atividade/produtividade (se houver colunas relacionadas)
            atividade_cols = [col for col in df.columns if
                              any(term in col.lower() for term in ['ativo', 'atividade', 'vendas', 'comissao', 'meta'])]
            if atividade_cols:
                print(f"\nCampos de Atividade/Produtividade encontrados:")
                for col in atividade_cols[:5]:  # Máximo 5 campos
                    valores_preenchidos = df[col].notna().sum()
                    print(f"  {col}: {valores_preenchidos} ({(valores_preenchidos / len(df) * 100):.1f}%)")

            # Análise de completude dos dados
            print(f"\nCompletude dos Dados (Top 10 campos com mais dados):")
            completude = {}
            for col in df.columns:
                non_null_count = df[col].notna().sum()
                completude[col] = (non_null_count / len(df)) * 100

            sorted_completude = sorted(completude.items(), key=lambda x: x[1], reverse=True)
            for col, percentage in sorted_completude[:10]:
                print(f"  {col}: {percentage:.1f}%")

            print(f"{'=' * 70}\n")

        def validate_corretor_data(self, df: pd.DataFrame):
            """Valida a qualidade dos dados de corretores"""
            if df.empty:
                return

            print(f"\n{'=' * 70}")
            print("VALIDAÇÃO DE QUALIDADE DOS DADOS DE CORRETORES")
            print(f"{'=' * 70}")

            # Validação de campos obrigatórios
            nome_cols = [col for col in df.columns if 'nome' in col.lower()]
            if nome_cols:
                nome_col = nome_cols[0]
                nomes_preenchidos = df[nome_col].notna().sum()
                print(f"Nomes preenchidos: {nomes_preenchidos} ({(nomes_preenchidos / len(df) * 100):.1f}%)")

            codigo_cols = [col for col in df.columns if 'codigo' in col.lower()]
            if codigo_cols:
                codigo_col = codigo_cols[0]
                codigos_preenchidos = df[codigo_col].notna().sum()
                codigos_unicos = df[codigo_col].nunique()
                print(f"Códigos preenchidos: {codigos_preenchidos} ({(codigos_preenchidos / len(df) * 100):.1f}%)")
                print(f"Códigos únicos: {codigos_unicos} (duplicatas: {len(df) - codigos_unicos})")

            # Validação de CRECI
            creci_cols = [col for col in df.columns if 'creci' in col.lower()]
            if creci_cols:
                creci_col = creci_cols[0]
                creci_preenchidos = df[creci_col].notna().sum()
                creci_unicos = df[creci_col].nunique()
                print(f"CRECI preenchidos: {creci_preenchidos} ({(creci_preenchidos / len(df) * 100):.1f}%)")
                print(f"CRECI únicos: {creci_unicos}")

            # Validação de contatos
            email_cols = [col for col in df.columns if 'email' in col.lower()]
            if email_cols:
                email_col = email_cols[0]
                email_preenchidos = df[email_col].notna().sum()
                print(f"Emails preenchidos: {email_preenchidos} ({(email_preenchidos / len(df) * 100):.1f}%)")

            tel_cols = [col for col in df.columns if
                        any(term in col.lower() for term in ['fone', 'celular', 'telefone'])]
            if tel_cols:
                tel_col = tel_cols[0]
                tel_preenchidos = df[tel_col].notna().sum()
                print(f"Telefones preenchidos: {tel_preenchidos} ({(tel_preenchidos / len(df) * 100):.1f}%)")

            # Detecção de possíveis duplicatas por nome
            if nome_cols:
                nome_col = nome_cols[0]
                nomes_duplicados = df[nome_col].value_counts()
                duplicatas = nomes_duplicados[nomes_duplicados > 1]
                if len(duplicatas) > 0:
                    print(f"\nPossíveis duplicatas por nome: {len(duplicatas)} nomes com múltiplas ocorrências")
                    print("Top 5 nomes duplicados:")
                    for nome, count in duplicatas.head(5).items():
                        if pd.notna(nome):
                            print(f"  {nome}: {count} ocorrências")

            print(f"{'=' * 70}\n")

        def generate_corretor_summary_stats(self, df: pd.DataFrame):
            """Gera estatísticas resumidas para dashboard"""
            if df.empty:
                return

            print(f"\n{'=' * 50}")
            print("ESTATÍSTICAS RESUMIDAS - CORRETORES")
            print(f"{'=' * 50}")

            # Estatísticas principais
            total_corretores = len(df)
            print(f"📊 Total de Corretores: {total_corretores}")

            # Taxa de preenchimento de dados críticos
            campos_criticos = []
            for col in df.columns:
                if any(term in col.lower() for term in ['nome', 'email', 'fone', 'celular', 'creci']):
                    campos_criticos.append(col)

            if campos_criticos:
                print(f"\n📋 Qualidade dos Dados:")
                for campo in campos_criticos[:5]:  # Top 5 campos críticos
                    preenchidos = df[campo].notna().sum()
                    percentual = (preenchidos / total_corretores) * 100
                    status_emoji = "✅" if percentual >= 90 else "⚠️" if percentual >= 70 else "❌"
                    print(f"  {status_emoji} {campo}: {percentual:.1f}%")

            # Status de atividade (se disponível)
            status_cols = [col for col in df.columns if 'status' in col.lower()]
            if status_cols:
                status_col = status_cols[0]
                ativos = df[status_col].value_counts().get('Ativo', 0)
                if ativos == 0:
                    # Tenta outras variações
                    for valor in df[status_col].value_counts().index:
                        if any(term in str(valor).lower() for term in ['ativo', 'active', '1', 'sim']):
                            ativos = df[status_col].value_counts().get(valor, 0)
                            break

                if ativos > 0:
                    taxa_atividade = (ativos / total_corretores) * 100
                    print(f"\n🎯 Taxa de Atividade: {taxa_atividade:.1f}% ({ativos} corretores ativos)")

            print(f"{'=' * 50}\n")

        def run(self):
            """Executa a coleta completa de dados da tabela de corretores"""
            try:
                print(f"INICIANDO | CorretoresCollector | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

                # Primeiro, vamos fazer um teste da primeira página para ver a estrutura
                print("TESTE INICIAL | Verificando estrutura da API...")
                test_url = self.get_api_url()
                test_response = self.make_request(test_url)

                if "error" not in test_response:
                    print(f"TESTE OK | Resposta recebida: {type(test_response)}")
                    if isinstance(test_response, dict):
                        print(f"TESTE OK | Chaves na resposta: {list(test_response.keys())}")
                    elif isinstance(test_response, list):
                        print(f"TESTE OK | Lista com {len(test_response)} itens")

                    # Mostra amostra dos dados para debug
                    sample_data = self.extract_data_from_response(test_response)
                    if sample_data:
                        print(f"TESTE OK | {len(sample_data)} registros extraídos")
                        if len(sample_data) > 0:
                            first_record = sample_data[0]
                            if isinstance(first_record, dict):
                                print(f"TESTE OK | Campos do primeiro registro: {list(first_record.keys())[:10]}...")
                    else:
                        print("TESTE FALHA | Nenhum dado extraído - verificando estrutura...")
                        # Mostra estrutura completa para debug
                        print(f"Resposta completa (primeiros 1000 chars): {str(test_response)[:1000]}")
                else:
                    print(f"TESTE FALHA | Erro na requisição: {test_response}")
                    return

                # Agora executa a coleta completa
                print("\nINICIANDO COLETA COMPLETA...")
                url = self.get_api_url()
                corretores_df = self.collect_corretores_with_pagination(url)

                if not corretores_df.empty:
                    # Gera relatórios específicos de corretores
                    self.generate_corretor_report(corretores_df)
                    self.validate_corretor_data(corretores_df)
                    self.generate_corretor_summary_stats(corretores_df)

                    # Salva os dados coletados
                    self.save_to_gcs(corretores_df)
                else:
                    print("NENHUM DADO COLETADO | Verifique os logs para mais detalhes")

            except Exception as e:
                logger.error(f"Erro durante a execução: {str(e)}")
                print(f"ERRO | Execução geral | {str(e)}")
                raise
            finally:
                # Exibe resumo
                self.print_summary()

        def print_summary(self):
            """Exibe resumo da execução"""
            execution_time = time.time() - self.start_time
            hours, remainder = divmod(execution_time, 3600)
            minutes, seconds = divmod(remainder, 60)

            print(f"\n{'=' * 80}")
            print(f"RESUMO DA EXECUÇÃO - TABELA DE CORRETORES")
            print(f"{'=' * 80}")
            print(f"Tempo de execução: {int(hours)}h{int(minutes)}m{int(seconds)}s")
            print(f"Total de registros coletados: {self.total_collected}")
            print(f"Total de páginas processadas: {self.total_pages}")
            print(f"Total de falhas: {self.total_failed}")
            print(
                f"Taxa de sucesso: {((self.total_collected / (self.total_collected + self.total_failed)) * 100):.2f}%" if (
                                                                                                                                      self.total_collected + self.total_failed) > 0 else "N/A")
            print(
                f"Registros por página (média): {(self.total_collected / self.total_pages):.1f}" if self.total_pages > 0 else "N/A")
            print(f"Método de paginação: page parameter")
            print(f"Endpoint: /corretores/listar")
            print(f"{'=' * 80}\n")

    # START
    print(f"INICIANDO | CorretoresCollector | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    collector = CorretoresCollector()
    collector.run()


def extract_rental_funnel(customer):
    import json
    import logging
    import pandas as pd
    import random
    import re
    import requests
    import time
    import unicodedata
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from datetime import datetime
    from google.cloud import storage
    from typing import Dict, List, Tuple
    import os
    import pathlib

    # Variáveis globais
    BASE_URL = customer['api_base_url']
    TOKEN = customer['api_token']
    EMPRESA = customer['empresa']
    BUCKET_NAME = customer['bucket_name']
    ENDPOINT_FUNIL_LOCACAO = dict(customer['endpoint_funil_locacao'])
    FOLDER = "funil_locacao"
    FILENAME = "funil_locacao.csv"
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    MAX_RETRIES, MAX_WORKERS, RECORDS_PER_PAGE = 5, 5, 50
    CODIGO_PIPE_LOCACAO = "2"  # Pipeline específico para locação

    # Configuração de logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(), logging.FileHandler('funil_locacao_collector.log')]
    )
    logger = logging.getLogger('funil_locacao_collector')

    class FunilLocacaoCollector:
        """Classe para coleta de dados do Funil de Locação da API VistaHost"""

        def __init__(self):
            self.session = requests.Session()
            self.session.headers.update({'Accept': 'application/json', 'Content-Type': 'application/json'})
            self.total_collected = 0
            self.total_failed = 0
            self.total_pages = 0
            self.start_time = time.time()
            self.failed_requests = []
            self.processed_urls = set()  # Para evitar duplicação de logs
            self.init_gcs_client()

        def init_gcs_client(self):
            """Inicializa cliente do Google Cloud Storage"""
            try:
                self.storage_client = storage.Client(project=customer['project_id'])
                self.bucket = self.storage_client.bucket(BUCKET_NAME)
                logger.info(f"Cliente GCS inicializado com sucesso para o bucket: {BUCKET_NAME}")
            except Exception as e:
                logger.error(f"Erro ao inicializar cliente GCS: {str(e)}")
                raise

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

        def make_request(self, url: str, retries: int = 0) -> Dict:
            """Faz uma requisição à API com estratégia de backoff exponencial"""
            try:
                response = self.session.get(url, timeout=30)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429 or response.status_code >= 500:
                    if retries < MAX_RETRIES:
                        wait_time = (2 ** retries) + random.uniform(0, 1)
                        logger.info(f"Limitação da API. Aguardando {wait_time:.2f}s")
                        time.sleep(wait_time)
                        return self.make_request(url, retries + 1)
                    else:
                        logger.error(f"Falha após {MAX_RETRIES} tentativas")
                        self.total_failed += 1
                        return {"error": f"Falha após {MAX_RETRIES} tentativas", "url": url}
                else:
                    logger.error(f"Erro na requisição. Status: {response.status_code}")
                    if response.status_code == 401:
                        print('ERRO NA AUTENTICACAO/MONTAGEM DA URL DINAMICA')
                        raise
                    if retries < MAX_RETRIES:
                        time.sleep((2 ** retries) + random.uniform(0, 1))
                        return self.make_request(url, retries + 1)
                    self.total_failed += 1
                    return {"error": f"Erro {response.status_code}", "url": url}
            except Exception as e:
                logger.error(f"Exceção ao fazer requisição: {str(e)}")
                if retries < MAX_RETRIES:
                    time.sleep((2 ** retries) + random.uniform(0, 1))
                    return self.make_request(url, retries + 1)
                self.total_failed += 1
                return {"error": str(e), "url": url}

        def extract_data_from_response(self, response_data: Dict) -> List[Dict]:
            """Extrai dados da resposta da API"""
            if "error" in response_data:
                return []

            # Para o endpoint de negócios, os dados estão em chaves numéricas
            numeric_keys = [k for k in response_data.keys() if k.isdigit()]
            return [response_data[k] for k in numeric_keys]

        def get_page_data(self, base_url: str, page: int) -> Tuple[List[Dict], int, bool]:
            """Obtém os dados de uma página específica - Retorna: (dados, próxima_página, tem_erro)"""
            try:
                # Montar URL para paginação
                start_idx = base_url.find('pesquisa=') + 9
                pesquisa_obj = json.loads(base_url[start_idx:])
                pesquisa_obj['paginacao']['pagina'] = page
                url = base_url[:start_idx] + json.dumps(pesquisa_obj)

                # Criar um ID único para a requisição
                req_id = f"funil_locacao_{page}"

                # Verifica se já processamos esta requisição antes
                if req_id in self.processed_urls:
                    logger.warning(f"Requisição duplicada evitada: {req_id}")
                    return [], 0, True

                self.processed_urls.add(req_id)

                # Fazer a requisição
                response_data = self.make_request(url)

                if "error" in response_data:
                    self.failed_requests.append(url)
                    return [], 0, True

                # Extrai dados da resposta
                data = self.extract_data_from_response(response_data)

                # Atualiza contador de páginas
                self.total_pages += 1

                # Verifica se há mais páginas
                next_page = page + 1 if len(data) >= RECORDS_PER_PAGE else 0

                # Print detalhado para acompanhamento da paginação
                print(
                    f"FUNIL_LOCACAO | Pág {page} | +{len(data)} | T:{self.total_collected + len(data)} | P:{self.total_pages} | F:{self.total_failed}")

                return data, next_page, False
            except Exception as e:
                logger.error(f"Erro ao processar página {page}: {str(e)}")
                self.failed_requests.append(base_url)
                self.total_failed += 1
                return [], 0, True

        def collect_funil_locacao_with_pagination(self, base_url: str) -> pd.DataFrame:
            """Coleta todos os dados do funil de locação com paginação"""
            print("INICIANDO | FUNIL DE LOCAÇÃO")

            first_page_data, next_page, has_error = self.get_page_data(base_url, 1)
            all_data = first_page_data
            self.total_collected += len(first_page_data)

            # Coleta páginas adicionais enquanto houver dados
            current_page = 1
            while next_page > 0:
                current_page = next_page
                page_data, next_page, has_error = self.get_page_data(base_url, current_page)
                if has_error or not page_data:
                    logger.warning(f"Parada na paginação devido a erro ou dados vazios na página {current_page}")
                    break
                all_data.extend(page_data)
                self.total_collected += len(page_data)

            # Reprocessa falhas, se houver
            if self.failed_requests:
                self.reprocess_failed_requests(all_data)

            # Converte para DataFrame e normaliza
            if all_data:
                df = pd.DataFrame([self.flatten_json(item) for item in all_data])
                df.columns = [self.normalize_column_name(col) for col in df.columns]
                print(
                    f"CONCLUÍDO | FUNIL_LOCACAO | Total {len(df)} | Páginas {self.total_pages} | Falhas {self.total_failed}")
                return df

            print(f"CONCLUÍDO | FUNIL_LOCACAO | Total 0 | Páginas {self.total_pages} | Falhas {self.total_failed}")
            return pd.DataFrame()

        def reprocess_failed_requests(self, all_data: List[Dict]):
            """Reprocessa requisições que falharam"""
            if not self.failed_requests:
                return

            failed_count = len(self.failed_requests)
            print(f"REPROCESSANDO | FUNIL_LOCACAO | {failed_count} pendentes")
            failed_urls = self.failed_requests.copy()
            self.failed_requests = []

            with ThreadPoolExecutor(max_workers=max(1, MAX_WORKERS // 2)) as executor:
                futures = {executor.submit(self.make_request, url): url for url in failed_urls}
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if "error" not in result:
                            data = self.extract_data_from_response(result)
                            all_data.extend(data)
                            self.total_collected += len(data)
                            self.total_failed -= 1
                    except Exception as e:
                        logger.error(f"Exceção no reprocessamento: {str(e)}")

        def save_to_gcs(self, df: pd.DataFrame):
            """Salva DataFrame como CSV no Google Cloud Storage"""
            if df.empty:
                logger.warning("DataFrame vazio, não será salvo")
                return

            try:
                blob_path = f"{FOLDER}/{FILENAME}"
                blob = self.bucket.blob(blob_path)
                blob.upload_from_string(df.to_csv(sep=';', index=False), content_type='text/csv')
                print(f"SALVO | FUNIL_LOCACAO | {len(df)} registros | gs://{BUCKET_NAME}/{blob_path}")
            except Exception as e:
                logger.error(f"Erro ao salvar arquivo: {str(e)}")
                print("ERRO | FUNIL_LOCACAO | falha ao salvar")
                raise

        def get_api_url(self, codigo_pipe: str = CODIGO_PIPE_LOCACAO) -> str:
            """Retorna a URL da API para o funil de locação"""
            return f"{BASE_URL}/negocios/listar?key={TOKEN}&codigo_pipe={codigo_pipe}&empresa={EMPRESA}&pesquisa={ENDPOINT_FUNIL_LOCACAO}"

        def collect_related_locacao_pipelines(self) -> pd.DataFrame:
            """Coleta dados de pipelines relacionados à locação"""
            print("INICIANDO | COLETA DE PIPELINES DE LOCAÇÃO")

            all_data = []

            # Lista de códigos de pipeline comumente usados para locação
            # Pipeline 2 é o principal (conforme endpoint fornecido)
            # Mas vamos testar outros que podem estar relacionados à locação
            locacao_pipeline_codes = ["2", "4", "6", "8"]  # Expandir conforme necessário

            for pipe_code in locacao_pipeline_codes:
                try:
                    url = self.get_api_url(pipe_code)
                    print(f"Testando pipeline de locação {pipe_code}...")

                    # Reset dos contadores para este pipeline específico
                    initial_collected = self.total_collected
                    initial_pages = self.total_pages
                    initial_failed = self.total_failed

                    pipeline_data = self.collect_funil_locacao_with_pagination(url)

                    # Calcula estatísticas específicas deste pipeline
                    pipeline_collected = self.total_collected - initial_collected
                    pipeline_pages = self.total_pages - initial_pages
                    pipeline_failed = self.total_failed - initial_failed

                    if not pipeline_data.empty:
                        print(
                            f"Pipeline {pipe_code}: {len(pipeline_data)} registros | {pipeline_pages} páginas | {pipeline_failed} falhas")
                        all_data.append(pipeline_data)
                    else:
                        print(f"Pipeline {pipe_code}: Nenhum registro encontrado")

                    # Pequena pausa entre pipelines para evitar rate limiting
                    time.sleep(1)

                except Exception as e:
                    logger.error(f"Erro ao coletar pipeline de locação {pipe_code}: {str(e)}")
                    continue

            # Concatena todos os DataFrames
            if all_data:
                final_df = pd.concat(all_data, ignore_index=True)

                # Remove duplicatas baseado no código do negócio (se existir)
                codigo_cols = [col for col in final_df.columns if 'codigo' in col.lower() and 'pipe' not in col.lower()]
                if codigo_cols:
                    initial_count = len(final_df)
                    final_df = final_df.drop_duplicates(subset=codigo_cols[0])
                    removed_duplicates = initial_count - len(final_df)
                    if removed_duplicates > 0:
                        print(f"DUPLICATAS REMOVIDAS | {removed_duplicates} registros duplicados")

                print(f"TOTAL GERAL LOCAÇÃO | {len(final_df)} registros únicos de {len(all_data)} pipelines")
                return final_df
            else:
                print("NENHUM DADO ENCONTRADO em pipelines de locação")
                return pd.DataFrame()

        def generate_locacao_report(self, df: pd.DataFrame):
            """Gera um relatório específico sobre os dados de locação coletados"""
            if df.empty:
                return

            print(f"\n{'=' * 60}")
            print("RELATÓRIO DE LOCAÇÃO")
            print(f"{'=' * 60}")

            # Estatísticas básicas
            print(f"Total de negócios de locação: {len(df)}")

            # Análise por status (se a coluna existir)
            status_cols = [col for col in df.columns if 'status' in col.lower()]
            if status_cols:
                status_col = status_cols[0]
                print(f"\nDistribuição por Status:")
                status_counts = df[status_col].value_counts()
                for status, count in status_counts.items():
                    percentage = (count / len(df)) * 100
                    print(f"  {status}: {count} ({percentage:.1f}%)")

            # Análise por pipeline (se a coluna existir)
            pipe_cols = [col for col in df.columns if 'pipe' in col.lower() and 'nome' in col.lower()]
            if pipe_cols:
                pipe_col = pipe_cols[0]
                print(f"\nDistribuição por Pipeline:")
                pipe_counts = df[pipe_col].value_counts()
                for pipe, count in pipe_counts.items():
                    percentage = (count / len(df)) * 100
                    print(f"  {pipe}: {count} ({percentage:.1f}%)")

            # Análise de valores (se a coluna existir)
            valor_cols = [col for col in df.columns if 'valor' in col.lower()]
            if valor_cols:
                valor_col = valor_cols[0]
                valores_numericos = pd.to_numeric(df[valor_col], errors='coerce').dropna()
                if len(valores_numericos) > 0:
                    print(f"\nAnálise de Valores:")
                    print(f"  Valor médio: R$ {valores_numericos.mean():.2f}")
                    print(f"  Valor mediano: R$ {valores_numericos.median():.2f}")
                    print(f"  Valor mínimo: R$ {valores_numericos.min():.2f}")
                    print(f"  Valor máximo: R$ {valores_numericos.max():.2f}")
                    print(f"  Valor total: R$ {valores_numericos.sum():.2f}")

            print(f"{'=' * 60}\n")

        def run(self):
            """Executa a coleta completa de dados do funil de locação"""
            try:
                print(f"INICIANDO | FunilLocacaoCollector | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

                # Opção 1: Coletar apenas pipeline 2 (como no endpoint fornecido)
                # url = self.get_api_url()
                # funil_df = self.collect_funil_locacao_with_pagination(url)

                # Opção 2: Coletar todos os pipelines relacionados à locação (recomendado)
                funil_df = self.collect_related_locacao_pipelines()

                # Gera relatório específico de locação
                self.generate_locacao_report(funil_df)

                # Salva os dados coletados
                self.save_to_gcs(funil_df)
            except Exception as e:
                logger.error(f"Erro durante a execução: {str(e)}")
                print(f"ERRO | Execução geral | {str(e)}")
                raise
            finally:
                # Exibe resumo
                self.print_summary()

        def print_summary(self):
            """Exibe resumo da execução"""
            execution_time = time.time() - self.start_time
            hours, remainder = divmod(execution_time, 3600)
            minutes, seconds = divmod(remainder, 60)

            print(f"\n{'=' * 80}")
            print(f"RESUMO DA EXECUÇÃO - FUNIL DE LOCAÇÃO")
            print(f"{'=' * 80}")
            print(f"Tempo de execução: {int(hours)}h{int(minutes)}m{int(seconds)}s")
            print(f"Total de registros coletados: {self.total_collected}")
            print(f"Total de páginas processadas: {self.total_pages}")
            print(f"Total de falhas: {self.total_failed}")
            print(
                f"Taxa de sucesso: {((self.total_collected / (self.total_collected + self.total_failed)) * 100):.2f}%" if (
                                                                                                                                      self.total_collected + self.total_failed) > 0 else "N/A")
            print(
                f"Registros por página (média): {(self.total_collected / self.total_pages):.1f}" if self.total_pages > 0 else "N/A")
            print(f"Pipeline principal: {CODIGO_PIPE_LOCACAO} (Locação)")
            print(f"{'=' * 80}\n")

    # START
    print(f"INICIANDO | FunilLocacaoCollector | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    collector = FunilLocacaoCollector()
    collector.run()


def extract_sales_funnel(customer):
    import requests
    import json
    import pandas as pd
    import time
    import logging
    import unicodedata
    import re
    from datetime import datetime
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from typing import Dict, List, Tuple
    from google.cloud import storage
    import random
    import os
    import pathlib

    # Variáveis globais
    BASE_URL = customer['api_base_url']
    TOKEN = customer['api_token']
    EMPRESA = customer['empresa']
    BUCKET_NAME = customer['bucket_name']
    ENDPOINT_FUNIL_VENDAS = dict(customer['endpoint_funil_vendas'])
    FOLDER = "funil_vendas"
    FILENAME = "funil_vendas.csv"
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    MAX_RETRIES, MAX_WORKERS, RECORDS_PER_PAGE = 5, 5, 50

    # Configuração de logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(), logging.FileHandler('funil_vendas_collector.log')]
    )
    logger = logging.getLogger('funil_vendas_collector')

    class FunilVendasCollector:
        """Classe para coleta de dados do Funil de Vendas da API VistaHost"""

        def __init__(self):
            self.session = requests.Session()
            self.session.headers.update({'Accept': 'application/json', 'Content-Type': 'application/json'})
            self.total_collected = 0
            self.total_failed = 0
            self.total_pages = 0
            self.start_time = time.time()
            self.failed_requests = []
            self.processed_urls = set()  # Para evitar duplicação de logs
            self.init_gcs_client()

        def init_gcs_client(self):
            """Inicializa cliente do Google Cloud Storage"""
            try:
                self.storage_client = storage.Client(project=customer['project_id'])
                self.bucket = self.storage_client.bucket(BUCKET_NAME)
                logger.info(f"Cliente GCS inicializado com sucesso para o bucket: {BUCKET_NAME}")
            except Exception as e:
                logger.error(f"Erro ao inicializar cliente GCS: {str(e)}")
                raise

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

        def make_request(self, url: str, retries: int = 0) -> Dict:
            """Faz uma requisição à API com estratégia de backoff exponencial"""
            try:
                response = self.session.get(url, timeout=30)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429 or response.status_code >= 500:
                    if retries < MAX_RETRIES:
                        wait_time = (2 ** retries) + random.uniform(0, 1)
                        logger.info(f"Limitação da API. Aguardando {wait_time:.2f}s")
                        time.sleep(wait_time)
                        return self.make_request(url, retries + 1)
                    else:
                        logger.error(f"Falha após {MAX_RETRIES} tentativas")
                        self.total_failed += 1
                        return {"error": f"Falha após {MAX_RETRIES} tentativas", "url": url}
                else:
                    logger.error(f"Erro na requisição. Status: {response.status_code}")
                    if response.status_code == 401:
                        print('ERRO NA AUTENTICACAO/MONTAGEM DA URL DINAMICA')
                        raise
                    if retries < MAX_RETRIES:
                        time.sleep((2 ** retries) + random.uniform(0, 1))
                        return self.make_request(url, retries + 1)
                    self.total_failed += 1
                    return {"error": f"Erro {response.status_code}", "url": url}
            except Exception as e:
                logger.error(f"Exceção ao fazer requisição: {str(e)}")
                if retries < MAX_RETRIES:
                    time.sleep((2 ** retries) + random.uniform(0, 1))
                    return self.make_request(url, retries + 1)
                self.total_failed += 1
                return {"error": str(e), "url": url}

        def extract_data_from_response(self, response_data: Dict) -> List[Dict]:
            """Extrai dados da resposta da API"""
            if "error" in response_data:
                return []

            # Para o endpoint de negócios, os dados estão em chaves numéricas
            numeric_keys = [k for k in response_data.keys() if k.isdigit()]
            return [response_data[k] for k in numeric_keys]

        def get_page_data(self, base_url: str, page: int) -> Tuple[List[Dict], int, bool]:
            """Obtém os dados de uma página específica - Retorna: (dados, próxima_página, tem_erro)"""
            try:
                # Montar URL para paginação
                start_idx = base_url.find('pesquisa=') + 9
                pesquisa_obj = json.loads(base_url[start_idx:])
                pesquisa_obj['paginacao']['pagina'] = page
                url = base_url[:start_idx] + json.dumps(pesquisa_obj)

                # Criar um ID único para a requisição
                req_id = f"funil_vendas_{page}"

                # Verifica se já processamos esta requisição antes
                if req_id in self.processed_urls:
                    logger.warning(f"Requisição duplicada evitada: {req_id}")
                    return [], 0, True

                self.processed_urls.add(req_id)

                # Fazer a requisição
                response_data = self.make_request(url)

                if "error" in response_data:
                    self.failed_requests.append(url)
                    return [], 0, True

                # Extrai dados da resposta
                data = self.extract_data_from_response(response_data)

                # Atualiza contador de páginas
                self.total_pages += 1

                # Verifica se há mais páginas
                next_page = page + 1 if len(data) >= RECORDS_PER_PAGE else 0

                # Print detalhado para acompanhamento da paginação
                print(
                    f"FUNIL_VENDAS | Pág {page} | +{len(data)} | T:{self.total_collected + len(data)} | P:{self.total_pages} | F:{self.total_failed}")

                return data, next_page, False
            except Exception as e:
                logger.error(f"Erro ao processar página {page}: {str(e)}")
                self.failed_requests.append(base_url)
                self.total_failed += 1
                return [], 0, True

        def collect_funil_vendas_with_pagination(self, base_url: str) -> pd.DataFrame:
            """Coleta todos os dados do funil de vendas com paginação"""
            print("INICIANDO | FUNIL DE VENDAS")

            first_page_data, next_page, has_error = self.get_page_data(base_url, 1)
            all_data = first_page_data
            self.total_collected += len(first_page_data)

            # Coleta páginas adicionais enquanto houver dados
            current_page = 1
            while next_page > 0:
                current_page = next_page
                page_data, next_page, has_error = self.get_page_data(base_url, current_page)
                if has_error or not page_data:
                    logger.warning(f"Parada na paginação devido a erro ou dados vazios na página {current_page}")
                    break
                all_data.extend(page_data)
                self.total_collected += len(page_data)

            # Reprocessa falhas, se houver
            if self.failed_requests:
                self.reprocess_failed_requests(all_data)

            # Converte para DataFrame e normaliza
            if all_data:
                df = pd.DataFrame([self.flatten_json(item) for item in all_data])
                df.columns = [self.normalize_column_name(col) for col in df.columns]
                print(
                    f"CONCLUÍDO | FUNIL_VENDAS | Total {len(df)} | Páginas {self.total_pages} | Falhas {self.total_failed}")
                return df

            print(f"CONCLUÍDO | FUNIL_VENDAS | Total 0 | Páginas {self.total_pages} | Falhas {self.total_failed}")
            return pd.DataFrame()

        def reprocess_failed_requests(self, all_data: List[Dict]):
            """Reprocessa requisições que falharam"""
            if not self.failed_requests:
                return

            failed_count = len(self.failed_requests)
            print(f"REPROCESSANDO | FUNIL_VENDAS | {failed_count} pendentes")
            failed_urls = self.failed_requests.copy()
            self.failed_requests = []

            with ThreadPoolExecutor(max_workers=max(1, MAX_WORKERS // 2)) as executor:
                futures = {executor.submit(self.make_request, url): url for url in failed_urls}
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if "error" not in result:
                            data = self.extract_data_from_response(result)
                            all_data.extend(data)
                            self.total_collected += len(data)
                            self.total_failed -= 1
                    except Exception as e:
                        logger.error(f"Exceção no reprocessamento: {str(e)}")

        def save_to_gcs(self, df: pd.DataFrame):
            """Salva DataFrame como CSV no Google Cloud Storage"""
            if df.empty:
                logger.warning("DataFrame vazio, não será salvo")
                return

            try:
                blob_path = f"{FOLDER}/{FILENAME}"
                blob = self.bucket.blob(blob_path)
                blob.upload_from_string(df.to_csv(sep=';', index=False), content_type='text/csv')
                print(f"SALVO | FUNIL_VENDAS | {len(df)} registros | gs://{BUCKET_NAME}/{blob_path}")
            except Exception as e:
                logger.error(f"Erro ao salvar arquivo: {str(e)}")
                print("ERRO | FUNIL_VENDAS | falha ao salvar")
                raise

        def get_api_url(self) -> str:
            """Retorna a URL da API para o funil de vendas"""
            return f"{BASE_URL}/negocios/listar?key={TOKEN}&codigo_pipe=1&empresa={EMPRESA}&pesquisa={ENDPOINT_FUNIL_VENDAS}"

        def collect_all_pipelines_data(self) -> pd.DataFrame:
            """Coleta dados de todos os pipelines disponíveis"""
            print("INICIANDO | COLETA DE TODOS OS PIPELINES")

            # Primeiro, vamos descobrir quais pipelines existem
            # Para isso, vamos fazer uma chamada inicial e analisar os dados
            all_data = []

            # Lista de códigos de pipeline para testar (pode ser expandida conforme necessário)
            pipeline_codes = ["1", "2", "3", "4", "5"]  # Adicione mais códigos conforme necessário

            for pipe_code in pipeline_codes:
                try:
                    url = f"{BASE_URL}/negocios/listar?key={TOKEN}&codigo_pipe={pipe_code}&empresa={EMPRESA}&pesquisa={ENDPOINT_FUNIL_VENDAS}"

                    print(f"Testando pipeline {pipe_code}...")
                    pipeline_data = self.collect_funil_vendas_with_pagination(url)

                    if not pipeline_data.empty:
                        print(f"Pipeline {pipe_code}: {len(pipeline_data)} registros encontrados")
                        all_data.append(pipeline_data)
                    else:
                        print(f"Pipeline {pipe_code}: Nenhum registro encontrado")

                except Exception as e:
                    logger.error(f"Erro ao coletar pipeline {pipe_code}: {str(e)}")
                    continue

            # Concatena todos os DataFrames
            if all_data:
                final_df = pd.concat(all_data, ignore_index=True)
                print(f"TOTAL GERAL | {len(final_df)} registros de {len(all_data)} pipelines")
                return final_df
            else:
                print("NENHUM DADO ENCONTRADO em nenhum pipeline")
                return pd.DataFrame()

        def run(self):
            """Executa a coleta completa de dados do funil de vendas"""
            try:
                print(f"INICIANDO | FunilVendasCollector | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

                # Opção 1: Coletar apenas pipeline 1 (como no endpoint fornecido)
                # url = self.get_api_url()
                # funil_df = self.collect_funil_vendas_with_pagination(url)

                # Opção 2: Coletar todos os pipelines disponíveis (recomendado)
                funil_df = self.collect_all_pipelines_data()

                # Salva os dados coletados
                self.save_to_gcs(funil_df)

                # Salva também localmente para backup
                if not funil_df.empty:
                    local_filename = f"funil_vendas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                    funil_df.to_csv(local_filename, sep=';', index=False, encoding='utf-8-sig')
                    print(f"BACKUP LOCAL | {local_filename} | {len(funil_df)} registros")

            except Exception as e:
                logger.error(f"Erro durante a execução: {str(e)}")
                print(f"ERRO | Execução geral | {str(e)}")
            finally:
                # Exibe resumo
                self.print_summary()

        def print_summary(self):
            """Exibe resumo da execução"""
            execution_time = time.time() - self.start_time
            hours, remainder = divmod(execution_time, 3600)
            minutes, seconds = divmod(remainder, 60)

            print(f"\n{'=' * 80}")
            print(f"RESUMO DA EXECUÇÃO - FUNIL DE VENDAS")
            print(f"{'=' * 80}")
            print(f"Tempo de execução: {int(hours)}h{int(minutes)}m{int(seconds)}s")
            print(f"Total de registros coletados: {self.total_collected}")
            print(f"Total de páginas processadas: {self.total_pages}")
            print(f"Total de falhas: {self.total_failed}")
            print(
                f"Taxa de sucesso: {((self.total_collected / (self.total_collected + self.total_failed)) * 100):.2f}%" if (
                                                                                                                                      self.total_collected + self.total_failed) > 0 else "N/A")
            print(
                f"Registros por página (média): {(self.total_collected / self.total_pages):.1f}" if self.total_pages > 0 else "N/A")
            print(f"{'=' * 80}\n")

    # START
    print(f"INICIANDO | FunilVendasCollector | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    collector = FunilVendasCollector()
    collector.run()


def extract_real_state(customer):
    import requests
    import json
    import pandas as pd
    import time
    import logging
    import unicodedata
    import re
    import ast
    from datetime import datetime
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from typing import Dict, List, Tuple, Any, Optional
    from google.cloud import storage
    import random
    import os
    import pathlib

    # Variáveis globais
    BASE_URL = customer['api_base_url']
    TOKEN = customer['api_token']
    EMPRESA = customer['empresa']
    BUCKET_NAME = customer['bucket_name']
    PAYLOAD_ENDPOINT_IMOVEIS = dict(customer['payload_endpoint_imoveis'])
    FOLDER = "tabela_imoveis"
    FILENAME = "imoveis.csv"
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    MAX_RETRIES, MAX_WORKERS, RECORDS_PER_PAGE = 5, 5, 50

    # Configuração de logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(), logging.FileHandler('imoveis_collector.log')]
    )
    logger = logging.getLogger('imoveis_collector')

    class ImoveisCollector:
        """Classe para coleta de dados da Tabela de Imóveis da API VistaHost"""

        def __init__(self):
            self.session = requests.Session()
            self.session.headers.update({'Accept': 'application/json', 'Content-Type': 'application/json'})
            self.total_collected = 0
            self.total_failed = 0
            self.total_pages = 0
            self.start_time = time.time()
            self.failed_requests = []
            self.processed_urls = set()
            self.init_gcs_client()

        def init_gcs_client(self):
            """Inicializa cliente do Google Cloud Storage"""
            try:
                self.storage_client = storage.Client(project=customer['project_id'])
                self.bucket = self.storage_client.bucket(BUCKET_NAME)
                logger.info(f"Cliente GCS inicializado com sucesso para o bucket: {BUCKET_NAME}")
            except Exception as e:
                logger.error(f"Erro ao inicializar cliente GCS: {str(e)}")
                raise

        def normalize_column_name(self, name: str) -> str:
            """Normaliza nomes de colunas: remove acentos, converte para lowercase e substitui espaços por underlines"""
            normalized = unicodedata.normalize('NFKD', str(name)).encode('ASCII', 'ignore').decode('utf-8').lower()
            normalized = re.sub(r'[^a-z0-9]', '_', normalized)
            return re.sub(r'_+', '_', normalized).strip('_')

        def parse_field(self, field: Any) -> Optional[Dict]:
            """Parse de campos que podem ser strings, dicts ou outros tipos"""
            if field is None:
                return None
            if isinstance(field, dict):
                return field
            if isinstance(field, str):
                try:
                    return ast.literal_eval(field)
                except:
                    return None
            return None

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

        def process_nested_fields(self, imovel: Dict) -> Dict:
            """Processa campos aninhados específicos (Agencia e Corretor)"""
            processed_imovel = imovel.copy()

            # Tratamento Agencia
            agencia_raw = processed_imovel.get('Agencia')
            agencia_dict = self.parse_field(agencia_raw)
            if agencia_dict:
                # Pega o primeiro valor do dicionário (ex: {'1': {...}} -> {...})
                ag = list(agencia_dict.values())[0]
                for k, v in ag.items():
                    processed_imovel[f"{k.lower()}_agencia"] = v
                del processed_imovel['Agencia']

            # Tratamento Corretor
            corretor_raw = processed_imovel.get('Corretor')
            corretor_dict = self.parse_field(corretor_raw)
            if corretor_dict:
                cr = list(corretor_dict.values())[0]
                for k, v in cr.items():
                    processed_imovel[f"{k.lower()}_corretor"] = v
                del processed_imovel['Corretor']

            return processed_imovel

        def make_request(self, url: str, retries: int = 0) -> Dict:
            """Faz uma requisição à API com estratégia de backoff exponencial"""
            try:
                response = self.session.get(url, timeout=30)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429 or response.status_code >= 500:
                    if retries < MAX_RETRIES:
                        wait_time = (2 ** retries) + random.uniform(0, 1)
                        logger.info(f"Limitação da API. Aguardando {wait_time:.2f}s")
                        time.sleep(wait_time)
                        return self.make_request(url, retries + 1)
                    else:
                        logger.error(f"Falha após {MAX_RETRIES} tentativas")
                        self.total_failed += 1
                        return {"error": f"Falha após {MAX_RETRIES} tentativas", "url": url}
                else:
                    logger.error(f"Erro na requisição. Status: {response.status_code}")
                    if response.status_code == 401:
                        print('ERRO NA AUTENTICACAO/MONTAGEM DA URL DINAMICA')
                        raise
                    if retries < MAX_RETRIES:
                        time.sleep((2 ** retries) + random.uniform(0, 1))
                        return self.make_request(url, retries + 1)
                    self.total_failed += 1
                    return {"error": f"Erro {response.status_code}", "url": url}
            except Exception as e:
                logger.error(f"Exceção ao fazer requisição: {str(e)}")
                if retries < MAX_RETRIES:
                    time.sleep((2 ** retries) + random.uniform(0, 1))
                    return self.make_request(url, retries + 1)
                self.total_failed += 1
                return {"error": str(e), "url": url}

        def get_total_pages(self) -> int:
            """Obtém o total de páginas disponíveis"""
            try:
                pesquisa = json.dumps({
                    "paginacao": {
                        "pagina": 1,
                        "quantidade": RECORDS_PER_PAGE
                    }
                })
                url = f"{BASE_URL}/imoveis/listar?key={TOKEN}&empresa={EMPRESA}&showtotal=1&pesquisa={pesquisa}"

                response_data = self.make_request(url)
                if "error" in response_data:
                    logger.warning("Erro ao obter total de páginas, usando valor padrão")
                    return 100  # Valor padrão de segurança

                total_pages = response_data.get('paginas', 1) + 1
                logger.info(f"Total de páginas detectado: {total_pages}")
                return total_pages

            except Exception as e:
                logger.error(f"Erro ao calcular total de páginas: {str(e)}")
                return 100  # Valor padrão de segurança

        def extract_data_from_response(self, response_data: Dict, page: int) -> List[Dict]:
            """Extrai dados da resposta da API"""
            if "error" in response_data:
                return []

            dados = []
            if isinstance(response_data, dict):
                for id_imovel, imovel in response_data.items():
                    if isinstance(imovel, dict) and id_imovel != 'paginas':  # Ignora campo 'paginas'
                        imovel['IdDinamico'] = id_imovel
                        # Processa campos aninhados
                        processed_imovel = self.process_nested_fields(imovel)
                        dados.append(processed_imovel)
            else:
                logger.warning(f"Resposta inesperada na página {page}: {type(response_data)}")

            return dados

        def get_page_data(self, page: int) -> Tuple[List[Dict], bool]:
            """Obtém os dados de uma página específica - Retorna: (dados, tem_erro)"""
            try:
                url = f"{BASE_URL}/imoveis/listar?key={TOKEN}&empresa={EMPRESA}&showtotal=1&pesquisa={json.dumps(PAYLOAD_ENDPOINT_IMOVEIS)}"

                # Criar um ID único para a requisição
                req_id = f"imoveis_{page}"

                # Verifica se já processamos esta requisição antes
                if req_id in self.processed_urls:
                    logger.warning(f"Requisição duplicada evitada: {req_id}")
                    return [], True

                self.processed_urls.add(req_id)

                # Fazer a requisição
                response_data = self.make_request(url)

                if "error" in response_data:
                    self.failed_requests.append(url)
                    return [], True

                # Extrai dados da resposta
                data = self.extract_data_from_response(response_data, page)

                # Atualiza contador de páginas
                self.total_pages += 1

                # Print detalhado para acompanhamento da paginação
                print(
                    f"IMÓVEIS | Pág {page} | +{len(data)} | T:{self.total_collected + len(data)} | P:{self.total_pages} | F:{self.total_failed}")

                return data, False

            except Exception as e:
                logger.error(f"Erro ao processar página {page}: {str(e)}")
                self.failed_requests.append(f"page_{page}")
                self.total_failed += 1
                return [], True

        def collect_imoveis_with_pagination(self) -> pd.DataFrame:
            """Coleta todos os dados de imóveis com paginação (ordem reversa como no script original)"""
            print("INICIANDO | TABELA DE IMÓVEIS")

            # Obtém total de páginas primeiro
            total_pages = self.get_total_pages()
            all_data = []

            # Coleta páginas em ordem reversa (como no script original)
            for page in range(total_pages, 0, -1):
                page_data, has_error = self.get_page_data(page)

                if has_error:
                    logger.warning(f"Erro na página {page}, continuando...")
                    continue

                if not page_data:
                    logger.info(f"Página {page} vazia, continuando...")
                    continue

                all_data.extend(page_data)
                self.total_collected += len(page_data)

                # Pequena pausa para não sobrecarregar a API
                time.sleep(0.1)

            # Reprocessa falhas
            if self.failed_requests:
                self.reprocess_failed_requests(all_data)

            # Converte para DataFrame e normaliza
            if all_data:
                df = pd.DataFrame([self.flatten_json(item) for item in all_data])
                df.columns = [self.normalize_column_name(col) for col in df.columns]
                print(
                    f"CONCLUÍDO | IMÓVEIS | Total {len(df)} | Páginas {self.total_pages} | Falhas {self.total_failed}")
                return df

            print(f"CONCLUÍDO | IMÓVEIS | Total 0 | Páginas {self.total_pages} | Falhas {self.total_failed}")
            return pd.DataFrame()

        def reprocess_failed_requests(self, all_data: List[Dict]):
            """Reprocessa requisições que falharam"""
            if not self.failed_requests:
                return

            failed_count = len(self.failed_requests)
            print(f"REPROCESSANDO | IMÓVEIS | {failed_count} pendentes")
            failed_urls = self.failed_requests.copy()
            self.failed_requests = []

            with ThreadPoolExecutor(max_workers=max(1, MAX_WORKERS // 2)) as executor:
                futures = {executor.submit(self.make_request, url): url for url in failed_urls if
                           url.startswith('http')}
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if "error" not in result:
                            data = self.extract_data_from_response(result, 0)
                            all_data.extend(data)
                            self.total_collected += len(data)
                            self.total_failed -= 1
                    except Exception as e:
                        logger.error(f"Exceção no reprocessamento: {str(e)}")

        def save_to_gcs(self, df: pd.DataFrame):
            """Salva DataFrame como CSV no Google Cloud Storage"""
            if df.empty:
                logger.warning("DataFrame vazio, não será salvo")
                return

            try:
                blob_path = f"{FOLDER}/{FILENAME}"
                blob = self.bucket.blob(blob_path)
                blob.upload_from_string(df.to_csv(sep=';', index=False), content_type='text/csv')
                print(f"SALVO | IMÓVEIS | {len(df)} registros | gs://{BUCKET_NAME}/{blob_path}")
            except Exception as e:
                logger.error(f"Erro ao salvar arquivo: {str(e)}")
                print("ERRO | IMÓVEIS | falha ao salvar")
                raise

        def generate_imovel_report(self, df: pd.DataFrame):
            """Gera um relatório específico sobre os dados de imóveis coletados"""
            if df.empty:
                return

            print(f"\n{'=' * 70}")
            print("RELATÓRIO DE IMÓVEIS")
            print(f"{'=' * 70}")

            # Estatísticas básicas
            print(f"Total de imóveis: {len(df)}")

            # Análise por tipo de imóvel
            tipo_cols = [col for col in df.columns if 'tipo' in col.lower() and 'imovel' in col.lower()]
            if tipo_cols:
                tipo_col = tipo_cols[0]
                print(f"\nDistribuição por Tipo de Imóvel (Top 10):")
                tipo_counts = df[tipo_col].value_counts()
                for tipo, count in tipo_counts.head(10).items():
                    percentage = (count / len(df)) * 100
                    print(f"  {tipo}: {count} ({percentage:.1f}%)")

            # Análise por finalidade
            finalidade_cols = [col for col in df.columns if 'finalidade' in col.lower()]
            if finalidade_cols:
                finalidade_col = finalidade_cols[0]
                print(f"\nDistribuição por Finalidade:")
                finalidade_counts = df[finalidade_col].value_counts()
                for finalidade, count in finalidade_counts.items():
                    percentage = (count / len(df)) * 100
                    print(f"  {finalidade}: {count} ({percentage:.1f}%)")

            # Análise por situação/status
            situacao_cols = [col for col in df.columns if 'situacao' in col.lower()]
            if situacao_cols:
                situacao_col = situacao_cols[0]
                print(f"\nDistribuição por Situação (Top 10):")
                situacao_counts = df[situacao_col].value_counts()
                for situacao, count in situacao_counts.head(10).items():
                    percentage = (count / len(df)) * 100
                    print(f"  {situacao}: {count} ({percentage:.1f}%)")

            # Análise por UF
            uf_cols = [col for col in df.columns if col.lower() == 'uf']
            if uf_cols:
                uf_col = uf_cols[0]
                print(f"\nDistribuição por UF:")
                uf_counts = df[uf_col].value_counts()
                for uf, count in uf_counts.items():
                    percentage = (count / len(df)) * 100
                    print(f"  {uf}: {count} ({percentage:.1f}%)")

            # Análise por região
            regiao_cols = [col for col in df.columns if 'regiao' in col.lower()]
            if regiao_cols:
                regiao_col = regiao_cols[0]
                print(f"\nDistribuição por Região (Top 10):")
                regiao_counts = df[regiao_col].value_counts()
                for regiao, count in regiao_counts.head(10).items():
                    percentage = (count / len(df)) * 100
                    if pd.notna(regiao) and str(regiao).strip():
                        print(f"  {regiao}: {count} ({percentage:.1f}%)")

            # Análise de valores
            valor_venda_cols = [col for col in df.columns if 'valor' in col.lower() and 'venda' in col.lower()]
            if valor_venda_cols:
                valor_col = valor_venda_cols[0]
                valores_numericos = pd.to_numeric(df[valor_col], errors='coerce').dropna()
                if len(valores_numericos) > 0:
                    print(f"\nAnálise de Valores de Venda:")
                    print(
                        f"  Imóveis com valor: {len(valores_numericos)} ({(len(valores_numericos) / len(df) * 100):.1f}%)")
                    print(f"  Valor médio: R$ {valores_numericos.mean():,.2f}")
                    print(f"  Valor mediano: R$ {valores_numericos.median():,.2f}")
                    print(f"  Valor mínimo: R$ {valores_numericos.min():,.2f}")
                    print(f"  Valor máximo: R$ {valores_numericos.max():,.2f}")

            valor_locacao_cols = [col for col in df.columns if 'valor' in col.lower() and 'locacao' in col.lower()]
            if valor_locacao_cols:
                valor_col = valor_locacao_cols[0]
                valores_numericos = pd.to_numeric(df[valor_col], errors='coerce').dropna()
                if len(valores_numericos) > 0:
                    print(f"\nAnálise de Valores de Locação:")
                    print(
                        f"  Imóveis com valor: {len(valores_numericos)} ({(len(valores_numericos) / len(df) * 100):.1f}%)")
                    print(f"  Valor médio: R$ {valores_numericos.mean():,.2f}")
                    print(f"  Valor mediano: R$ {valores_numericos.median():,.2f}")
                    print(f"  Valor mínimo: R$ {valores_numericos.min():,.2f}")
                    print(f"  Valor máximo: R$ {valores_numericos.max():,.2f}")

            # Análise de completude dos dados
            print(f"\nCompletude dos Dados (Top 15 campos com mais dados):")
            completude = {}
            for col in df.columns:
                non_null_count = df[col].notna().sum()
                completude[col] = (non_null_count / len(df)) * 100

            sorted_completude = sorted(completude.items(), key=lambda x: x[1], reverse=True)
            for col, percentage in sorted_completude[:15]:
                print(f"  {col}: {percentage:.1f}%")

            print(f"{'=' * 70}\n")

        def validate_imovel_data(self, df: pd.DataFrame):
            """Valida a qualidade dos dados de imóveis"""
            if df.empty:
                return

            print(f"\n{'=' * 70}")
            print("VALIDAÇÃO DE QUALIDADE DOS DADOS")
            print(f"{'=' * 70}")

            # Validação de códigos/matrículas
            codigo_cols = [col for col in df.columns if any(term in col.lower() for term in ['codigo', 'matricula'])]
            if codigo_cols:
                for cod_col in codigo_cols[:2]:
                    cod_preenchidos = df[cod_col].notna().sum()
                    print(f"Códigos {cod_col}: {cod_preenchidos} ({(cod_preenchidos / len(df) * 100):.1f}%)")

            # Validação de valores
            valor_cols = [col for col in df.columns if 'valor' in col.lower()]
            if valor_cols:
                for val_col in valor_cols[:2]:
                    val_preenchidos = df[val_col].notna().sum()
                    val_numericos = pd.to_numeric(df[val_col], errors='coerce').notna().sum()
                    print(
                        f"Valores {val_col}: {val_preenchidos} ({(val_preenchidos / len(df) * 100):.1f}%) | Numéricos: {val_numericos} ({(val_numericos / len(df) * 100):.1f}%)")

            # Validação de localização
            loc_cols = [col for col in df.columns if
                        any(term in col.lower() for term in ['uf', 'regiao', 'empreendimento'])]
            if loc_cols:
                for loc_col in loc_cols[:3]:
                    loc_preenchidos = df[loc_col].notna().sum()
                    print(f"Localização {loc_col}: {loc_preenchidos} ({(loc_preenchidos / len(df) * 100):.1f}%)")

            # Validação de datas
            data_cols = [col for col in df.columns if 'data' in col.lower()]
            if data_cols:
                for data_col in data_cols[:2]:
                    data_preenchidas = df[data_col].notna().sum()
                    print(f"Datas {data_col}: {data_preenchidas} ({(data_preenchidas / len(df) * 100):.1f}%)")

            print(f"{'=' * 70}\n")

        def run(self):
            """Executa a coleta completa de dados da tabela de imóveis"""
            try:
                print(f"INICIANDO | ImoveisCollector | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

                # Coleta dados de imóveis
                imoveis_df = self.collect_imoveis_with_pagination()

                # Gera relatórios específicos de imóveis
                self.generate_imovel_report(imoveis_df)
                self.validate_imovel_data(imoveis_df)

                # Salva os dados coletados
                self.save_to_gcs(imoveis_df)
            except Exception as e:
                logger.error(f"Erro durante a execução: {str(e)}")
                print(f"ERRO | Execução geral | {str(e)}")
                raise
            finally:
                # Exibe resumo
                self.print_summary()

        def print_summary(self):
            """Exibe resumo da execução"""
            execution_time = time.time() - self.start_time
            hours, remainder = divmod(execution_time, 3600)
            minutes, seconds = divmod(remainder, 60)

            print(f"\n{'=' * 80}")
            print(f"RESUMO DA EXECUÇÃO - TABELA DE IMÓVEIS")
            print(f"{'=' * 80}")
            print(f"Tempo de execução: {int(hours)}h{int(minutes)}m{int(seconds)}s")
            print(f"Total de registros coletados: {self.total_collected}")
            print(f"Total de páginas processadas: {self.total_pages}")
            print(f"Total de falhas: {self.total_failed}")
            print(
                f"Taxa de sucesso: {((self.total_collected / (self.total_collected + self.total_failed)) * 100):.2f}%" if (
                                                                                                                                      self.total_collected + self.total_failed) > 0 else "N/A")
            print(
                f"Registros por página (média): {(self.total_collected / self.total_pages):.1f}" if self.total_pages > 0 else "N/A")
            print(f"Total de campos coletados: 22 campos + Agencia + Corretor")
            print(f"{'=' * 80}\n")

    # START
    print(f"INICIANDO | ImoveisCollector | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    collector = ImoveisCollector()
    collector.run()


def get_extraction_tasks():
    """
    Get the list of data extraction tasks for Moskit.

    Returns:
        list: List of task configurations
    """
    return [
        # {
        #     'task_id': 'extract_customers',
        #     'python_callable': extract_customers
        # },
        # {
        #     'task_id': 'extract_realtor',
        #     'python_callable': extract_realtor
        # },
        # {
        #     'task_id': 'extract_rental_funnel',
        #     'python_callable': extract_rental_funnel
        # },
        # {
        #     'task_id': 'extract_sales_funnel',
        #     'python_callable': extract_sales_funnel
        # },
        {
            'task_id': 'extract_real_state',
            'python_callable': extract_real_state
        }
    ]
