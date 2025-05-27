"""
Script otimizado para coleta de múltiplos endpoints da API ImoView.
"""
import io
import logging
import pandas as pd
import random
import re
import requests
import threading
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from queue import Queue
from typing import Dict, List, Tuple, Any, Optional, Union

# Configurações globais
CHAVE_API = "hGJta73vJt9IzS3XJzhX83O9PsR2CLE5DMckqymzis8="
SERVICE_ACCOUNT_PATH = r"C:\Users\nicho\Desktop\contas_servico\nalk-app-demo-49a7a06bc8c9.json"
BUCKET_NAME = "imoview-gavea"
MAX_WORKERS = 500
REGISTROS_POR_PAGINA = 20
MAX_TENTATIVAS = 8
BASE_URL = "https://api.imoview.com.br"

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuração de endpoints
ENDPOINTS = {
    "comprador_negocios": {
        "url": f"{BASE_URL}/Comprador/RetornarNegocios",
        "folder": "comprador_negocios",
        "filename": "negocios_comprador_imoview.csv",
        "id_param": "codigoCliente",
        "needs_cliente": True
    },
    "locatario_contratos": {
        "url": f"{BASE_URL}/Locatario/RetornarContratos",
        "folder": "locatario_contratos",
        "filename": "contratos_imoview.csv",
        "id_param": "codigoCliente",
        "needs_cliente": True
    },
    "locatario_imoveis": {
        "url": f"{BASE_URL}/Locatario/RetornarImoveis",
        "folder": "locatario_imoveis",
        "filename": "imoveis_imoview.csv",
        "id_param": "codigoCliente",
        "needs_cliente": True
    },
    "locador_contratos": {
        "url": f"{BASE_URL}/Locador/RetornarContratos",
        "folder": "locador_contratos",
        "filename": "contratos_locador_imoview.csv",
        "id_param": "codigoCliente",
        "needs_cliente": True
    },
    "locador_imoveis": {
        "url": f"{BASE_URL}/Locador/RetornarImoveis",
        "folder": "locador_imoveis",
        "filename": "imoveis_locador_imoview.csv",
        "id_param": "codigoCliente",
        "needs_cliente": True
    },
    "vendedor_imoveis": {
        "url": f"{BASE_URL}/Vendedor/RetornarImoveis",
        "folder": "vendedor_imoveis",
        "filename": "imoveis_vendedor_imoview.csv",
        "id_param": "codigoCliente",
        "needs_cliente": True
    },
    "atendimentos": {
        "url": f"{BASE_URL}/Atendimento/RetornarAtendimentos",
        "folder": "atendimentos",
        "filename": "atendimentos_imoview.csv",
        "id_param": None,
        "needs_cliente": False,
        "combo_params": ["finalidade", "situacao", "fase"],
        "combo_values": {
            "finalidade": [1, 2],
            "situacao": [0, 1, 2, 3],
            "fase": [1, 2, 3, 4, 5, 6]
        }
    },
    "campos_extras": {
        "url": f"{BASE_URL}/Imovel/RetornarCamposExtrasDisponiveis",
        "folder": "campos_extras_imoveis",
        "filename": "campos_extras_imoview.csv",
        "id_param": None,
        "needs_cliente": False,
        "no_pagination": True
    },
    "usuarios": {
        "url": f"{BASE_URL}/Usuario/RetornarTipo3",
        "folder": "listagem_usuarios",
        "filename": "listar_contatos.csv",
        "id_param": None,
        "needs_cliente": False
    }
}


def normalize_column_name(name: str) -> str:
    """Normaliza nome da coluna removendo acentos e caracteres especiais."""
    if not isinstance(name, str):
        return str(name)
    nfkd = unicodedata.normalize('NFKD', name)
    ascii_name = nfkd.encode('ASCII', 'ignore').decode('ASCII')
    cleaned = re.sub(r'[^\w\s]', '', ascii_name)
    return re.sub(r'\s+', '_', cleaned).lower()


def normalize_json_data(data: List[Dict]) -> pd.DataFrame:
    """
    Processa dados JSON com aninhamento recursivo e transforma em DataFrame.
    Garante que todos os dados aninhados, independente da profundidade,
    sejam transformados em colunas.
    """
    if not data:
        return pd.DataFrame()

    # Primeira passagem: normalize o que for possível
    df = pd.json_normalize(data, sep='_')

    # Identifica colunas que ainda têm estruturas aninhadas (dict ou list)
    nested_columns = []
    for col in df.columns:
        # Verifica se a coluna contém dicts ou listas - de forma segura
        try:
            if df[col].apply(lambda x: isinstance(x, (dict, list)) if x is not None else False).any():
                nested_columns.append(col)
        except:
            # Se não conseguir verificar, assume que não é aninhado
            continue

    # Se não houver mais aninhamento, normaliza os nomes e retorna
    if not nested_columns:
        df.columns = [normalize_column_name(col) for col in df.columns]
        return df

    # Processa colunas aninhadas
    for col in nested_columns:
        # Para cada coluna aninhada, extraímos e normalizamos seu conteúdo
        nested_df = pd.DataFrame()

        # Processa cada linha para extrair dados aninhados
        for idx, value in df[col].items():
            # Tratamento seguro para valores nulos ou incompatíveis
            try:
                # Verificações mais seguras para valores nulos ou tipos incompatíveis
                if value is None:
                    continue

                # Para valores que podem ser arrays ou séries pandas
                is_null = False
                try:
                    if hasattr(value, '__len__') and len(value) == 0:
                        is_null = True
                    if hasattr(pd, 'isna') and hasattr(value, 'size') and value.size == 0:
                        is_null = True
                except:
                    pass

                if is_null or not isinstance(value, (dict, list)):
                    continue

                # Converte valor único em lista para processamento uniforme
                if isinstance(value, dict):
                    value = [value]

                if isinstance(value, list) and value:
                    # Normaliza a lista de objetos
                    temp_df = pd.json_normalize(value, sep='_')

                    # Renomeia colunas para indicar a origem
                    temp_df.columns = [f"{col}_{subcol}" for subcol in temp_df.columns]

                    # Se for a primeira linha, inicializa o dataframe
                    if nested_df.empty:
                        nested_df = pd.DataFrame(index=df.index)

                    # Preenche o dataframe com os valores desta linha
                    for nested_col in temp_df.columns:
                        if len(temp_df) == 1:  # Um objeto
                            nested_df.at[idx, nested_col] = temp_df.iloc[0].get(nested_col)
                        else:  # Lista de objetos
                            nested_df.at[idx, nested_col] = temp_df[nested_col].tolist()
            except Exception as e:
                logger.error(f"Erro ao processar valor aninhado para coluna {col}: {str(e)}")
                continue

        # Remove a coluna original e adiciona as novas colunas
        try:
            df = df.drop(columns=[col])

            if not nested_df.empty:
                df = pd.concat([df, nested_df], axis=1)
        except Exception as e:
            logger.error(f"Erro ao concatenar dados aninhados para coluna {col}: {str(e)}")

    # Normaliza todas as colunas finais e retorna
    df.columns = [normalize_column_name(col) for col in df.columns]
    return df


def upload_to_gcs(data: str, destination_blob_name: str) -> None:
    """Envia dados para o Google Cloud Storage."""
    try:
        client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_PATH)
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(data, content_type='text/csv')
        logger.info(f"Arquivo enviado para {destination_blob_name}")
    except Exception as e:
        logger.error(f"Erro ao enviar arquivo para GCS: {e}")
        raise


def get_total_contatos() -> int:
    """Obtém o número total de contatos disponíveis."""
    headers = {"accept": "application/json", "chave": CHAVE_API}
    params = {"numeroPagina": 1, "numeroRegistros": 1}

    for tentativa in range(1, MAX_TENTATIVAS + 1):
        try:
            response = requests.get(
                f"{BASE_URL}/Usuario/RetornarTipo3",
                headers=headers,
                params=params,
                timeout=30
            )
            if response.status_code == 200:
                return response.json().get("quantidade", 0)
            elif response.status_code == 429:  # Rate limit
                wait_time = (2 ** tentativa) + random.uniform(0, 1)
                logger.warning(f"Rate limit atingido. Aguardando {wait_time:.2f}s antes de tentar novamente.")
                time.sleep(wait_time)
            else:
                logger.error(f"Erro ao obter total de contatos: {response.status_code}")
                time.sleep(2)
        except Exception as e:
            logger.error(f"Exceção ao obter total de contatos: {e}")
            time.sleep(2)

    return 0


def fetch_data(endpoint_key: str, page: int = 1, **kwargs) -> List[Dict]:
    """Busca dados de um endpoint específico com tratamento de erros e backoff."""
    endpoint = ENDPOINTS[endpoint_key]
    headers = {"accept": "application/json", "chave": CHAVE_API}

    params = {
        "numeroPagina": page,
        "numeroRegistros": REGISTROS_POR_PAGINA
    }
    params.update(kwargs)

    dados = []

    for tentativa in range(1, MAX_TENTATIVAS + 1):
        try:
            response = requests.get(
                endpoint["url"],
                headers=headers,
                params=params,
                timeout=30
            )

            if response.status_code == 200:
                result = response.json()

                # Alguns endpoints não usam o formato {"lista": [...]}
                if endpoint.get("no_pagination"):
                    if isinstance(result, list):
                        dados = result
                    else:
                        dados = [result]
                    param_info = ", ".join([f"{k}={v}" for k, v in kwargs.items() if k != "numeroRegistros"])
                    logger.info(f"Endpoint {endpoint_key}: Coletados {len(dados)} registros {param_info}")
                    return dados

                lista = result.get("lista", [])

                if not lista and page == 1:
                    param_info = ", ".join([f"{k}={v}" for k, v in kwargs.items() if k != "numeroRegistros"])
                    logger.info(f"Endpoint {endpoint_key}: Sem dados para coletar {param_info}")
                    return []

                param_info = ", ".join([f"{k}={v}" for k, v in kwargs.items() if k != "numeroRegistros"])
                logger.info(f"Endpoint {endpoint_key}: Página {page}, coletados {len(lista)} registros {param_info}")

                return lista

            elif response.status_code == 429:  # Rate limit
                wait_time = (2 ** tentativa) + random.uniform(0, 1)
                logger.warning(
                    f"Rate limit no endpoint {endpoint_key}. Aguardando {wait_time:.2f}s antes de tentar novamente.")
                time.sleep(wait_time)
            else:
                logger.error(
                    f"Erro na requisição do endpoint {endpoint_key}: {response.status_code}. Tentativa {tentativa}")
                time.sleep(2)

        except Exception as e:
            logger.error(f"Exceção na requisição do endpoint {endpoint_key}: {e}. Tentativa {tentativa}")
            time.sleep(2)

    logger.error(f"Falha após {MAX_TENTATIVAS} tentativas para endpoint {endpoint_key} com parâmetros {params}")
    return []


def process_endpoint(endpoint_key: str) -> None:
    """Processa um endpoint específico e salva os dados coletados."""
    endpoint = ENDPOINTS[endpoint_key]
    start_time = time.time()

    # Determina o tipo de coleta baseado na configuração do endpoint
    if endpoint.get("no_pagination"):
        # Endpoints sem paginação (como campos extras)
        todos_dados = fetch_data(endpoint_key)
    elif endpoint.get("combo_params"):
        # Endpoints com combinações de parâmetros (como atendimentos)
        todos_dados = process_combo_endpoint(endpoint_key)
    elif endpoint.get("needs_cliente"):
        # Endpoints que precisam de ID de cliente (como contratos, imóveis)
        todos_dados = process_client_based_endpoint(endpoint_key)
    else:
        # Endpoints com paginação simples (como usuários)
        todos_dados = process_paginated_endpoint(endpoint_key)

    if not todos_dados:
        logger.warning(f"Nenhum dado coletado para o endpoint {endpoint_key}")
        return

    try:
        # Processa e salva os dados - lidando com aninhamento de forma recursiva
        logger.info(f"Normalizando {len(todos_dados)} registros do endpoint {endpoint_key}")

        # Para endpoints com muitos registros, processamos em lotes para evitar problemas de memória
        if len(todos_dados) > 10000:
            logger.info(f"Processando em lotes devido ao grande volume de dados: {len(todos_dados)} registros")
            batch_size = 5000
            all_dfs = []

            for i in range(0, len(todos_dados), batch_size):
                batch = todos_dados[i:i + batch_size]
                logger.info(f"Processando lote {i // batch_size + 1} com {len(batch)} registros")
                batch_df = normalize_json_data(batch)
                all_dfs.append(batch_df)

            # Combina todos os DataFrames
            df = pd.concat(all_dfs, ignore_index=True)
        else:
            df = normalize_json_data(todos_dados)

        logger.info(f"Salvando {len(df)} linhas e {len(df.columns)} colunas para o endpoint {endpoint_key}")
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, sep=';', index=False, encoding='utf-8-sig')

        destination = f"{endpoint['folder']}/{endpoint['filename']}"
        upload_to_gcs(csv_buffer.getvalue(), destination)

        elapsed_time = time.time() - start_time
        logger.info(
            f"Endpoint {endpoint_key} processado em {elapsed_time:.2f} segundos. Total de registros: {len(todos_dados)}")
    except Exception as e:
        logger.error(f"Erro ao processar endpoint {endpoint_key}: {str(e)}")
        # Salvamos os dados brutos em caso de erro para não perder o trabalho
        try:
            # Tenta salvar os dados brutos em CSV
            logger.info(f"Tentando salvar dados brutos sem processamento para {endpoint_key}")
            # Cria um DataFrame simples sem processamento
            raw_df = pd.DataFrame(todos_dados)
            # Remove colunas que possam conter objetos complexos
            for col in raw_df.columns:
                if raw_df[col].apply(lambda x: isinstance(x, (dict, list))).any():
                    raw_df[col] = raw_df[col].apply(lambda x: str(x) if isinstance(x, (dict, list)) else x)

            csv_buffer = io.StringIO()
            raw_df.to_csv(csv_buffer, sep=';', index=False, encoding='utf-8-sig')

            backup_destination = f"{endpoint['folder']}/backup_{endpoint['filename']}"
            upload_to_gcs(csv_buffer.getvalue(), backup_destination)
            logger.info(f"Backup dos dados brutos salvo em {backup_destination}")
        except Exception as backup_error:
            logger.error(f"Erro ao salvar backup dos dados: {str(backup_error)}")


def process_paginated_endpoint(endpoint_key: str) -> List[Dict]:
    """Processa endpoint com paginação simples."""
    todos_dados = []
    page = 1

    while True:
        dados_pagina = fetch_data(endpoint_key, page)
        if not dados_pagina:
            break

        todos_dados.extend(dados_pagina)

        if len(dados_pagina) < REGISTROS_POR_PAGINA:
            break

        page += 1

    return todos_dados


def process_combo_endpoint(endpoint_key: str) -> List[Dict]:
    """Processa endpoint com combinações de parâmetros usando paralelismo."""
    endpoint = ENDPOINTS[endpoint_key]
    todos_dados = []
    combinacoes_queue = Queue()

    # Mutex para proteger o acesso à lista todos_dados
    dados_lock = threading.Lock()

    # Gera todas as combinações possíveis
    combo_params = endpoint["combo_params"]
    combo_values = endpoint["combo_values"]

    for val1 in combo_values[combo_params[0]]:
        for val2 in combo_values[combo_params[1]]:
            for val3 in combo_values[combo_params[2]]:
                combinacoes_queue.put({
                    combo_params[0]: val1,
                    combo_params[1]: val2,
                    combo_params[2]: val3
                })

    def worker():
        while not combinacoes_queue.empty():
            try:
                combo = combinacoes_queue.get()
                page = 1
                combo_dados = []

                while True:
                    dados_pagina = fetch_data(endpoint_key, page, **combo)
                    if not dados_pagina:
                        break

                    combo_dados.extend(dados_pagina)

                    if len(dados_pagina) < REGISTROS_POR_PAGINA:
                        break

                    page += 1

                if combo_dados:
                    with dados_lock:
                        todos_dados.extend(combo_dados)

            except Exception as e:
                logger.error(f"Erro no worker para endpoint {endpoint_key}: {e}")
            finally:
                combinacoes_queue.task_done()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(worker) for _ in range(min(MAX_WORKERS, combinacoes_queue.qsize()))]

        for future in futures:
            try:
                future.result()
            except Exception as e:
                logger.error(f"Erro em future para endpoint {endpoint_key}: {e}")

    return todos_dados


def process_client_based_endpoint(endpoint_key: str) -> List[Dict]:
    """Processa endpoint baseado em ID de cliente usando paralelismo."""
    total_contatos = get_total_contatos()
    logger.info(f"Total de contatos encontrados: {total_contatos}")

    if total_contatos == 0:
        return []

    todos_dados = []
    codigos_queue = Queue()
    endpoint = ENDPOINTS[endpoint_key]

    # Mutex para proteger o acesso à lista todos_dados
    dados_lock = threading.Lock()

    for codigo in range(1, total_contatos + 1):
        codigos_queue.put(codigo)

    def worker():
        while not codigos_queue.empty():
            try:
                codigo_cliente = codigos_queue.get()
                page = 1
                cliente_dados = []

                while True:
                    dados_pagina = fetch_data(
                        endpoint_key,
                        page,
                        **{endpoint["id_param"]: codigo_cliente}
                    )

                    if not dados_pagina:
                        break

                    cliente_dados.extend(dados_pagina)

                    if len(dados_pagina) < REGISTROS_POR_PAGINA:
                        break

                    page += 1

                if cliente_dados:
                    with dados_lock:
                        todos_dados.extend(cliente_dados)

            except Exception as e:
                logger.error(f"Erro no worker para cliente {codigo_cliente}, endpoint {endpoint_key}: {e}")
                # Se houver erro, recoloca na fila para tentar novamente
                codigos_queue.put(codigo_cliente)
            finally:
                codigos_queue.task_done()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(worker) for _ in range(min(MAX_WORKERS, codigos_queue.qsize()))]

        for future in futures:
            try:
                future.result()
            except Exception as e:
                logger.error(f"Erro em future para endpoint {endpoint_key}: {e}")

    return todos_dados


def main() -> None:
    """Função principal que coordena o processamento de todos os endpoints."""
    total_start_time = time.time()

    logger.info("Iniciando coleta de dados da API ImoView")

    for endpoint_key in ENDPOINTS:
        process_endpoint(endpoint_key)

    total_elapsed_time = time.time() - total_start_time
    logger.info(f"Tempo total de execução: {total_elapsed_time:.2f} segundos")


if __name__ == "__main__":
    main()
