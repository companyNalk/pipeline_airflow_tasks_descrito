import csv
import glob
import logging
import os
import re
import shutil
import time
import unicodedata
import uuid
from typing import Dict, Any, List, Callable

import pandas as pd

# Configuração básica do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Utils:
    """Classe utilitária para processamento de dados e operações com arquivos."""

    @staticmethod
    def _flatten_json(data: Any, parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
        """
        Achata estruturas JSON aninhadas em um dicionário de nível único.
        """
        try:
            items = []

            if isinstance(data, dict):
                for key, value in data.items():
                    new_key = f"{parent_key}{sep}{key}" if parent_key else key
                    items.extend(Utils._flatten_json(value, new_key, sep).items())
            elif isinstance(data, list):
                for index, item in enumerate(data):
                    new_key = f"{parent_key}{sep}{index + 1}"
                    items.extend(Utils._flatten_json(item, new_key, sep).items())
            else:
                items.append((parent_key, data))

            return dict(items)

        except Exception as e:
            logging.error(f"Erro ao achatar JSON: {str(e)}")
            raise

    @staticmethod
    def _remove_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove colunas vazias (NaN ou strings vazias) de um DataFrame.
        """
        try:
            if df.empty:
                logging.warning("DataFrame vazio recebido para remoção de colunas")
                return df

            # Número inicial de colunas
            initial_columns = df.shape[1]

            # Remover colunas onde todos os valores são NaN
            df_no_nan = df.dropna(axis=1, how='all')
            nan_columns_removed = initial_columns - df_no_nan.shape[1]

            if nan_columns_removed > 0:
                logging.info(f"Colunas removidas por serem NaN: {nan_columns_removed}")

            # Identificar e remover colunas onde todos os valores são strings vazias
            empty_columns = [
                col for col in df_no_nan.columns
                if df_no_nan[col].astype(str).str.strip().eq('').all()
            ]

            if empty_columns:
                df_cleaned = df_no_nan.drop(columns=empty_columns)
                logging.info(f"Colunas removidas por serem strings vazias: {len(empty_columns)}")
            else:
                df_cleaned = df_no_nan

            # Total de colunas removidas
            total_columns_removed = nan_columns_removed + len(empty_columns)

            if total_columns_removed > 0:
                logging.info(f"Total de colunas removidas: {total_columns_removed}")

            # Verificar se o DataFrame resultante está vazio
            if df_cleaned.empty and not df.empty:
                raise ValueError("O DataFrame resultante está vazio após a remoção de colunas")

            return df_cleaned

        except Exception as e:
            logging.error(f"Erro ao remover colunas vazias: {str(e)}")
            raise

    @staticmethod
    def _normalize_key(key: str, use_pascal_case: bool = False) -> str:
        """
        Normaliza uma chave de string aplicando transformações para padronização.
        """
        key = key.strip('\'"')

        # Detecta se é um camelCase com a primeira letra maiúscula (PascalCase)
        # is_pascal_case = bool(re.match(r'^[A-Z]', key) and re.search(r'[a-z]', key))

        # Detecta se é um verdadeiro camelCase (inicia com minúscula, depois tem maiúscula)
        is_true_camel_case = bool(re.match(r'^[a-z].*[A-Z]', key))

        # Converte PascalCase para snake_case (ex: ExibeContato -> exibe_contato)
        if use_pascal_case:
            # Insere underscore antes de cada letra maiúscula, exceto a primeira
            key = re.sub(r'(?<!^)([A-Z][a-z])', r'_\1', key)
            # Trata seguências de maiúsculas como siglas (ex: ID em IdStatusGestao)
            key = re.sub(r'([A-Z])([A-Z][a-z])', r'\1_\2', key)
        # Se for camelCase, aplica a conversão para snake_case antes
        elif is_true_camel_case:
            key = re.sub(r'([a-z])([A-Z])', r'\1_\2', key)

        # Converte para lowercase
        key = key.lower()

        # Remove acentos e outros caracteres Unicode
        key = unicodedata.normalize('NFKD', key).encode('ASCII', 'ignore').decode('ASCII')

        # Substitui caracteres especiais e espaços por underscores
        key = re.sub(r'[^a-z0-9]+', '_', key)

        # Remove underscores extras no início ou fim
        key = key.strip('_')

        return key

    @staticmethod
    def _normalize_keys(data: Any) -> Any:
        """
        Normaliza todas as chaves em estruturas aninhadas (dict/list).
        """
        try:
            if isinstance(data, dict):
                # Normaliza as chaves do dicionário
                return {Utils._normalize_key(k): Utils._normalize_keys(v) for k, v in data.items()}
            elif isinstance(data, list):
                # Normaliza cada item na lista
                return [Utils._normalize_keys(item) for item in data]
            else:
                # Retorna o valor original se não for um dicionário ou lista
                return data

        except Exception as e:
            logging.error(f"Erro ao normalizar chaves: {str(e)}")
            raise

    @staticmethod
    def _normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza os nomes das colunas de um DataFrame.
        """
        try:
            if df.empty:
                return df

            # Normalização dos nomes das colunas
            df.columns = [Utils._normalize_key(col) for col in df.columns]
            return df

        except Exception as e:
            logging.error(f"Erro ao normalizar nomes de colunas: {str(e)}")
            raise

    @staticmethod
    def _save_local_dataframe(df: pd.DataFrame, file_name: str, separator: str = ";") -> bool:
        """
        Salva um DataFrame como arquivo CSV no diretório 'output/'.
        """
        try:
            # Diretório base de saída
            base_output_dir = "output"
            os.makedirs(base_output_dir, exist_ok=True)

            # Diretório específico
            output_dir = os.path.join(base_output_dir, file_name)
            os.makedirs(output_dir, exist_ok=True)

            csv_filename = os.path.join(output_dir, f"{file_name}.csv")

            # Salvar o arquivo
            df.to_csv(csv_filename, sep=separator, index=False)
            logging.info(f"Arquivo salvo com sucesso: {csv_filename}")
            return True

        except Exception as e:
            logging.error(f"Erro ao salvar DataFrame como CSV: {str(e)}", exc_info=True)
            raise

    @staticmethod
    def _convert_columns_to_nullable_int(df: pd.DataFrame) -> pd.DataFrame:
        """
        Converte colunas do DataFrame para Int64 somente se:
        - Todos os valores não nulos forem inteiros "puros" (sem ponto decimal, ex: 7 e não 7.0)
        - E não houver valores float com casas decimais

        Mantém colunas como float se houver valores como 7.0 explicitamente.
        """
        try:
            converted_cols = []
            for col in df.columns:
                if pd.api.types.is_numeric_dtype(df[col]):
                    non_null_series = df[col].dropna()

                    # Se todos os valores forem inteiros puros (ex: 7, não 7.0)
                    if non_null_series.apply(
                            lambda x: isinstance(x, int) or (isinstance(x, float) and x.is_integer())).all():
                        # Se a coluna NÃO tiver valores como 7.0 explicitamente
                        if not non_null_series.apply(lambda x: isinstance(x, float) and not x.is_integer()).any():
                            df[col] = df[col].astype("Int64")
                            converted_cols.append(col)

            if converted_cols:
                preview = ', '.join(converted_cols[:5])
                suffix = "..." if len(converted_cols) > 5 else ""
                logging.info(f"Colunas convertidas para Int64: {preview}{suffix}")
            return df

        except Exception as e:
            logging.error(f"Erro ao converter colunas numericas para Int64: {str(e)}")
            raise

    @staticmethod
    def _remove_newlines_from_fields(data: List[Dict]) -> List[Dict]:
        """
        Remove quebras de linha (\n, \r ou \r\n) de todos os campos de texto.
        """
        if not data:
            return data

        processed_data = []

        for item in data:
            processed_item = {}

            for key, value in item.items():
                # Processar apenas valores string
                if isinstance(value, str):
                    # Substituir quebras de linha por espaços
                    processed_item[key] = value.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ')
                else:
                    processed_item[key] = value

            processed_data.append(processed_item)

        return processed_data

    @staticmethod
    def _process_and_convert_id_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        Verifica as colunas que possuem 'id' ou '_id' no nome e, caso TODOS os valores sejam float,
        vazios ou nulos, converte esses valores para inteiros.
        """
        converted_columns = []

        for col in df.columns:
            if 'id' in col.lower():  # Verifica se o nome da coluna contém 'id' ou '_id'
                try:
                    # Se for UUID, mantenha como string
                    if df[col].apply(lambda x: isinstance(x, uuid.UUID) if pd.notnull(x) else False).any():
                        continue

                    # Verificar se TODOS os valores são nulos
                    if df[col].isnull().all():
                        df[col] = df[col].astype('Int64')
                        converted_columns.append(col)
                        logging.info(f"Coluna {col} convertida para Int64")  # ← Logging individual
                        continue

                    # Verificar se TODOS os valores não-nulos são float
                    non_null_mask = df[col].notnull()
                    if non_null_mask.any():  # Se há valores não-nulos
                        non_null_values = df[col][non_null_mask]

                        # TODOS os valores não-nulos devem ser float
                        if non_null_values.apply(lambda x: isinstance(x, float)).all():
                            # Verificar se TODOS podem ser convertidos para int sem perda
                            # (ou seja, não têm parte decimal significativa)
                            if (non_null_values % 1 == 0).all():
                                # Caso contrário, converta para inteiro
                                df[col] = df[col].fillna(0).astype('Int64')
                                converted_columns.append(col)
                                logging.info(f"Coluna {col} convertida para Int64")  # ← Logging individual
                            else:
                                logging.warning(f"Coluna {col} NÃO convertida: contém valores decimais significativos")
                        else:
                            logging.debug(f"Coluna {col} NÃO convertida: nem todos os valores são float")

                except Exception as e:
                    logging.error(f"Erro ao processar coluna {col}: {str(e)}")
                    # Continuar sem converter esta coluna
                    continue

        return df

    @staticmethod
    def _validate_csv(endpoint_name: str, delimiter: str = ';') -> Dict:
        """
        Valida o CSV e retorna relatório detalhado.
        """
        file_path = f"output/{endpoint_name}/{endpoint_name}.csv"

        validation_result = {
            'valid': False,
            'total_lines': 0,
            'header_columns': 0,
            'errors': [],
            'file_path': file_path
        }

        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                reader = csv.reader(file, delimiter=delimiter)

                header = next(reader)
                validation_result['header_columns'] = len(header)

                errors = []
                total_lines = 1

                for line_num, row in enumerate(reader, start=2):
                    total_lines += 1
                    row_count = len(row)

                    if row_count != len(header):
                        errors.append({
                            'line': line_num,
                            'found_columns': row_count,
                            'expected_columns': len(header)
                        })

                validation_result['total_lines'] = total_lines
                validation_result['errors'] = errors
                validation_result['valid'] = len(errors) == 0

                # Log do resultado
                if validation_result['valid']:
                    logging.info(f"✅ CSV {endpoint_name}: {total_lines} linhas válidas")
                else:
                    logging.error(f"❌ CSV {endpoint_name}: {len(errors)} linhas inválidas de {total_lines}")

            return validation_result

        except Exception as e:
            logging.error(f"Erro ao validar CSV {endpoint_name}: {str(e)}")
            validation_result['errors'].append(f"Erro de leitura: {str(e)}")
            raise

    @staticmethod
    def process_and_save_data(raw_data: List[Dict], endpoint_name: str) -> List[Dict]:
        """
        Processa dados brutos e salva o resultado como CSV.
        """
        if not raw_data:
            logging.warning(f"Nenhum dado recebido para o endpoint {endpoint_name}")
            return []

        try:
            # Normalizar e processar dados
            logging.info(f"Processando {len(raw_data)} registros do endpoint {endpoint_name}")

            # Remover quebra de linhas
            raw_data = Utils._remove_newlines_from_fields(raw_data)

            # Normalizar as chaves dos dados
            normalized_data = Utils._normalize_keys(raw_data)

            # Achatar a estrutura JSON
            flattened_data = [Utils._flatten_json(item) for item in normalized_data]

            # Converter para DataFrame
            df = pd.DataFrame(flattened_data)

            if df.empty:
                logging.warning(f"DataFrame vazio após processamento para {endpoint_name}")
                return []

            # Limpar e normalizar o DataFrame
            df = Utils._convert_columns_to_nullable_int(df)
            df = Utils._normalize_column_names(df)
            df = Utils._remove_empty_columns(df)

            # Verificar e converter colunas de ID
            df = Utils._process_and_convert_id_columns(df)

            # Salvar localmente
            Utils._save_local_dataframe(df, endpoint_name)

            # Verificar validade do csv
            Utils._validate_csv(endpoint_name, delimiter=';')

            logging.info(f"Processamento concluído: {len(df)} registros para {endpoint_name}")

            # Retornar os dados processados
            return df.to_dict(orient='records')

        except Exception as e:
            logging.error(f"Erro no processamento de dados para {endpoint_name}: {str(e)}")
            raise

    @staticmethod
    def _get_chunks_dir(endpoint_name: str) -> str:
        """
        Retorna o diretório para armazenar chunks temporários.
        """
        base_output_dir = "output"
        output_dir = os.path.join(base_output_dir, endpoint_name)
        chunks_dir = os.path.join(output_dir, "chunks")
        os.makedirs(chunks_dir, exist_ok=True)
        return chunks_dir

    @staticmethod
    def _cleanup_chunks_directory(endpoint_name: str) -> bool:
        """
        Remove o diretório de chunks temporários após o processamento bem-sucedido.
        """
        try:
            chunks_dir = Utils._get_chunks_dir(endpoint_name)

            # Verificar se o diretório existe
            if os.path.exists(chunks_dir):
                # Remover o diretório e todo seu conteúdo
                shutil.rmtree(chunks_dir)
                logging.info(f"🗑 Diretório de chunks removido com sucesso: {chunks_dir}")
                return True
            else:
                logging.warning(f"Diretório de chunks não encontrado: {chunks_dir}")
                return False
        except Exception as e:
            logging.error(f"❌ Erro ao remover diretório de chunks para {endpoint_name}: {str(e)}")
            return False

    @staticmethod
    def process_and_save_data_in_chunks(raw_data: List[Dict], endpoint_name: str, chunk_size: int = 1000,
                                        save_to_disk: bool = True, batch_id: int = 0,
                                        skip_empty_columns: bool = False) -> List[Dict]:
        """
        Processa dados brutos em chunks e salva cada chunk como um arquivo separado.
        """
        if not raw_data:
            logging.warning(f"Nenhum dado recebido para o endpoint {endpoint_name}")
            return []

        try:
            # Normalizar e processar dados
            logging.info(f"Processando {len(raw_data)} registros do endpoint {endpoint_name} em chunks de {chunk_size}")

            # Normalizar as chaves dos dados
            normalized_data = Utils._normalize_keys(raw_data)

            # Achatar a estrutura JSON
            flattened_data = [Utils._flatten_json(item) for item in normalized_data]

            # Converter para DataFrame
            df = pd.DataFrame(flattened_data)

            if df.empty:
                logging.warning(f"DataFrame vazio após processamento para {endpoint_name}")
                return []

            # Limpar e normalizar o DataFrame
            df = Utils._normalize_column_names(df)

            # Remoção de colunas vazias é opcional
            if skip_empty_columns:
                df = Utils._remove_empty_columns(df)

            df = Utils._convert_columns_to_nullable_int(df)

            # Se não precisar salvar em disco, retornar os dados processados imediatamente
            if not save_to_disk:
                logging.info(
                    f"Processamento concluído: {len(df)} registros para {endpoint_name} (mantidos apenas em memória)")
                return df.to_dict(orient='records')

            # Preparar para salvar em disco como chunks separados
            chunks_dir = Utils._get_chunks_dir(endpoint_name)

            # Determinar o número total de chunks
            total_rows = len(df)
            num_chunks = (total_rows + chunk_size - 1) // chunk_size  # Arredonda para cima

            # Processar cada chunk e salvar em arquivos separados
            for i in range(num_chunks):
                start_idx = i * chunk_size
                end_idx = min((i + 1) * chunk_size, total_rows)
                chunk_df = df.iloc[start_idx:end_idx].copy()

                # Nome do arquivo com identificador de lote e chunk
                chunk_filename = os.path.join(chunks_dir, f"batch_{batch_id}_chunk_{i}.csv")

                # Salvar o chunk com header sempre incluído
                chunk_df.to_csv(chunk_filename, sep=";", index=False, header=True)

                logging.info(
                    f"Batch {batch_id}, Chunk {i + 1}/{num_chunks}: Salvos {len(chunk_df)} registros em {chunk_filename}")

            logging.info(f"Processamento concluído: {total_rows} registros salvos em chunks separados")

            # Retornar os registros processados
            return df.to_dict(orient='records')

        except Exception as e:
            logging.error(f"Erro no processamento de dados em chunks para {endpoint_name}: {str(e)}")
            raise

    @staticmethod
    def merge_chunks_and_normalize(endpoint_name: str) -> bool:
        """
        Mescla todos os chunks em um único arquivo CSV normalizado.
        """
        try:
            base_output_dir = "output"
            output_dir = os.path.join(base_output_dir, endpoint_name)
            chunks_dir = Utils._get_chunks_dir(endpoint_name)
            final_csv = os.path.join(output_dir, f"{endpoint_name}.csv")

            # Obter lista de todos os chunks
            chunk_files = glob.glob(os.path.join(chunks_dir, "*.csv"))

            if not chunk_files:
                logging.warning(f"Nenhum arquivo chunk encontrado para {endpoint_name}")
                return False

            start_time = time.time()
            logging.info(f"💾 Iniciando mesclagem e normalização de chunks para: {endpoint_name}")

            # Primeiro vamos descobrir todas as colunas possíveis
            logging.info(f"🔍 Analisando estrutura de colunas em {len(chunk_files)} chunks...")
            all_columns = set()
            for chunk_file in chunk_files:
                try:
                    # Leitura apenas dos cabeçalhos para descobrir colunas
                    chunk_header = pd.read_csv(chunk_file, sep=";", nrows=0)
                    all_columns.update(chunk_header.columns)
                except Exception as e:
                    logging.warning(f"Erro ao ler cabeçalho do chunk {chunk_file}: {str(e)}")

            if not all_columns:
                logging.warning(f"Não foi possível extrair colunas dos chunks para {endpoint_name}")
                return False

            # Ordenar colunas para garantir consistência
            all_columns_sorted = sorted(all_columns)
            logging.info(f"✓ Identificadas {len(all_columns)} colunas únicas nos chunks")

            # Inicializar dataframe principal
            main_df = pd.DataFrame(columns=all_columns_sorted)
            total_records = 0

            # Iniciar processamento com barra de progresso no log
            chunk_count = len(chunk_files)
            progress_interval = max(1, min(10, chunk_count // 10))  # Mostra no máximo 10 atualizações de progresso

            # Processar cada chunk e adicionar ao dataframe principal
            for chunk_idx, chunk_file in enumerate(chunk_files):
                try:
                    # Ler o chunk
                    chunk_df = pd.read_csv(chunk_file, sep=";")

                    # Resolver o aviso de Performance: Criar dicionário de colunas faltantes
                    missing_columns = {}
                    for col in all_columns_sorted:
                        if col not in chunk_df.columns:
                            missing_columns[col] = [None] * len(chunk_df)

                    # Adicionar colunas ausentes de uma só vez usando concat (mais eficiente)
                    if missing_columns:
                        missing_df = pd.DataFrame(missing_columns)
                        chunk_df = pd.concat([chunk_df, missing_df], axis=1)

                    # Reordenar colunas para garantir consistência
                    chunk_df = chunk_df[all_columns_sorted]

                    # Estratégia para evitar o warning do pandas
                    if chunk_idx == 0:
                        main_df = chunk_df.copy()
                    else:
                        # Evitar o warning do pandas usando dtypes explícitos
                        for col in main_df.columns:
                            # Garantir que as colunas tenham o mesmo dtype antes da concatenação
                            col_dtype = main_df[col].dtype
                            if col in chunk_df:
                                # Só convertemos se necessário para evitar operações caras
                                if chunk_df[col].dtype != col_dtype:
                                    try:
                                        chunk_df[col] = chunk_df[col].astype(col_dtype)
                                    except Exception:
                                        # Se a conversão falhar, não tem problema
                                        pass

                        # Usar concat com ignore_index=True para evitar problemas com índices duplicados
                        main_df = pd.concat([main_df, chunk_df], ignore_index=True, sort=False)

                    total_records += len(chunk_df)

                    # Log de progresso simplificado
                    show_progress = (chunk_idx % progress_interval == 0) or (chunk_idx == 0) or (chunk_idx == chunk_count - 1)
                    if show_progress:
                        progress_pct = (chunk_idx + 1) / chunk_count * 100
                        logging.info(
                            f"Progresso: {progress_pct:.1f}% | Chunk: {chunk_idx + 1}/{chunk_count} | Registros: {total_records}")

                except Exception as e:
                    logging.error(f"Erro ao processar chunk {os.path.basename(chunk_file)}: {str(e)}")

            # Se não temos dados após o processamento, sair com erro
            if main_df.empty:
                logging.warning(f"DataFrame mesclado está vazio após processamento para {endpoint_name}")
                logging.info(f"| {endpoint_name:<15} | {'❌ Falha':<25} | {0:<10} | {0:<10.2f} |")
                logging.info(f"{'--' * 45}")
                return False

            # Iniciar pós-processamento
            logging.info("{'#' * 100}")
            logging.info("📋 RESUMO DO PÓS-PROCESSAMENTO:")
            logging.info("{'#' * 100}")

            # Identificar e remover colunas vazias
            empty_cols = [col for col in main_df.columns if main_df[col].isna().all() or (main_df[col].astype(str).str.strip().eq('').all())]  # noqa

            if empty_cols:
                main_df = main_df.drop(columns=empty_cols)
                logging.info(f"- Total registros: {len(main_df)}")
                logging.info(f"- Colunas originais: {len(main_df.columns) + len(empty_cols)}")
                logging.info(f"- Colunas removidas: {len(empty_cols)}")
                logging.info(f"- Colunas finais: {len(main_df.columns)}")
            else:
                logging.info(f"- Total registros: {len(main_df)}")
                logging.info(f"- Colunas: {len(main_df.columns)}")

            # Converter colunas para inteiros quando possível
            numeric_cols = []
            for col in main_df.columns:
                try:
                    # Verificar se a coluna parece ser numérica
                    if pd.to_numeric(main_df[col], errors='coerce').notna().all():
                        # Se todas as entradas não-NA podem ser convertidas para inteiros
                        if pd.to_numeric(main_df[col], errors='coerce').dropna().apply(lambda x: x.is_integer()).all():
                            main_df[col] = pd.to_numeric(main_df[col], errors='coerce').astype('Int64')
                            numeric_cols.append(col)
                except Exception:
                    pass  # Ignorar erros e manter a coluna como está

            # Salvar o resultado final
            main_df.to_csv(final_csv, sep=";", index=False)

            # Estatísticas finais
            duration = time.time() - start_time
            logging.info(f"{'-' * 100}")
            logging.info(f"| {endpoint_name:<15} | {'✓ Sucesso':<25} | {len(main_df):<10} | {duration:<10.2f} |")
            logging.info(f"{'-' * 100}")
            logging.info(f"✓ {endpoint_name}: {len(main_df)} registros em {duration:.2f}s")
            logging.info(f"{'#' * 90}")

            return True
        except Exception as e:
            logging.error(f"❌ Erro ao mesclar chunks para {endpoint_name}: {str(e)}")
            logging.info(f"| {endpoint_name:<15} | {'❌ Falha':<25} | {0:<10} | {0:<10.2f} |")
            logging.info(f"{'--' * 45}")
            raise

    @staticmethod
    def post_process_csv_file(endpoint_name: str, cleanup_chunks: bool = True) -> bool:
        """
        Função para pós-processamento do arquivo CSV completo.
        Aplica normalização de colunas após todos os lotes terem sido processados.
        Opcionalmente remove os chunks temporários após o processamento.
        """
        # Mesclar os chunks
        merge_success = Utils.merge_chunks_and_normalize(endpoint_name)

        # Se solicitado e a mesclagem foi bem-sucedida, remover os chunks
        if merge_success and cleanup_chunks:
            Utils._cleanup_chunks_directory(endpoint_name)

        return merge_success

    @staticmethod
    def process_data_in_batches(endpoint_name: str, endpoint_path: str, headers: Dict, fetch_page_func: Callable,
                                save_to_disk: bool = True, chunk_size: int = 100, batch_size: int = 1,
                                delay_between_pages: int = 1) -> Dict:
        """
        Função genérica para buscar e processar dados em lotes.
        """
        try:
            logging.info(f"📚 Buscando e processando dados em lotes para: {endpoint_path}")

            start_time = time.time()

            # Limpar diretórios existentes se estiver salvando em disco
            if save_to_disk:
                base_output_dir = "output"
                output_dir = os.path.join(base_output_dir, endpoint_name)
                chunks_dir = Utils._get_chunks_dir(endpoint_name)

                # Verificar e criar os diretórios
                os.makedirs(output_dir, exist_ok=True)

                # Limpar chunks existentes
                chunk_files = glob.glob(os.path.join(chunks_dir, "*.csv"))
                for f in chunk_files:
                    try:
                        os.remove(f)
                    except Exception as e:
                        logging.warning(f"Não foi possível remover arquivo {f}: {str(e)}")

                logging.info(f"Diretório de chunks limpo para: {endpoint_name}")

            # Buscar primeira página para obter metadados
            first_page = fetch_page_func(endpoint_path, headers, 1)
            if not first_page or 'total_pages' not in first_page:
                raise ValueError(f"A primeira página retornou dados inválidos: {first_page}")

            total_pages = first_page['total_pages']

            # Contadores e armazenamento temporário
            data_buffer = []

            # Adicionar itens da primeira página
            if 'items' in first_page and first_page['items']:
                data_buffer.extend(first_page['items'])
            else:
                logging.warning("Primeira página não contém itens")

            processed_count = 0
            total_batch_count = 0

            # Processar primeira página se necessário
            if len(data_buffer) >= chunk_size or total_pages == 1 or batch_size == 1:
                logging.info(f"Processando lote inicial com {len(data_buffer)} registros")
                processed_data = Utils.process_and_save_data_in_chunks(
                    data_buffer,
                    endpoint_name,
                    chunk_size=chunk_size,
                    save_to_disk=save_to_disk,
                    batch_id=total_batch_count,
                    skip_empty_columns=False  # Não remover colunas vazias ainda
                )
                processed_count += len(processed_data)
                data_buffer = []
                total_batch_count += 1
                logging.info(f"Processado lote {total_batch_count} com {len(processed_data)} registros")

            # Buscar páginas restantes
            if total_pages > 1:
                batch_counter = 0
                for page_num in range(2, total_pages + 1):
                    try:
                        logging.info(f"🔄 Buscando página {page_num}/{total_pages}")
                        page_data = fetch_page_func(endpoint_path, headers, page_num)

                        if not page_data or 'items' not in page_data:
                            raise ValueError(f"Página {page_num} retornou dados inválidos: {page_data}")

                        data_buffer.extend(page_data['items'])
                        batch_counter += 1

                        # Processar quando atingir o tamanho do lote ou na última página
                        if batch_counter >= batch_size or page_num == total_pages:
                            processed_data = Utils.process_and_save_data_in_chunks(
                                data_buffer,
                                endpoint_name,
                                chunk_size=chunk_size,
                                save_to_disk=save_to_disk,
                                batch_id=total_batch_count,
                                skip_empty_columns=False  # Não remover colunas vazias ainda
                            )

                            processed_count += len(processed_data)
                            total_batch_count += 1
                            logging.info(f"Processado até a página {page_num}/{total_pages} - Lote {total_batch_count} com total acumulado de {processed_count} registros")  # noqa

                            data_buffer = []
                            batch_counter = 0

                        # Pausa entre requisições
                        if page_num < total_pages:
                            time.sleep(delay_between_pages)

                    except Exception as e:
                        logging.error(f"❌ Erro na página {page_num}: {str(e)}")
                        if data_buffer:
                            try:
                                Utils.process_and_save_data_in_chunks(
                                    data_buffer,
                                    endpoint_name,
                                    chunk_size=chunk_size,
                                    save_to_disk=save_to_disk,
                                    batch_id=total_batch_count,
                                    skip_empty_columns=False  # Não remover colunas vazias ainda
                                )
                                total_batch_count += 1
                            except Exception as inner_e:
                                logging.error(
                                    f"❌ Erro ao processar buffer após falha na página {page_num}: {str(inner_e)}")
                            data_buffer = []

            # Processar buffer restante ao final
            if data_buffer:
                logging.info(f"📦 Processando buffer final com {len(data_buffer)} registros restantes")
                processed_data = Utils.process_and_save_data_in_chunks(
                    data_buffer,
                    endpoint_name,
                    chunk_size=chunk_size,
                    save_to_disk=save_to_disk,
                    batch_id=total_batch_count,
                    skip_empty_columns=False  # Não remover colunas vazias ainda
                )
                processed_count += len(processed_data)
                total_batch_count += 1

            # Pós-processamento para mesclar todos os chunks e normalizar
            if save_to_disk and processed_count > 0:
                logging.info(f"🧹 Iniciando mesclagem e normalização de chunks para: {endpoint_name}")
                Utils.merge_chunks_and_normalize(endpoint_name)

            duration = time.time() - start_time
            logging.info(f"✅ Processamento concluído: {processed_count} registros em {duration:.2f}s")

            return {
                "registros": processed_count,
                "status": "Sucesso",
                "tempo": duration
            }

        except Exception as e:
            logging.exception(f"❌ Falha no processamento em lotes: {str(e)}")
            return {
                "registros": 0,
                "status": f"Falha: {type(e).__name__}: {str(e)}",
                "tempo": 0
            }
