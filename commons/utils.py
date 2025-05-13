import logging
import os
import re
import time
import unicodedata
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
    def _normalize_key(key: str) -> str:
        """
        Normaliza uma chave de string aplicando transformações para padronização.
        """
        key = key.strip('\'"')

        # Detecta se é um camelCase com a primeira letra maiúscula (PascalCase)
        is_pascal_case = bool(re.match(r'^[A-Z]', key) and re.search(r'[a-z]', key))

        # Detecta se é um verdadeiro camelCase (inicia com minúscula, depois tem maiúscula)
        is_true_camel_case = bool(re.match(r'^[a-z].*[A-Z]', key))

        # Converte PascalCase para snake_case (ex: ExibeContato -> exibe_contato)
        if is_pascal_case:
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

            # Salvar localmente
            Utils._save_local_dataframe(df, endpoint_name)

            logging.info(f"Processamento concluído: {len(df)} registros para {endpoint_name}")

            # Retornar os dados processados
            return df.to_dict(orient='records')

        except Exception as e:
            logging.error(f"Erro no processamento de dados para {endpoint_name}: {str(e)}")
            raise

    @staticmethod
    def process_and_save_data_in_chunks(raw_data: List[Dict], endpoint_name: str, chunk_size: int = 1000,
                                        save_to_disk: bool = True, append_mode: bool = False,
                                        skip_empty_columns: bool = False) -> List[Dict]:
        """
        Processa dados brutos em chunks e opcionalmente salva em disco.
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

            # Remoção de colunas vazias agora é opcional e controlada pelo parâmetro
            if skip_empty_columns:
                df = Utils._remove_empty_columns(df)

            df = Utils._convert_columns_to_nullable_int(df)

            # Se não precisar salvar em disco, retornar os dados processados imediatamente
            if not save_to_disk:
                logging.info(
                    f"Processamento concluído: {len(df)} registros para {endpoint_name} (mantidos apenas em memória)")
                return df.to_dict(orient='records')

            # Preparar para salvar em disco
            base_output_dir = "output"
            os.makedirs(base_output_dir, exist_ok=True)

            output_dir = os.path.join(base_output_dir, endpoint_name)
            os.makedirs(output_dir, exist_ok=True)

            csv_filename = os.path.join(output_dir, f"{endpoint_name}.csv")

            # Determinar o número total de chunks
            total_rows = len(df)
            num_chunks = (total_rows + chunk_size - 1) // chunk_size  # Arredonda para cima

            # Verificar se o arquivo existe (para modo append)
            file_exists = os.path.isfile(csv_filename) and append_mode

            # Processar cada chunk e salvar
            for i in range(num_chunks):
                start_idx = i * chunk_size
                end_idx = min((i + 1) * chunk_size, total_rows)
                chunk_df = df.iloc[start_idx:end_idx].copy()

                # Determinar se devemos incluir cabeçalho e modo de escrita
                include_header = (not append_mode and i == 0) or (append_mode and not file_exists and i == 0)
                write_mode = 'w' if (i == 0 and not append_mode) or (not file_exists and append_mode) else 'a'

                # Salvar o chunk
                chunk_df.to_csv(csv_filename, sep=";", index=False, mode=write_mode, header=include_header)

                # Registrar que o arquivo agora existe
                file_exists = True

                # Log do progresso
                if include_header:
                    logging.info(
                        f"Chunk {i + 1}/{num_chunks}: Iniciado arquivo com cabeçalho ({len(chunk_df)} registros)")
                else:
                    logging.info(f"Chunk {i + 1}/{num_chunks}: Adicionados {len(chunk_df)} registros ao arquivo")

            logging.info(f"Processamento concluído: {total_rows} registros salvos em {csv_filename}")

            # Retornar os registros processados
            return df.to_dict(orient='records')

        except Exception as e:
            logging.error(f"Erro no processamento de dados em chunks para {endpoint_name}: {str(e)}")
            raise

    @staticmethod
    def post_process_csv_file(endpoint_name: str) -> bool:
        """
        Função para pós-processamento do arquivo CSV completo.
        Aplica normalização de colunas após todos os lotes terem sido processados.
        """
        try:
            base_output_dir = "output"
            output_dir = os.path.join(base_output_dir, endpoint_name)
            csv_filename = os.path.join(output_dir, f"{endpoint_name}.csv")

            if not os.path.exists(csv_filename):
                logging.warning(f"Arquivo CSV não encontrado para pós-processamento: {csv_filename}")
                return False

            # Carregar o arquivo CSV completo
            logging.info(f"Iniciando pós-processamento para {csv_filename}")
            df = pd.read_csv(csv_filename, sep=";")

            if df.empty:
                logging.warning(f"DataFrame vazio após leitura para {endpoint_name}")
                return False

            # Registrar estatísticas antes do processamento
            before_cols = len(df.columns)
            before_rows = len(df)
            logging.info(f"Arquivo original: {before_rows} linhas, {before_cols} colunas")

            # Aplicar transformações globais
            df = Utils._remove_empty_columns(df)

            # Registrar estatísticas após o processamento
            after_cols = len(df.columns)
            removed_cols = before_cols - after_cols
            logging.info(f"Colunas removidas: {removed_cols} ({before_cols} -> {after_cols})")

            # Criar arquivo temporário com novos dados
            temp_csv = csv_filename + ".temp"
            df.to_csv(temp_csv, sep=";", index=False)

            # Substituir o arquivo original pelo temporário
            os.replace(temp_csv, csv_filename)

            logging.info(f"✅ Pós-processamento concluído: {csv_filename}")
            return True

        except Exception as e:
            logging.error(f"❌ Erro no pós-processamento do arquivo CSV para {endpoint_name}: {str(e)}")
            return False

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

            # Limpar arquivo existente se estiver salvando em disco
            if save_to_disk:
                output_dir = os.path.join("output", endpoint_name)
                os.makedirs(output_dir, exist_ok=True)
                csv_filename = os.path.join(output_dir, f"{endpoint_name}.csv")
                if os.path.exists(csv_filename):
                    os.remove(csv_filename)
                    logging.info(f"Arquivo anterior removido: {csv_filename}")

            # Buscar primeira página para obter metadados
            first_page = fetch_page_func(endpoint_path, headers, 1)
            total_pages = first_page['total_pages']

            # Contadores e armazenamento temporário
            data_buffer = []
            data_buffer.extend(first_page['items'])
            processed_count = 0
            file_header_written = False

            # Processar primeira página se necessário
            if len(data_buffer) >= chunk_size or total_pages == 1 or batch_size == 1:
                processed_data = Utils.process_and_save_data_in_chunks(
                    data_buffer,
                    endpoint_name,
                    chunk_size=chunk_size,
                    save_to_disk=save_to_disk,
                    skip_empty_columns=False  # Não remover colunas vazias ainda
                )
                processed_count += len(processed_data)
                data_buffer = []
                file_header_written = True
                logging.info(f"Processado lote 1/{total_pages} com {len(processed_data)} registros")

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
                            if file_header_written:
                                processed_data = Utils.process_and_save_data_in_chunks(
                                    data_buffer,
                                    endpoint_name,
                                    chunk_size=chunk_size,
                                    save_to_disk=save_to_disk,
                                    append_mode=True,
                                    skip_empty_columns=False  # Não remover colunas vazias ainda
                                )
                            else:
                                processed_data = Utils.process_and_save_data_in_chunks(
                                    data_buffer,
                                    endpoint_name,
                                    chunk_size=chunk_size,
                                    save_to_disk=save_to_disk,
                                    skip_empty_columns=False  # Não remover colunas vazias ainda
                                )
                                file_header_written = True

                            processed_count += len(processed_data)
                            logging.info(
                                f"Processado até a página {page_num}/{total_pages} com {processed_count} registros total")

                            data_buffer = []
                            batch_counter = 0

                        # Pausa entre requisições
                        if page_num < total_pages:
                            time.sleep(delay_between_pages)

                    except Exception as e:
                        logging.error(f"❌ Erro na página {page_num}: {str(e)}")
                        if data_buffer:
                            Utils.process_and_save_data_in_chunks(
                                data_buffer,
                                endpoint_name,
                                chunk_size=chunk_size,
                                save_to_disk=save_to_disk,
                                append_mode=file_header_written,
                                skip_empty_columns=False  # Não remover colunas vazias ainda
                            )
                            data_buffer = []

            # ✅ Corrigir: Processar buffer restante ao final
            if data_buffer:
                logging.info(f"📦 Processando buffer final com {len(data_buffer)} registros restantes")
                if file_header_written:
                    processed_data = Utils.process_and_save_data_in_chunks(
                        data_buffer,
                        endpoint_name,
                        chunk_size=chunk_size,
                        save_to_disk=save_to_disk,
                        append_mode=True,
                        skip_empty_columns=False  # Não remover colunas vazias ainda
                    )
                else:
                    processed_data = Utils.process_and_save_data_in_chunks(
                        data_buffer,
                        endpoint_name,
                        chunk_size=chunk_size,
                        save_to_disk=save_to_disk,
                        skip_empty_columns=False  # Não remover colunas vazias ainda
                    )
                    file_header_written = True

                processed_count += len(processed_data)

            # ✅ NOVO: Pós-processamento para remover colunas vazias após processar todos os dados
            if save_to_disk and processed_count > 0:
                logging.info(f"🧹 Iniciando pós-processamento para remover colunas vazias: {endpoint_name}")
                Utils.post_process_csv_file(endpoint_name)

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
