import logging
import os
import re
import unicodedata
from typing import Dict, Any, List

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
        # Converte para lowercase
        key = key.lower()
        # Remove acentos e outros caracteres Unicode
        key = unicodedata.normalize('NFKD', key).encode('ASCII', 'ignore').decode('ASCII')
        # Substitui caracteres especiais e espaços por underscores
        key = re.sub(r'[^a-z0-9]+', '_', key)
        # Remove underscores extras no início ou fim
        key = key.strip('_')
        # Converte camelCase para snake_case
        key = re.sub(r'(?<!^)(?=[A-Z])', '_', key).lower()
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
            # Criar estrutura de diretórios
            output_dir = f"output/{file_name}"
            csv_filename = f"{output_dir}/{file_name}.csv"
            os.makedirs(output_dir, exist_ok=True)

            # Salvar o arquivo
            df.to_csv(csv_filename, sep=separator, index=False)
            logging.info(f"Arquivo salvo com sucesso: {csv_filename}")
            return True

        except Exception as e:
            logging.error(f"Erro ao salvar DataFrame como CSV: {str(e)}")
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
