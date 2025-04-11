import logging
import os
import re
import unicodedata
from datetime import datetime
from typing import Dict, Any

import pandas as pd

# Configuração básica do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def flatten_json(data: dict, parent_key: str = '', sep: str = '_') -> Dict[str, str]:
    try:
        items = []
        if isinstance(data, dict):
            for key, value in data.items():
                new_key = f"{parent_key}{sep}{key}" if parent_key else key
                items.extend(flatten_json(value, new_key, sep).items())
        elif isinstance(data, list):
            for index, item in enumerate(data):
                new_key = f"{parent_key}{sep}{index + 1}"
                items.extend(flatten_json(item, new_key, sep).items())
        else:
            items.append((parent_key, data))
        return dict(items)

    except Exception as e:
        logging.error(f"Erro ao achatadar JSON: {str(e)}")
        raise


def remove_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
    try:
        # Número inicial de colunas
        initial_columns = df.shape[1]

        # Drop columns where all values are NaN
        df_no_nan = df.dropna(axis=1, how='all')
        nan_columns_removed = initial_columns - df_no_nan.shape[1]
        logging.info(f"Colunas removidas por serem NaN: {nan_columns_removed}")

        # Identify and drop columns where all values are empty strings
        empty_columns = [
            col for col in df_no_nan.columns
            if df_no_nan[col].astype(str).str.strip().eq('').all()
        ]
        df_cleaned = df_no_nan.drop(columns=empty_columns)
        empty_columns_removed = len(empty_columns)
        logging.info(f"Colunas removidas por serem strings vazias: {empty_columns_removed}")

        # Total de colunas removidas
        total_columns_removed = nan_columns_removed + empty_columns_removed
        logging.info(f"Total de colunas removidas: {total_columns_removed}")

        # Verifica se ocorreu algum erro durante o processamento
        if df_cleaned.empty:
            raise ValueError("O DataFrame resultante está vazio após a remoção de colunas.")

        return df_cleaned

    except Exception as e:
        logging.error(f"Erro ao remover colunas vazias: {str(e)}")
        raise


def normalize_keys(data: Any) -> Any:
    try:
        def normalize_key(key):
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

        if isinstance(data, dict):
            # Normaliza as chaves do dicionário
            normalized_dict = {normalize_key(k): normalize_keys(v) for k, v in data.items()}
            return normalized_dict
        elif isinstance(data, list):
            # Normaliza cada item na lista
            return [normalize_keys(item) for item in data]
        else:
            # Retorna o valor original se não for um dicionário ou lista
            return data

    except Exception as e:
        logging.error(f"Erro ao normalizar chaves: {str(e)}")
        raise


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    try:
        # Função auxiliar para converter camelCase em snake_case
        def camel_to_snake(name):
            return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()

        # Registro do estado inicial das colunas
        original_columns = df.columns.tolist()
        logging.info(f"Colunas originais: {original_columns}")

        # Normalização dos nomes das colunas
        normalized_columns = []
        for col in df.columns:
            # Converte para lowercase
            col = col.lower()
            # Substitui caracteres especiais e espaços por underscores
            col = re.sub(r'[^a-z0-9]+', '_', col)
            # Remove underscores extras no início ou fim
            col = col.strip('_')
            # Converte camelCase para snake_case
            col = camel_to_snake(col)
            normalized_columns.append(col)

        # Aplica os novos nomes de colunas ao DataFrame
        df.columns = normalized_columns

        # Registro das colunas normalizadas
        logging.info(f"Colunas normalizadas: {normalized_columns}")

        return df

    except Exception as e:
        logging.error(f"Erro ao normalizar nomes de colunas: {str(e)}")
        raise


def save_dataframe(df, file_name, separator=";"):
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f"data/{file_name}_{timestamp}.csv"
        os.makedirs("data", exist_ok=True)
        df.to_csv(csv_filename, sep=separator, index=False)
        return csv_filename
    except Exception as e:
        logging.error(f"Erro ao salvar .csv local: {str(e)}")
        raise
