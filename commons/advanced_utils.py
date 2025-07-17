import logging
from typing import Dict, Any, List, Optional

from commons.utils import Utils

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class RelationalExtractor:

    def __init__(self, parent_id_field: str = "id"):
        self.tables = {}
        self.id_counter = 1
        self.parent_id_field = parent_id_field

    def _normalize_table_name(self, name: str) -> str:
        """Normaliza nomes de tabelas seguindo o padrão do Utils."""
        return name.lower().replace(' ', '_').replace('-', '_')

    def extract_all_lists(self, data: List[Dict], table_prefix: str = "main") -> Dict[str, List[Dict]]:
        """
        Extrai TODAS as listas em QUALQUER profundidade.
        """
        # Tabela principal
        main_table = []

        for record in data:
            # Usar a chave especificada ou fallback para 'id' ou contador
            record_id = record.get(self.parent_id_field) or record.get('id', self.id_counter)
            if self.parent_id_field not in record and 'id' not in record:
                self.id_counter += 1

            # Processar este registro e extrair todas as listas
            cleaned_record = self._extract_lists_from_object(
                obj=record,
                parent_table=table_prefix,
                parent_id=record_id
            )

            main_table.append(cleaned_record)

        # Resultado final
        result = {table_prefix: main_table}
        result.update(self.tables)

        return result

    def _extract_lists_from_object(self, obj: Any, parent_table: str, parent_id: Any) -> Any:
        """
        Processa um objeto e extrai TODAS as listas.
        """
        if isinstance(obj, dict):
            cleaned_obj = {}

            for key, value in obj.items():
                if isinstance(value, list) and value and self._is_object_list(value):
                    # EXTRAIR LISTA DE OBJETOS - NORMALIZAR NOME DA CHAVE
                    normalized_key = self._normalize_table_name(key)
                    child_table = f"{parent_table}_{normalized_key}"
                    parent_fk = f"{parent_table}_id"

                    self._process_object_list(value, child_table, parent_id, parent_fk)

                    # NÃO incluir no objeto pai
                    continue
                else:
                    # RECURSÃO: Processar valores aninhados
                    cleaned_obj[key] = self._extract_lists_from_object(value, parent_table, parent_id)

            return cleaned_obj

        elif isinstance(obj, list):
            # Lista de valores primitivos ou já processada
            return [self._extract_lists_from_object(item, parent_table, parent_id) for item in obj]

        else:
            # Valor primitivo
            return obj

    def _process_object_list(self, obj_list: List[Dict], table_name: str, parent_id: Any, parent_fk: str):
        """
        Processa uma lista de objetos e extrai suas sublistas.
        """
        if table_name not in self.tables:
            self.tables[table_name] = []

        for obj in obj_list:
            if not isinstance(obj, dict):
                continue

            # ID único para este objeto - usar 'id' padrão ou contador
            obj_id = obj.get('id', self.id_counter)
            if 'id' not in obj:
                self.id_counter += 1

            # RECURSÃO: Processar este objeto para extrair suas listas
            cleaned_obj = self._extract_lists_from_object(obj, table_name, obj_id)

            # Criar registro final
            record = {
                'id': obj_id,
                parent_fk: parent_id,
                **{k: v for k, v in cleaned_obj.items() if k != 'id'}
            }

            self.tables[table_name].append(record)

    def _is_object_list(self, lst: List) -> bool:
        """Verifica se é uma lista de objetos (não primitivos)."""
        return any(isinstance(item, dict) for item in lst)


class AdvancedUtils(Utils):

    @staticmethod
    def process_with_relational_extraction(raw_data: List[Dict], endpoint_name: str, parent_id_field: str = "id",
                                           table_configs: Optional[Dict] = None, auto_detect: bool = True, **kwargs) -> Dict[str, List[Dict]]:
        """
        EXTRAÇÃO AUTOMÁTICA com TOTAL compatibilidade.

        Args:
            raw_data: Dados brutos da API
            endpoint_name: Nome do endpoint/tabela principal
            parent_id_field: Campo que será usado como chave primária (padrão: "id")
            table_configs: IGNORADO - mantido para compatibilidade
            auto_detect: IGNORADO - mantido para compatibilidade
            **kwargs: Outros parâmetros antigos ignorados
        """
        if not raw_data:
            logging.warning(f"Nenhum dado recebido para o endpoint {endpoint_name}")
            return {}

        # Log de compatibilidade se parâmetros antigos foram passados
        ignored_params = []
        if table_configs is not None:
            ignored_params.append("table_configs")
        if not auto_detect:
            ignored_params.append("auto_detect=False")
        if kwargs:
            ignored_params.extend(kwargs.keys())

        if ignored_params:
            logging.info(f"⚠️ Parâmetros ignorados (modo compatibilidade): {', '.join(ignored_params)}")

        logging.info(f"🔍 EXTRAÇÃO AUTOMÁTICA - {endpoint_name} (chave: {parent_id_field})...")

        # Extração automática completa com chave customizada
        extractor = RelationalExtractor(parent_id_field=parent_id_field)
        extracted_data = extractor.extract_all_lists(raw_data, endpoint_name)

        # Log das tabelas encontradas
        logging.info(f"📋 Tabelas extraídas: {list(extracted_data.keys())}")
        for table_name, table_data in extracted_data.items():
            if table_data:
                logging.info(f"   └── {table_name}: {len(table_data)} registros")

        # Processar e salvar cada tabela
        results = {}
        for table_name, table_data in extracted_data.items():
            if table_data:
                logging.info(f"💾 Salvando: {table_name} ({len(table_data)} registros)")
                results[table_name] = AdvancedUtils.process_and_save_data(table_data, table_name)

        return results
