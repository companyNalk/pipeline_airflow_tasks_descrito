import logging
from typing import Dict, Any, List

from commons.utils import Utils

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class RelationalExtractor:

    def __init__(self):
        self.extracted_tables = {}
        self.foreign_keys = {}
        self.table_configs = {}

    def configure_extraction(self, table_configs: Dict[str, Dict]):
        """
        Configura quais campos devem ser extraídos como tabelas separadas.

        Exemplo de configuração:
        {
            "tags": {
                "parent_id_field": "lead_id",  # Nome do campo que será a FK na tabela filha
                "fields_to_extract": ["id", "tag_id", "tag_name", "added_at"],  # Campos específicos ou None para todos
                "table_name": "lead_tags"  # Nome da tabela resultante
            },
            "messages": {
                "parent_id_field": "lead_id",
                "fields_to_extract": None,  # Extrai todos os campos
                "table_name": "lead_messages"
            },
            "schedulated_actions": {
                "parent_id_field": "lead_id",
                "fields_to_extract": None,
                "table_name": "lead_scheduled_actions"
            }
        }
        """
        self.table_configs = table_configs
        # Inicializar estruturas para cada tabela configurada
        for config in table_configs.values():
            table_name = config["table_name"]
            self.extracted_tables[table_name] = []

    def extract_relational_data(self, data: List[Dict], parent_id_field: str = "id") -> Dict[str, List[Dict]]:
        """
        Extrai dados relacionais baseado na configuração.

        Args:
            data: Lista de registros JSON
            parent_id_field: Campo que será usado como ID pai (ex: "id", "internal_id")

        Returns:
            Dict com tabelas extraídas e dados principais limpos
        """
        main_data = []

        for record in data:
            # Obter ID do registro pai
            parent_id = self._get_nested_value(record, parent_id_field.split('.'))

            # Processar cada configuração de tabela
            cleaned_record = record.copy()

            for field_path, config in self.table_configs.items():
                table_name = config["table_name"]
                parent_fk_field = config["parent_id_field"]
                fields_to_extract = config.get("fields_to_extract", None)

                # Extrair dados da lista/array
                list_data = self._get_nested_value(record, field_path.split('.'))

                if list_data and isinstance(list_data, list):
                    for item in list_data:
                        # Adicionar FK do pai
                        extracted_item = {parent_fk_field: parent_id}

                        # Extrair campos específicos ou todos
                        if fields_to_extract:
                            for field in fields_to_extract:
                                if field in item:
                                    extracted_item[field] = item[field]
                        else:
                            extracted_item.update(item)

                        self.extracted_tables[table_name].append(extracted_item)

                    # Remover o campo original do registro principal
                    self._remove_nested_field(cleaned_record, field_path.split('.'))

            main_data.append(cleaned_record)

        return {
            "main_table": main_data,
            **self.extracted_tables
        }

    def _get_nested_value(self, data: Dict, path: List[str]) -> Any:
        """Obtém valor aninhado seguindo um caminho de chaves."""
        current = data
        for key in path:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return None
        return current

    def _remove_nested_field(self, data: Dict, path: List[str]) -> None:
        """Remove campo aninhado seguindo um caminho de chaves."""
        current = data
        for key in path[:-1]:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return

        if isinstance(current, dict) and path[-1] in current:
            del current[path[-1]]


class AdvancedUtils(Utils):
    """Extensão da classe Utils com capacidades de extração relacional."""

    @staticmethod
    def auto_detect_list_fields(data: List[Dict], max_sample: int = 100) -> Dict[str, Dict]:
        """
        Detecta automaticamente campos que contêm listas/arrays e sugere configuração.

        Args:
            data: Lista de registros para análise
            max_sample: Número máximo de registros a analisar

        Returns:
            Configuração sugerida para extração relacional
        """
        list_fields = {}
        sample_data = data[:max_sample] if len(data) > max_sample else data

        def find_lists_recursive(obj: Any, path: str = "") -> None:
            if isinstance(obj, dict):
                for key, value in obj.items():
                    current_path = f"{path}.{key}" if path else key

                    if isinstance(value, list) and value:
                        # Verificar se a lista contém dicionários (objetos)
                        if isinstance(value[0], dict):
                            list_fields[current_path] = {
                                "sample_item": value[0],
                                "list_length": len(value),
                                "suggested_table_name": f"{current_path.replace('.', '_')}",
                                "suggested_parent_id": "parent_id"
                            }
                    else:
                        find_lists_recursive(value, current_path)
            elif isinstance(obj, list):
                for item in obj:
                    find_lists_recursive(item, path)

        # Analisar amostra de dados
        for record in sample_data:
            find_lists_recursive(record)

        # Gerar configuração sugerida
        suggested_config = {}
        for field_path, info in list_fields.items():
            table_name = info["suggested_table_name"]
            suggested_config[field_path] = {
                "parent_id_field": "parent_id",
                "fields_to_extract": None,  # Todos os campos
                "table_name": table_name
            }

        return suggested_config

    @staticmethod
    def process_with_relational_extraction(raw_data: List[Dict], endpoint_name: str,
                                           table_configs: Dict[str, Dict] = None,
                                           parent_id_field: str = "id",
                                           auto_detect: bool = True) -> Dict[str, List[Dict]]:
        """
        Processa dados com extração relacional automática ou configurada.

        Args:
            raw_data: Dados brutos da API
            endpoint_name: Nome do endpoint (usado para nomes de arquivo)
            table_configs: Configuração manual das tabelas a extrair
            parent_id_field: Campo ID do registro pai
            auto_detect: Se deve detectar automaticamente listas

        Returns:
            Dict com todas as tabelas processadas
        """
        if not raw_data:
            logging.warning(f"Nenhum dado recebido para o endpoint {endpoint_name}")
            return {}

        # Auto-detectar configuração se não fornecida
        if not table_configs and auto_detect:
            logging.info("🔍 Detectando automaticamente campos de lista...")
            detected = AdvancedUtils.auto_detect_list_fields(raw_data)

            if detected:
                logging.info(f"📋 Campos de lista detectados: {list(detected.keys())}")
                table_configs = detected
            else:
                logging.info("ℹ️ Nenhum campo de lista detectado. Processando normalmente.")
                return {"main_table": AdvancedUtils.process_and_save_data(raw_data, endpoint_name)}

        # Inicializar extrator relacional
        extractor = RelationalExtractor()
        if table_configs:
            extractor.configure_extraction(table_configs)

            # Extrair dados relacionais
            logging.info(f"🔄 Extraindo dados relacionais para {len(table_configs)} tabelas...")
            extracted_data = extractor.extract_relational_data(raw_data, parent_id_field)

            # Processar e salvar cada tabela
            results = {}

            # Processar tabela principal
            main_data = extracted_data["main_table"]
            results["main_table"] = AdvancedUtils.process_and_save_data(main_data, endpoint_name)

            # Processar tabelas relacionais
            for table_name, table_data in extracted_data.items():
                if table_name != "main_table" and table_data:
                    logging.info(f"📊 Processando tabela relacional: {table_name} ({len(table_data)} registros)")
                    results[table_name] = AdvancedUtils.process_and_save_data(table_data,
                                                                              f"{endpoint_name}_{table_name}")

            # Log do resumo
            logging.info("📋 RESUMO DA EXTRAÇÃO RELACIONAL:")
            logging.info(f"   Tabela principal: {len(results.get('main_table', []))} registros")
            for table_name, data in results.items():
                if table_name != "main_table":
                    logging.info(f"   {table_name}: {len(data)} registros")

            return results
        else:
            # Processamento normal se não há configuração
            return {"main_table": AdvancedUtils.process_and_save_data(raw_data, endpoint_name)}

    @staticmethod
    def create_relational_config_for_contact2sale() -> Dict[str, Dict]:
        """
        Configuração específica para a API do Contact2Sale baseada no exemplo fornecido.
        """
        return {
            "attributes.tags": {
                "parent_id_field": "lead_id",
                "fields_to_extract": ["id", "tag_id", "tag_name", "added_at"],
                "table_name": "lead_tags"
            },
            "attributes.log": {
                "parent_id_field": "lead_id",
                "fields_to_extract": ["body", "created_at"],
                "table_name": "lead_log"
            },
            "messages": {
                "parent_id_field": "lead_id",
                "fields_to_extract": None,  # Todos os campos
                "table_name": "lead_messages"
            },
            "schedulated_actions": {
                "parent_id_field": "lead_id",
                "fields_to_extract": None,
                "table_name": "lead_scheduled_actions"
            },
            "attributes.collaborators": {
                "parent_id_field": "lead_id",
                "fields_to_extract": None,
                "table_name": "lead_collaborators"
            }
        }

    @staticmethod
    def process_contact2sale_leads(raw_data: List[Dict], endpoint_name: str = "leads") -> Dict[str, List[Dict]]:
        """
        Função específica para processar leads do Contact2Sale com extração relacional.
        """
        # Usar configuração específica do Contact2Sale
        config = AdvancedUtils.create_relational_config_for_contact2sale()

        return AdvancedUtils.process_with_relational_extraction(
            raw_data=raw_data,
            endpoint_name=endpoint_name,
            table_configs=config,
            parent_id_field="id",  # ou "internal_id" se preferir
            auto_detect=False  # Usando configuração manual
        )


# Exemplo de uso para Contact2Sale
def exemplo_uso_contact2sale():
    """Exemplo de como usar a nova funcionalidade com dados do Contact2Sale."""

    # Simular dados da API (seu raw_data atual)
    raw_leads_data = [
        # Seus dados JSON aqui...
    ]

    # Opção 1: Uso automático com detecção
    logging.info("=== OPÇÃO 1: DETECÇÃO AUTOMÁTICA ===")
    results_auto = AdvancedUtils.process_with_relational_extraction(
        raw_data=raw_leads_data,
        endpoint_name="leads_auto",
        auto_detect=True
    )

    # Opção 2: Uso com configuração específica do Contact2Sale
    logging.info("=== OPÇÃO 2: CONFIGURAÇÃO ESPECÍFICA ===")
    results_manual = AdvancedUtils.process_contact2sale_leads(raw_leads_data)

    # Opção 3: Configuração personalizada
    logging.info("=== OPÇÃO 3: CONFIGURAÇÃO PERSONALIZADA ===")
    custom_config = {
        "attributes.tags": {
            "parent_id_field": "lead_internal_id",
            "fields_to_extract": ["tag_id", "tag_name"],
            "table_name": "custom_tags"
        }
    }

    results_custom = AdvancedUtils.process_with_relational_extraction(
        raw_data=raw_leads_data,
        endpoint_name="leads_custom",
        table_configs=custom_config,
        parent_id_field="internal_id"
    )

    return results_auto, results_manual, results_custom
