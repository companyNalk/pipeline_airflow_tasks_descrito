import os
import shutil
from unittest.mock import patch

import pytest

from commons.advanced_utils import RelationalExtractor, AdvancedUtils


@pytest.fixture(autouse=True)
def cleanup_output_directory():
    """Fixture que limpa o diretório output antes e depois de cada teste."""
    output_dir = "output"
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    yield
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)


class TestRelationalExtractor:
    """Testes para a nova implementação da classe RelationalExtractor."""

    def test_init_default(self):
        # GIVEN & WHEN
        extractor = RelationalExtractor()

        # THEN
        assert extractor.tables == {}
        assert extractor.id_counter == 1
        assert extractor.parent_id_field == "id"

    def test_init_custom_parent_id_field(self):
        # GIVEN & WHEN
        extractor = RelationalExtractor(parent_id_field="custom_id")

        # THEN
        assert extractor.parent_id_field == "custom_id"

    def test_normalize_table_name(self):
        # GIVEN
        extractor = RelationalExtractor()

        # WHEN & THEN
        assert extractor._normalize_table_name("My Table") == "my_table"
        assert extractor._normalize_table_name("table-name") == "table_name"
        assert extractor._normalize_table_name("MIXED_Case-Name") == "mixed_case_name"
        assert extractor._normalize_table_name("already_normalized") == "already_normalized"

    def test_is_object_list_true(self):
        # GIVEN
        extractor = RelationalExtractor()
        object_list = [{"id": 1, "name": "test"}, {"id": 2, "name": "test2"}]

        # WHEN
        result = extractor._is_object_list(object_list)

        # THEN
        assert result is True

    def test_is_object_list_false_primitives(self):
        # GIVEN
        extractor = RelationalExtractor()
        primitive_list = [1, 2, 3, "string"]

        # WHEN
        result = extractor._is_object_list(primitive_list)

        # THEN
        assert result is False

    def test_is_object_list_false_empty(self):
        # GIVEN
        extractor = RelationalExtractor()
        empty_list = []

        # WHEN
        result = extractor._is_object_list(empty_list)

        # THEN
        assert result is False

    def test_is_object_list_mixed_with_objects(self):
        # GIVEN
        extractor = RelationalExtractor()
        mixed_list = [1, "string", {"id": 1}]

        # WHEN
        result = extractor._is_object_list(mixed_list)

        # THEN
        assert result is True  # Retorna True se pelo menos um item for dict

    def test_extract_all_lists_simple(self):
        # GIVEN
        extractor = RelationalExtractor()
        data = [
            {
                "id": 1,
                "name": "Item 1",
                "tags": [
                    {"id": 10, "name": "tag1"},
                    {"id": 20, "name": "tag2"}
                ]
            }
        ]

        # WHEN
        result = extractor.extract_all_lists(data, "main")

        # THEN
        assert "main" in result
        assert "main_tags" in result
        assert len(result["main"]) == 1
        assert len(result["main_tags"]) == 2
        assert "tags" not in result["main"][0]  # Lista removida da tabela principal
        assert result["main_tags"][0]["main_id"] == 1
        assert result["main_tags"][0]["name"] == "tag1"

    def test_extract_all_lists_with_custom_parent_id_field(self):
        # GIVEN
        extractor = RelationalExtractor(parent_id_field="custom_id")
        data = [
            {
                "custom_id": "abc123",
                "items": [{"value": "test"}]
            }
        ]

        # WHEN
        result = extractor.extract_all_lists(data, "main")

        # THEN
        assert result["main"][0]["custom_id"] == "abc123"
        assert result["main_items"][0]["main_id"] == "abc123"

    def test_extract_all_lists_auto_generated_ids(self):
        # GIVEN
        extractor = RelationalExtractor()
        data = [
            {
                "name": "No ID Item",
                "tags": [{"name": "tag1"}]
            }
        ]

        # WHEN
        result = extractor.extract_all_lists(data, "main")

        # THEN
        assert result["main"][0]["name"] == "No ID Item"
        assert result["main_tags"][0]["main_id"] == 1  # ID auto-gerado para item principal
        assert result["main_tags"][0]["id"] == 2  # ID auto-gerado para o objeto filho (contador global)

    def test_extract_all_lists_nested_objects(self):
        # GIVEN
        extractor = RelationalExtractor()
        data = [
            {
                "id": 1,
                "level1": {
                    "level2": {
                        "items": [{"value": "nested"}]
                    }
                }
            }
        ]

        # WHEN
        result = extractor.extract_all_lists(data, "main")

        # THEN
        assert "main" in result
        assert "main_items" in result
        assert result["main_items"][0]["value"] == "nested"

    def test_extract_all_lists_multiple_levels(self):
        # GIVEN
        extractor = RelationalExtractor()
        data = [
            {
                "id": 1,
                "tags": [
                    {
                        "id": 10,
                        "name": "tag1",
                        "properties": [
                            {"key": "color", "value": "blue"},
                            {"key": "size", "value": "large"}
                        ]
                    }
                ]
            }
        ]

        # WHEN
        result = extractor.extract_all_lists(data, "main")

        # THEN
        assert "main" in result
        assert "main_tags" in result
        assert "main_tags_properties" in result
        assert len(result["main_tags"]) == 1
        assert len(result["main_tags_properties"]) == 2
        assert "properties" not in result["main_tags"][0]  # Lista removida
        assert result["main_tags_properties"][0]["main_tags_id"] == 10

    def test_extract_all_lists_empty_data(self):
        # GIVEN
        extractor = RelationalExtractor()
        data = []

        # WHEN
        result = extractor.extract_all_lists(data, "main")

        # THEN
        assert result == {"main": []}

    def test_extract_all_lists_no_object_lists(self):
        # GIVEN
        extractor = RelationalExtractor()
        data = [
            {
                "id": 1,
                "name": "Item",
                "simple_list": [1, 2, 3],  # Lista de primitivos
                "value": "test"
            }
        ]

        # WHEN
        result = extractor.extract_all_lists(data, "main")

        # THEN
        assert result == {"main": data}  # Nenhuma extração realizada

    def test_extract_all_lists_empty_object_lists(self):
        # GIVEN
        extractor = RelationalExtractor()
        data = [
            {
                "id": 1,
                "tags": [],  # Lista vazia
                "name": "Item"
            }
        ]

        # WHEN
        result = extractor.extract_all_lists(data, "main")

        # THEN
        assert "main" in result
        assert "main_tags" not in result  # Lista vazia não cria tabela
        assert result["main"][0]["name"] == "Item"

    def test_extract_lists_from_object_primitive_value(self):
        # GIVEN
        extractor = RelationalExtractor()

        # WHEN
        result = extractor._extract_lists_from_object("primitive_value", "main", 1)

        # THEN
        assert result == "primitive_value"

    def test_extract_lists_from_object_primitive_list(self):
        # GIVEN
        extractor = RelationalExtractor()
        primitive_list = [1, 2, 3]

        # WHEN
        result = extractor._extract_lists_from_object(primitive_list, "main", 1)

        # THEN
        assert result == [1, 2, 3]

    def test_extract_lists_from_object_dict_no_lists(self):
        # GIVEN
        extractor = RelationalExtractor()
        obj = {"id": 1, "name": "test", "value": 42}

        # WHEN
        result = extractor._extract_lists_from_object(obj, "main", 1)

        # THEN
        assert result == obj

    def test_process_object_list_with_existing_ids(self):
        # GIVEN
        extractor = RelationalExtractor()
        obj_list = [
            {"id": 100, "name": "item1"},
            {"id": 200, "name": "item2"}
        ]

        # WHEN
        extractor._process_object_list(obj_list, "test_table", 1, "parent_id")

        # THEN
        assert len(extractor.tables["test_table"]) == 2
        assert extractor.tables["test_table"][0]["id"] == 100
        assert extractor.tables["test_table"][0]["parent_id"] == 1
        assert extractor.tables["test_table"][1]["id"] == 200

    def test_process_object_list_auto_generated_ids(self):
        # GIVEN
        extractor = RelationalExtractor()
        obj_list = [
            {"name": "item1"},
            {"name": "item2"}
        ]

        # WHEN
        extractor._process_object_list(obj_list, "test_table", 1, "parent_id")

        # THEN
        assert len(extractor.tables["test_table"]) == 2
        assert extractor.tables["test_table"][0]["id"] == 1
        assert extractor.tables["test_table"][1]["id"] == 2

    def test_process_object_list_mixed_types(self):
        # GIVEN
        extractor = RelationalExtractor()
        obj_list = [
            {"id": 100, "name": "item1"},
            "string_item",  # Não é dict
            {"name": "item2"}
        ]

        # WHEN
        extractor._process_object_list(obj_list, "test_table", 1, "parent_id")

        # THEN
        assert len(extractor.tables["test_table"]) == 2  # String ignorada
        assert extractor.tables["test_table"][0]["id"] == 100
        assert extractor.tables["test_table"][1]["id"] == 1  # Auto-gerado


class TestAdvancedUtils:
    """Testes para a nova implementação da classe AdvancedUtils."""

    def test_process_with_relational_extraction_empty_data(self):
        # GIVEN
        raw_data = []

        # WHEN
        with patch('commons.advanced_utils.logging') as mock_logging:
            result = AdvancedUtils.process_with_relational_extraction(
                raw_data, "test_endpoint"
            )

        # THEN
        assert result == {}
        mock_logging.warning.assert_called_once_with(
            "Nenhum dado recebido para o endpoint test_endpoint"
        )

    def test_process_with_relational_extraction_basic(self):
        # GIVEN
        raw_data = [
            {
                "id": 1,
                "name": "Test",
                "tags": [{"id": 10, "name": "tag1"}]
            }
        ]

        # WHEN
        with patch.object(AdvancedUtils, 'process_and_save_data', return_value=raw_data) as mock_process, \
                patch('commons.advanced_utils.logging') as mock_logging:
            result = AdvancedUtils.process_with_relational_extraction(
                raw_data, "test_endpoint"
            )

        # THEN
        assert "test_endpoint" in result
        assert "test_endpoint_tags" in result
        mock_logging.info.assert_called()
        mock_process.assert_called()

    def test_process_with_relational_extraction_custom_parent_id_field(self):
        # GIVEN
        raw_data = [
            {
                "custom_id": "abc123",
                "items": [{"value": "test"}]
            }
        ]

        # WHEN
        with patch.object(AdvancedUtils, 'process_and_save_data', return_value=raw_data) as mock_process, \
                patch('commons.advanced_utils.logging'):
            result = AdvancedUtils.process_with_relational_extraction(
                raw_data, "test_endpoint", parent_id_field="custom_id"
            )

        # THEN
        assert "test_endpoint" in result
        assert "test_endpoint_items" in result
        mock_process.assert_called()

    def test_process_with_relational_extraction_compatibility_mode(self):
        # GIVEN
        raw_data = [{"id": 1, "name": "test"}]
        table_configs = {"some": "config"}  # Será ignorado
        extra_params = {"old_param": "value"}

        # WHEN
        with patch.object(AdvancedUtils, 'process_and_save_data', return_value=raw_data), \
                patch('commons.advanced_utils.logging') as mock_logging:
            result = AdvancedUtils.process_with_relational_extraction(
                raw_data, "test_endpoint",
                parent_id_field="id",
                table_configs=table_configs,
                auto_detect=False,
                **extra_params
            )

        # THEN
        assert "test_endpoint" in result
        mock_logging.info.assert_called()
        # Verificar se foi logado aviso sobre parâmetros ignorados
        info_calls = [call[0][0] for call in mock_logging.info.call_args_list]
        compatibility_warning = any("Parâmetros ignorados" in call for call in info_calls)
        assert compatibility_warning

    def test_process_with_relational_extraction_no_extracted_tables(self):
        # GIVEN
        raw_data = [{"id": 1, "name": "Simple data"}]  # Sem listas

        # WHEN
        with patch.object(AdvancedUtils, 'process_and_save_data', return_value=raw_data) as mock_process, \
                patch('commons.advanced_utils.logging'):
            result = AdvancedUtils.process_with_relational_extraction(
                raw_data, "test_endpoint"
            )

        # THEN
        assert result == {"test_endpoint": raw_data}
        mock_process.assert_called_once_with(raw_data, "test_endpoint")

    def test_process_with_relational_extraction_empty_tables_not_saved(self):
        # GIVEN
        raw_data = [{"id": 1, "empty_list": []}]  # Lista vazia

        # WHEN
        with patch.object(AdvancedUtils, 'process_and_save_data', return_value=raw_data) as mock_process, \
                patch('commons.advanced_utils.logging'):
            result = AdvancedUtils.process_with_relational_extraction(
                raw_data, "test_endpoint"
            )

        # THEN
        assert "test_endpoint" in result
        assert "test_endpoint_empty_list" not in result  # Tabela vazia não salva
        mock_process.assert_called_once()  # Só para a tabela principal

    def test_process_with_relational_extraction_process_error(self):
        # GIVEN
        raw_data = [{"id": 1, "tags": [{"name": "tag1"}]}]

        # WHEN & THEN
        with patch.object(AdvancedUtils, 'process_and_save_data', side_effect=Exception("Process error")), \
                patch('commons.advanced_utils.logging'):
            with pytest.raises(Exception, match="Process error"):
                AdvancedUtils.process_with_relational_extraction(
                    raw_data, "test_endpoint"
                )


class TestIntegrationNewImplementation:
    """Testes de integração para a nova implementação."""

    def test_complete_extraction_workflow(self):
        # GIVEN
        raw_data = [
            {
                "id": 1,
                "name": "Order 1",
                "items": [
                    {
                        "id": 10,
                        "product": "Product A",
                        "options": [
                            {"name": "color", "value": "red"},
                            {"name": "size", "value": "M"}
                        ]
                    },
                    {
                        "id": 20,
                        "product": "Product B",
                        "options": [
                            {"name": "color", "value": "blue"}
                        ]
                    }
                ]
            }
        ]

        # WHEN
        extractor = RelationalExtractor()
        result = extractor.extract_all_lists(raw_data, "orders")

        # THEN
        assert len(result.keys()) == 3
        assert "orders" in result
        assert "orders_items" in result
        assert "orders_items_options" in result

        # Verificar tabela principal
        assert len(result["orders"]) == 1
        assert "items" not in result["orders"][0]
        assert result["orders"][0]["name"] == "Order 1"

        # Verificar tabela de items
        assert len(result["orders_items"]) == 2
        assert "options" not in result["orders_items"][0]
        assert result["orders_items"][0]["product"] == "Product A"
        assert result["orders_items"][0]["orders_id"] == 1

        # Verificar tabela de options
        assert len(result["orders_items_options"]) == 3
        assert result["orders_items_options"][0]["name"] == "color"
        assert result["orders_items_options"][0]["orders_items_id"] == 10

    def test_deep_nested_extraction(self):
        # GIVEN
        raw_data = [
            {
                "id": 1,
                "level1": {
                    "level2": {
                        "level3": {
                            "deep_items": [
                                {
                                    "id": 100,
                                    "name": "Deep Item",
                                    "sub_items": [
                                        {"value": "sub1"},
                                        {"value": "sub2"}
                                    ]
                                }
                            ]
                        }
                    }
                }
            }
        ]

        # WHEN
        extractor = RelationalExtractor()
        result = extractor.extract_all_lists(raw_data, "main")

        # THEN
        assert "main" in result
        assert "main_deep_items" in result
        assert "main_deep_items_sub_items" in result
        assert len(result["main_deep_items"]) == 1
        assert len(result["main_deep_items_sub_items"]) == 2

    def test_mixed_data_types_handling(self):
        # GIVEN
        raw_data = [
            {
                "id": 1,
                "primitive_list": [1, 2, 3],  # Não será extraído
                "object_list": [{"id": 10}],  # Será extraído
                "mixed_list": [{"id": 20}, "string", 42],  # Será extraído
                "empty_list": [],  # Será ignorado
                "nested": {
                    "inner_objects": [{"value": "test"}]
                }
            }
        ]

        # WHEN
        extractor = RelationalExtractor()
        result = extractor.extract_all_lists(raw_data, "main")

        # THEN
        assert "main" in result
        assert "main_object_list" in result
        assert "main_mixed_list" in result
        assert "main_inner_objects" in result

        # Listas primitivas e vazias não geram tabelas
        assert "main_primitive_list" not in result
        assert "main_empty_list" not in result

        # Verificar que primitive_list permanece na tabela principal
        assert "primitive_list" in result["main"][0]

    def test_auto_id_generation_consistency(self):
        # GIVEN
        raw_data = [
            {
                "name": "Item 1",  # Sem ID
                "children": [
                    {"name": "Child 1"},  # Sem ID
                    {"name": "Child 2"}  # Sem ID
                ]
            },
            {
                "name": "Item 2",  # Sem ID
                "children": [
                    {"name": "Child 3"}  # Sem ID
                ]
            }
        ]

        # WHEN
        extractor = RelationalExtractor()
        result = extractor.extract_all_lists(raw_data, "main")

        # THEN
        # Verificar IDs auto-gerados sequenciais (contador global)
        # Item 1 = ID 1, Child 1 = ID 2, Child 2 = ID 3, Item 2 = ID 4, Child 3 = ID 5
        assert result["main"][0]["name"] == "Item 1"
        assert result["main"][1]["name"] == "Item 2"

        # Verificar foreign keys (referem-se aos IDs dos pais)
        assert result["main_children"][0]["main_id"] == 1  # Child 1 pertence ao Item 1
        assert result["main_children"][1]["main_id"] == 1  # Child 2 pertence ao Item 1
        assert result["main_children"][2]["main_id"] == 4  # Child 3 pertence ao Item 2 (que tem ID 4)

    def test_table_name_normalization(self):
        # GIVEN
        raw_data = [
            {
                "id": 1,
                "Custom Items": [{"value": "test1"}],
                "special-chars": [{"value": "test2"}],
                "UPPERCASE_LIST": [{"value": "test3"}]
            }
        ]

        # WHEN
        extractor = RelationalExtractor()
        result = extractor.extract_all_lists(raw_data, "main")

        # THEN
        assert "main_custom_items" in result
        assert "main_special_chars" in result
        assert "main_uppercase_list" in result

    def test_process_with_relational_extraction_full_workflow(self):
        # GIVEN
        raw_data = [
            {
                "id": 1,
                "name": "Test Entity",
                "tags": [{"id": 10, "name": "tag1"}],
                "metadata": {
                    "properties": [{"key": "prop1", "value": "val1"}]
                }
            }
        ]

        # WHEN
        with patch.object(AdvancedUtils, 'process_and_save_data', side_effect=lambda data, name: data) as mock_process, \
                patch('commons.advanced_utils.logging'):
            result = AdvancedUtils.process_with_relational_extraction(
                raw_data, "entities", parent_id_field="id"
            )

        # THEN
        assert "entities" in result
        assert "entities_tags" in result
        assert "entities_properties" in result
        assert mock_process.call_count == 3  # Uma chamada para cada tabela


class TestErrorHandlingNewImplementation:
    """Testes para tratamento de erros na nova implementação."""

    def test_extract_with_none_values(self):
        # GIVEN
        extractor = RelationalExtractor()
        data = [
            {
                "id": 1,
                "items": [
                    {"id": 10, "value": None},
                    {"id": 20, "value": "valid"}
                ]
            }
        ]

        # WHEN
        result = extractor.extract_all_lists(data, "main")

        # THEN
        assert len(result["main_items"]) == 2
        assert result["main_items"][0]["value"] is None
        assert result["main_items"][1]["value"] == "valid"

    def test_extract_with_circular_reference_protection(self):
        # GIVEN
        extractor = RelationalExtractor()
        # Criar estrutura que poderia causar recursão infinita
        data = [
            {
                "id": 1,
                "items": [
                    {
                        "id": 10,
                        "parent_ref": 1,  # Referência ao pai
                        "nested": {
                            "deep_items": [{"value": "test"}]
                        }
                    }
                ]
            }
        ]

        # WHEN
        result = extractor.extract_all_lists(data, "main")

        # THEN
        # Deve processar normalmente sem recursão infinita
        assert "main" in result
        assert "main_items" in result
        assert "main_items_deep_items" in result
        assert result["main_items"][0]["parent_ref"] == 1

    def test_extract_with_malformed_data(self):
        # GIVEN
        extractor = RelationalExtractor()
        data = [
            {
                "id": 1,
                "items": [
                    {"id": 10, "value": "valid"},
                    None,  # Item None
                    {"id": 20, "value": "valid2"}
                ]
            }
        ]

        # WHEN
        result = extractor.extract_all_lists(data, "main")

        # THEN
        # Deve processar itens válidos e ignorar None
        assert len(result["main_items"]) == 2
        assert result["main_items"][0]["value"] == "valid"
        assert result["main_items"][1]["value"] == "valid2"

    def test_process_with_relational_extraction_invalid_parent_id_field(self):
        # GIVEN
        raw_data = [
            {
                "name": "Item without specified ID field",
                "items": [{"value": "test"}]
            }
        ]

        # WHEN
        with patch.object(AdvancedUtils, 'process_and_save_data', side_effect=lambda data, name: data), \
                patch('commons.advanced_utils.logging'):
            result = AdvancedUtils.process_with_relational_extraction(
                raw_data, "test", parent_id_field="non_existent_field"
            )

        # THEN
        # Deve usar auto-geração de IDs
        assert "test" in result
        assert "test_items" in result

    def test_memory_usage_with_large_dataset(self):
        # GIVEN
        extractor = RelationalExtractor()
        # Simular dataset grande
        data = [
            {
                "id": i,
                "items": [
                    {"id": j, "value": f"item_{i}_{j}"}
                    for j in range(10)
                ]
            }
            for i in range(100)
        ]

        # WHEN
        result = extractor.extract_all_lists(data, "main")

        # THEN
        assert len(result["main"]) == 100
        assert len(result["main_items"]) == 1000
        # Verificar que não há vazamentos de memória nas estruturas internas
        assert len(extractor.tables) == 1  # Apenas main_items
