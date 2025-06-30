.PHONY: test lint clean check all docker-build docker-test

PYTHON_FILES := .
TEST_COMMAND := pytest --verbose --color=yes
FLAKE8_COMMAND := flake8 $(PYTHON_FILES)
DOCKER_IMAGE := airflow-tasks
DOCKER_TEST_CMD := docker run --rm $(DOCKER_IMAGE) pytest --verbose --color=yes

# Regra principal
all: check clean
	@echo "🎉 Tudo concluído com sucesso!"

# Verificar qualidade do código com flake8
lint:
	@echo "🔍 Verificando qualidade do código com flake8..."
	@$(FLAKE8_COMMAND)

# Rodar testes com pytest localmente (se tiver pytest instalado)
test:
	@echo "🧪 Executando testes com pytest..."
	@if command -v pytest > /dev/null; then \
	   $(TEST_COMMAND); \
	else \
	   echo "⚠️ pytest não encontrado. Use 'make docker-test' para executar testes no Docker"; \
	fi

# Pipeline completa (lint + test)
check: lint test
	@echo "✔️ Todos os testes e verificações passaram!"

# Limpar arquivos temporários
clean:
	@echo "🧹 Limpando arquivos temporários..."
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -rf .pytest_cache .mypy_cache .coverage output/

# Executa todos os main.py em crm-integration/**/main.py
run-all-crm:
	@echo "🚀 Executando todos os main.py em crm-integrations com seus respectivos .env..."
	@find crm-integrations -type f -name "main.py" | while read file; do \
		dir=$$(dirname "$$file"); \
		echo "➡️ Executando $$file..."; \
		( cd "$$dir" && PYTHONPATH=../.. python main.py ); \
	done

run-all-crm-parallel:
	@echo "🚀 Executando main.py de todas integrações em paralelo (até 4 ao mesmo tempo)..."
	@find crm-integrations -type f -name "main.py" | \
		xargs -I {} -P 12 bash -c ' \
			echo "➡️ Executando {}"; \
			cd "`dirname {}`" && PYTHONPATH=../.. python main.py \
		'
