.PHONY: lint clean

lint:
	flake8 . --exclude=.venv,__pycache__,*.pyc,*.pyo,*.pyd,build,dist

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete