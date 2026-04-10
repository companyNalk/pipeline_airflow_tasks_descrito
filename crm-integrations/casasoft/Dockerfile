FROM python:3.11-slim
WORKDIR /app

COPY crm-integrations/casasoft/main.py .
COPY crm-integrations/casasoft/requirements.txt .

COPY commons/ /app/commons/
COPY generic/ /app/generic/

RUN pip install --no-cache-dir -r requirements.txt
RUN mkdir -p /app/output && chmod 777 /app/output

CMD ["python", "main.py"]
