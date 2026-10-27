FROM python:3.14-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pipeline/ pipeline/
COPY schema/ schema/
COPY consultas/ consultas/
COPY relatorio_contadores.py .

# Credenciais NUNCA entram na imagem: use --env-file .env ao executar.
CMD ["python", "-m", "pipeline.main"]
