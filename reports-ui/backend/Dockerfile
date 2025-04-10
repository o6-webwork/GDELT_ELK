FROM python:3.13-slim

WORKDIR /app

COPY backend/ /app

RUN pip install --no-cache-dir fastapi uvicorn pandas

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
