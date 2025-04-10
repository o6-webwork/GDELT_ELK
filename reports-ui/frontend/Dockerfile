FROM python:3.13-slim

WORKDIR /app

COPY frontend/ /app

RUN pip install --no-cache-dir streamlit

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.enableCORS=false"]
