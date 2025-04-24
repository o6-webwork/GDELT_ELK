FROM python:3.12-slim

# Install system build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    python3-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy all relevant files
COPY streamlitapp.py requirements.txt /app/

# Install Python dependencies
RUN pip install --upgrade pip setuptools wheel
RUN pip install --no-cache-dir -r requirements.txt

# Run the new Streamlit entry file
CMD ["streamlit", "run", "streamlitapp.py", "--server.port=8501", "--server.enableCORS=false"]
