FROM python:3.12-slim

WORKDIR /app

COPY /reports-ui/backend/alert_generation.py ./shared_code/
COPY /reports-ui/backend/database.py ./shared_code/ 

RUN mkdir -p ./shared_code && touch ./shared_code/__init__.py

COPY /polling-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY /polling-service/polling_service.py .

ENV PYTHONPATH "${PYTHONPATH}:/app/shared_code"

CMD ["python", "polling_service.py"]