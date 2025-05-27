FROM python:3.13-slim

# Set the working directory
WORKDIR /app

# Copy the requirements.txt file into the container
COPY requirements.txt .

# Install the Python packages from requirements.txt
RUN pip install --upgrade pip 
RUN pip install -r requirements.txt

COPY . .

EXPOSE 7979

CMD ["python", "app.py"]