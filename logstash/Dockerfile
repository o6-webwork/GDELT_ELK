FROM python:3.10

# Install OpenJDK 17 (required for PySpark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

# Set the JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Set the working directory
WORKDIR /app

# Copy the requirements.txt file into the container
COPY requirements.txt .

# Install the Python packages from requirements.txt
RUN pip install --upgrade pip  

RUN pip install --upgrade setuptools wheel

RUN pip install -r requirements.txt

COPY . .

# Runs the Python app
CMD ["python", "main.py"]