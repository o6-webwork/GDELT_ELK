# Use an official Python runtime as a parent image
FROM python:3.12-slim

# Set environment variables
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    # Define the default port (can be overridden by docker-compose)
    PORT=8000

# Set the working directory in the container
WORKDIR /app

# Install system dependencies if needed (e.g., for certain libraries)
# RUN apt-get update && apt-get install -y --no-install-recommends some-package && rm -rf /var/lib/apt/lists/*

# Copy the requirements file into the container
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code into the container
# Copy '.' means copy everything from the build context (backend/api/) into WORKDIR (/app)
COPY . .

# Make port ${PORT} available to the world outside this container
EXPOSE ${PORT}

# Define the command to run your app using uvicorn
# It will run the 'app' instance from the 'main.py' file
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]