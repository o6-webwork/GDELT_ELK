# gdelt_app/frontend/Dockerfile

# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the frontend application code into the container at /app
COPY . .

# Make port 8501 available to the world outside this container (Streamlit default port)
EXPOSE 8501

# Define environment variable (optional, can also be set in docker-compose.yml)
# ENV STREAMLIT_SERVER_PORT=8501

# Set the default command to run when the container starts
# Use healthcheck for streamlit: https://docs.streamlit.io/knowledge-base/tutorials/deploy/docker#add-a-healthcheck
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

CMD ["streamlit", "run", "analysis.py", "--server.port=8501", "--server.address=0.0.0.0"]