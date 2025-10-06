# File: Dockerfile (Create this file in your repository root)

# Use the official Python base image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
# This is where your long list of pip packages gets installed
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code (including main.py)
COPY . .

# Set the environment variable for your BQ key path
ENV BQ_KEY_FILE="we_are_hipaa_smart_google_key.json"

# Set the entry point to your Python function (run_pipeline)
# The Cloud Run service itself will send the POST request to trigger this.
ENTRYPOINT ["gunicorn", "--bind", "0.0.0.0:8080", "main:run_pipeline"]