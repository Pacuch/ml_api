# Dockerfile
FROM python:3.12-slim

WORKDIR /code

# 1. Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2. Copy the application code
COPY ./app ./app

# 3. Command to run the application
# We use host 0.0.0.0 so it is accessible outside the container
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8512"]