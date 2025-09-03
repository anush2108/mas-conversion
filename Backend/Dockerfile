# # backend/Dockerfile
# FROM --platform=linux/amd64 python:3.11-slim
# # Copy your app
# WORKDIR /app
# RUN apt-get update && apt-get install -y \
#     gcc g++ make curl unzip \
#     unixodbc-dev libodbc2 \
#     && rm -rf /var/lib/apt/lists/*
# # Install required packages for ibm_db
# RUN apt-get update && apt-get install -y gcc g++ make curl unzip && rm -rf /var/lib/apt/lists/*
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt
# # Upgrade pip and install ibm_db
# RUN pip install --upgrade pip setuptools wheel && pip install ibm_db
# # Copy the FastAPI app
# COPY . .
# EXPOSE 8000
# CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

# Use Python slim image
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Environment
ENV PORT=8000

# Expose port (Render sets $PORT)
EXPOSE $PORT
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]