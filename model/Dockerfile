# Dockerfile for model service
FROM python:3.10-slim
 
WORKDIR /app
 
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
 
COPY . .
 
CMD ["python", "model_training.py"]
