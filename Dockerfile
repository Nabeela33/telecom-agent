# Use Python 3.11
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements.txt first
COPY requirements.txt .

# Upgrade pip
RUN pip install --upgrade pip

# Install dependencies except AI Platform
RUN pip install --no-cache-dir -r requirements.txt

# Install AI Platform last
RUN pip install --no-cache-dir google-cloud-aiplatform>=3.0.0

# Copy app code
COPY . .

# Expose port for Cloud Run
EXPOSE 8080

# Run Streamlit in headless mode
CMD ["streamlit", "run", "app.py", "--server.port", "8080", "--server.address", "0.0.0.0", "--server.headless", "true"]
