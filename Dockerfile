# Use Python 3.11 so AI Platform >=3.0 works
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements.txt first
COPY requirements.txt .

# Upgrade pip
RUN pip install --upgrade pip

# Install all dependencies except AI Platform
RUN pip install --no-cache-dir \
    streamlit==1.50.0 \
    pandas==2.3.3 \
    google-cloud-storage==3.4.1 \
    google-cloud-bigquery==3.38.0

# Install AI Platform last
RUN pip install --no-cache-dir google-cloud-aiplatform>=3.0.0

# Copy app code
COPY . .

# Expose Streamlit port
EXPOSE 8080

# Command to run your app
CMD ["streamlit", "run", "app.py", "--server.port", "8080", "--server.address", "0.0.0.0"]

