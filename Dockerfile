# Use Python 3.11 slim for compatibility with google-cloud-aiplatform >= 2.23
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Upgrade pip and install dependencies
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Copy all project files
COPY . .

# Expose the port that Streamlit will run on
EXPOSE 8080

# Cloud Run supplies PORT environment variable
ENV PORT=8080

# Start Streamlit
ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=$PORT", "--server.address=0.0.0.0", "--server.headless=true"]
