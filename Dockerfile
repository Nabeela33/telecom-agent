# Use a lightweight Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all project files
COPY . .

# Expose Streamlit default port
EXPOSE 8080

# Run Streamlit
CMD ["streamlit", "run", "app.py", "--server.port=$PORT", "--server.address=0.0.0.0", "--server.headless=true"]
