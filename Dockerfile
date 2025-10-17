# Use Python 3.11 slim
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy requirements
COPY requirements.txt .

# Upgrade pip and install dependencies
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Copy all project files
COPY . .

# Expose port 8080
EXPOSE 8080

# Set PORT environment variable (Cloud Run uses this)
ENV PORT=8080

# Run Streamlit
ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=$PORT", "--server.address=0.0.0.0", "--server.headless=true"]
