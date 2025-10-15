# Use a lightweight Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt streamlit

# Copy all project files
COPY . .

# Expose Streamlit default port
EXPOSE 8080

# Cloud Run supplies PORT; default to 8080 when local
ENV PORT=8080

# use bash so $PORT expands correctly
ENTRYPOINT ["bash","-c"]
CMD ["streamlit run app.py --server.port=$PORT --server.address=0.0.0.0 --server.headless=true"]
