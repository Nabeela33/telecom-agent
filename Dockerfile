# Use a lightweight Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all project files (app, clients, utils, mapping txt if needed)
COPY . .

# Make entrypoint executable
RUN chmod +x entrypoint.sh

# Cloud Run sets PORT dynamically; default to 8080
ENV PORT=8080

# Expose the port for Cloud Run
EXPOSE 8080

# Use entrypoint
ENTRYPOINT ["./entrypoint.sh"]
