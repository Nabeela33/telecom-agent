# Use a lightweight official Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all project files
COPY . .

# Make entrypoint script executable
RUN chmod +x entrypoint.sh

# Cloud Run sets PORT dynamically; default to 8080
ENV PORT=8080

# Expose the port
EXPOSE 8080

# Run the app
ENTRYPOINT ["./entrypoint.sh"]
