# Use a lightweight Python image
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt streamlit

COPY . .

EXPOSE 8080
ENV PORT=8080

# single string version — very important
CMD bash -c "streamlit run app.py --server.port=${PORT:-8080} --server.address=0.0.0.0 --server.headless=true"
