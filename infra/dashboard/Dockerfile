FROM python:3.11-slim-bookworm
WORKDIR /opt/retail
COPY infra/dashboard/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --retries 5 --timeout 180 -r /tmp/requirements.txt
CMD ["streamlit", "run", "dashboard/app.py", "--server.address=0.0.0.0", "--server.port=8501"]
