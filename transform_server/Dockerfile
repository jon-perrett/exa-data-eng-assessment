FROM python:3.13-slim
COPY requirements.txt /tmp/requirements.txt
RUN apt update -y && apt install curl -y
RUN pip install -r /tmp/requirements.txt

WORKDIR /app
COPY src/ /app/
ENTRYPOINT gunicorn -b 0.0.0.0:8000 server:app
