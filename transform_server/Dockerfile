FROM python:3.13-slim

# requirements as an early layer as should be slow moving
COPY requirements.txt /tmp/requirements.txt

# install curl so we can do our Docker healthcheck
RUN apt update -y && apt install curl -y
RUN pip install -r /tmp/requirements.txt

# copy source code and configure gunicorn web server
WORKDIR /app
COPY src/ /app/
ENTRYPOINT gunicorn -w 8 -b 0.0.0.0:8000 server:app
