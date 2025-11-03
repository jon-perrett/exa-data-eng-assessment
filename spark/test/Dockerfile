FROM eclipse-temurin:11
RUN apt-get update && apt-get install -y python3 python3-pip python3-venv \
    && ln -s /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*
ENV SPARK_MODE=local
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt --break-system-packages
COPY ./jobs/ ./jobs
COPY ./test/ ./test
ENV PYTHONPATH="/app/"
CMD ["pytest", "test"]
