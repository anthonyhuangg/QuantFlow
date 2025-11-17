FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY src/backend/server/requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

COPY src/backend/server /app/server
COPY src/backend/config.json /app/config.json

EXPOSE 14000

CMD ["python", "-m", "server.server"]
