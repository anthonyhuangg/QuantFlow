FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY src/backend/gateway/requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

COPY src/backend/gateway /app/gateway

COPY src/backend/server/market_data_pb2.py /app/server/market_data_pb2.py
COPY src/backend/server/market_data_pb2_grpc.py /app/server/market_data_pb2_grpc.py
RUN touch /app/server/__init__.py

EXPOSE 8001

ENV GRPC_HOST=server \
    GRPC_PORT=14000

CMD ["uvicorn", "gateway.main:app", "--host", "0.0.0.0", "--port", "8001"]
