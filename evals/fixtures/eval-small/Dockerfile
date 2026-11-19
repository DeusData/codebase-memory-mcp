FROM python:3.12-slim
WORKDIR /srv
COPY app ./app
ENV ORDER_ENV=prod
CMD ["python", "-m", "app.cli"]
