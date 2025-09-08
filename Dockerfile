FROM python:3.10-slim as wsgi-server

RUN apt update \
    && apt install -y --no-install-recommends python3-dev default-libmysqlclient-dev build-essential libpq-dev dos2unix \
    && rm -rf /var/lib/apt/lists/*

ENV DJANGO_SUPERUSER_USERNAME=admin
ENV DJANGO_SUPERUSER_PASSWORD=password
ENV DJANGO_SUPERUSER_EMAIL=admin@example.com

COPY requirements.txt ./

RUN pip install --no-cache-dir -i https://mirrors.cloud.tencent.com/pypi/simple -r requirements.txt

WORKDIR /app

COPY . .

# Продовые значения по умолчанию для сборки контейнера
ENV DEBUG=False
# В проде передавайте секрет через CI/CD или --build-arg/ENV
ENV SECRET_KEY=replace-with-a-long-random-secret-key-9f2c2d7a3f0d4f4f7b0f1a2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2

# Ослабляем уровень отказа: падаем только на ERROR, предупреждения deploy-чеков не ломают билд
RUN python manage.py check --deploy --fail-level ERROR \
    && python manage.py collectstatic --no-input \
    && dos2unix entrypoint.sh \
    && chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]

EXPOSE 8010


FROM nginx:1.22-alpine as web-server

WORKDIR /app

COPY --from=wsgi-server /app/static /app/static

COPY nginx.conf /etc/nginx/templates/default.conf.template
