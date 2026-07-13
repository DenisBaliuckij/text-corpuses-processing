FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        gnupg2 \
        apt-transport-https \
        unixodbc-dev \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
        | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] \
https://packages.microsoft.com/debian/12/prod bookworm main" \
        > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY webui/requirements.txt /tmp/requirements-webui.txt
RUN pip install --no-cache-dir -r /tmp/requirements-webui.txt

COPY webui/ /app/webui/

CMD ["gunicorn", "--chdir", "webui", "--bind", "0.0.0.0:5000", "app:app"]
