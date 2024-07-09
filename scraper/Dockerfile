FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y \
        curl \
        chromium \
        chromium-driver \
        wget \
        unzip \
    && rm -rf /var/lib/apt/lists/*

RUN pip install poetry

ENV APP_HOME /app
ENV DOWNLOADS_DIR /app/downloads

RUN mkdir $APP_HOME
RUN mkdir $DOWNLOADS_DIR

COPY pyproject.toml $APP_HOME/pyproject.toml
COPY poetry.lock $APP_HOME/poetry.lock
COPY main.py $APP_HOME/main.py

WORKDIR $APP_HOME

RUN poetry install --no-dev

ENV CHROME_BIN /usr/bin/chromium
ENV CHROME_PATH /usr/lib/chromium/

CMD ["poetry", "run", "python", "main.py"]
