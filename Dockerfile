FROM python:3.12.5-slim-bullseye

ENV PYTHONPATH ="/app/"

RUN pip install 'poetry==1.8.3'
RUN poetry config virtualenvs.create false
RUN mkdir /app

COPY miro_backup.py /app
COPY poetry.lock /app
COPY pyproject.toml /app

WORKDIR /app

RUN poetry install

ENTRYPOINT ["python", "miro_backup.py"]
