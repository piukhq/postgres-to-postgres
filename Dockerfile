FROM python:3.10

WORKDIR /app
ADD . .

RUN pip install pipenv && pipenv install --system --deploy --ignore-pipfile && \
    apt-get update && apt-get -y install postgresql-client && \
    rm -rf /var/lib/apt/lists/*

CMD [ "python", "main.py" ]
