FROM python:3.12-slim

WORKDIR /usr/app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY profiles.yml.example /root/.dbt/profiles.yml
COPY . /usr/app

ENV DBT_PROFILES_DIR=/root/.dbt
ENTRYPOINT ["dbt"]
CMD ["debug"]
