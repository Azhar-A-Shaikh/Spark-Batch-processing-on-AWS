FROM python:3.8
USER root

WORKDIR /app

COPY . ./

RUN pip install -r requirements.txt