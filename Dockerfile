FROM python:3.6-jessie

LABEL author="Rodrigo L. Gil"

COPY requirements.txt /code/requirements.txt

RUN apt-get update && pip install -r /code/requirements.txt
ENV LANG en_US.UTF-8

COPY auth/ /code/auth/
COPY dimension/ /code/dimension/
COPY fact/ /code/fact/
COPY raw/ /code/raw/
COPY util/ /code/util/
COPY *.py /code/

WORKDIR /code/