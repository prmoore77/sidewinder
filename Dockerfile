FROM --platform=linux/amd64 python:3.8

RUN apt-get update --yes && \
    apt-get dist-upgrade --yes

RUN useradd app_user --create-home

USER app_user

WORKDIR /home/app_user

COPY ./requirements.txt .

ENV PATH="${PATH}:/home/app_user/.local/bin"

RUN pip install --upgrade pip && \
    pip install --requirement ./requirements.txt

COPY . .

EXPOSE 8765
