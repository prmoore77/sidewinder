FROM --platform=linux/amd64 python:3.8

# Update OS and install packages
RUN apt-get update --yes && \
    apt-get dist-upgrade --yes

# Setup the AWS Client
WORKDIR /tmp

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install

# Create an application user
RUN useradd app_user --create-home

USER app_user

WORKDIR /home/app_user

# Update PATH
ARG LOCAL_BIN="/home/app_user/.local/bin"
ENV PATH="${PATH}:${LOCAL_BIN}"

# Install DuckDB CLI
RUN mkdir --parents "${LOCAL_BIN}" && \
    curl --location https://github.com/duckdb/duckdb/releases/download/v0.4.0/duckdb_cli-linux-amd64.zip --output /tmp/duckdb.zip && \
    unzip /tmp/duckdb.zip -d ${LOCAL_BIN}

# Install Python requirements
COPY ./requirements.txt .

RUN pip install --upgrade pip && \
    pip install --requirement ./requirements.txt

# Copy source code files
COPY . .

# Open web-socket port
EXPOSE 8765
