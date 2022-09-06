FROM --platform=linux/amd64 python:3.8

# Update OS and install packages
RUN apt-get update --yes && \
    apt-get dist-upgrade --yes && \
    apt-get install --yes \
      screen \
      unzip \
      vim \
      zip

# Setup the AWS Client
WORKDIR /tmp

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -f awscliv2.zip

# Create an application user
RUN useradd app_user --create-home

USER app_user

# Update PATH
ARG LOCAL_BIN="/home/app_user/.local/bin"
ENV PATH="${PATH}:${LOCAL_BIN}"

RUN mkdir --parents ${LOCAL_BIN}

WORKDIR /home/app_user

# Install Python requirements
COPY --chown=app_user:app_user ./requirements.txt .

RUN pip install --upgrade pip && \
    pip install --requirement ./requirements.txt

# Copy source code files
COPY --chown=app_user:app_user . .

# Install DuckDB CLI
RUN curl --output /tmp/duckdb.zip --location https://github.com/duckdb/duckdb/releases/download/v0.5.0/duckdb_cli-linux-amd64.zip && \
    unzip /tmp/duckdb.zip -d ${LOCAL_BIN} && \
    rm /tmp/duckdb.zip

# Open web-socket port
EXPOSE 8765
