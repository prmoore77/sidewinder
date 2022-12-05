FROM python:3.11

ARG TARGETPLATFORM
ARG TARGETARCH
ARG TARGETVARIANT
RUN printf "I'm building for TARGETPLATFORM=${TARGETPLATFORM}" \
    && printf ", TARGETARCH=${TARGETARCH}" \
    && printf ", TARGETVARIANT=${TARGETVARIANT} \n" \
    && printf "With uname -s : " && uname -s \
    && printf "and  uname -m : " && uname -m

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

RUN case ${TARGETPLATFORM} in \
         "linux/amd64")  AWSCLI_FILE=https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip  ;; \
         "linux/arm64")  AWSCLI_FILE=https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip  ;; \
    esac && \
    curl "${AWSCLI_FILE}" -o "awscliv2.zip" && \
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

ARG DUCKDB_VERSION="0.6.0"

# Install DuckDB CLI
RUN case ${TARGETPLATFORM} in \
         "linux/amd64")  DUCKDB_FILE=https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb_cli-linux-amd64.zip  ;; \
         "linux/arm64")  DUCKDB_FILE=https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb_cli-linux-aarch64.zip  ;; \
    esac && \
    curl --output /tmp/duckdb.zip --location ${DUCKDB_FILE} && \
    unzip /tmp/duckdb.zip -d ${LOCAL_BIN} && \
    rm /tmp/duckdb.zip

# Open web-socket port
EXPOSE 8765
