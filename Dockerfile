FROM python:3.12.3

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

# Install DuckDB CLI
ARG DUCKDB_VERSION="0.10.0"

RUN case ${TARGETPLATFORM} in \
         "linux/amd64")  DUCKDB_FILE=https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb_cli-linux-amd64.zip  ;; \
         "linux/arm64")  DUCKDB_FILE=https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb_cli-linux-aarch64.zip  ;; \
    esac && \
    curl --output /tmp/duckdb.zip --location ${DUCKDB_FILE} && \
    unzip /tmp/duckdb.zip -d /usr/bin && \
    rm /tmp/duckdb.zip

# Create an application user
RUN useradd app_user --create-home

ARG APP_DIR="/opt/sidewinder"
RUN mkdir --parents ${APP_DIR} && \
    chown app_user:app_user ${APP_DIR}

USER app_user

WORKDIR ${APP_DIR}

# Setup a Python Virtual environment
ENV VIRTUAL_ENV=${APP_DIR}/.venv
RUN python3 -m venv ${VIRTUAL_ENV} && \
    echo ". ${VIRTUAL_ENV}/bin/activate" >> ~/.bashrc

# Set the PATH so that the Python Virtual environment is referenced for subsequent RUN steps (hat tip: https://pythonspeed.com/articles/activate-virtualenv-dockerfile/)
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

# Upgrade pip, setuptools, and wheel
RUN pip install --upgrade setuptools pip wheel

# Install the Sidewinder package (from source)
RUN pwd
COPY --chown=app_user:app_user pyproject.toml README.md LICENSE .
COPY --chown=app_user:app_user src ./src
RUN ls -ltr ./src

RUN pip install .

# Cleanup source code
RUN rm -rf pyproject.toml README.md ./src

# Run an integration test to ensure everything works
COPY --chown=app_user:app_user scripts ./scripts
RUN ./scripts/run_integration_tests.sh && \
    rm -rf ./scripts

# Open web-socket port
EXPOSE 8765
