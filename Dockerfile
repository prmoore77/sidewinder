FROM --platform=linux/amd64 python:3.8

# Update OS and install packages
RUN apt-get update --yes && \
    apt-get dist-upgrade --yes

# Setup the AWS Client
WORKDIR /tmp

curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

# Create an application user
RUN useradd app_user --create-home

USER app_user

WORKDIR /home/app_user

# Update PATH
ENV PATH="${PATH}:/home/app_user/.local/bin"

# Install Python requirements
COPY ./requirements.txt .

RUN pip install --upgrade pip && \
    pip install --requirement ./requirements.txt

# Copy source code files
COPY . .

# Open web-socket port
EXPOSE 8765
