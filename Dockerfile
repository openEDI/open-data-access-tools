FROM node:16-bullseye-slim

# Setup environment variables
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONDONTWRITEBYTECODE=TRUE
ENV PYTHONUNBUFFERED=TRUE

# Install system dependencies
RUN apt-get update -y --fix-missing && \
    apt-get install -y --no-install-recommends \
    build-essential \
    python3 \
    python3-dev \
    python3-pip

# Set Python3 alias as Python
RUN echo 'alias python=python3' >> ~/.bashrc

# Install AWS CDK
RUN npm install -g aws-cdk@1.x
RUN cdk --version

# Install Python packages
WORKDIR /open-data-access-tools
COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Setup OEDI environment
COPY . .
RUN pip install -e .
