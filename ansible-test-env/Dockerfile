FROM python:3.12-slim

# Minimal Ansible execution environment for deployment testing
RUN pip install --no-cache-dir \
        "ansible-core==2.17.*" \
        "ansible==9.*"

RUN apt-get update && apt-get install -y --no-install-recommends \
        openssh-client \
        rsync \
    && rm -rf /var/lib/apt/lists/*

# Create non-root ansible user for safer execution
RUN useradd -ms /bin/bash ansible

WORKDIR /workspace
USER ansible

# Disable host key checking by default
ENV ANSIBLE_HOST_KEY_CHECKING=False
ENV ANSIBLE_RETRY_FILES_ENABLED=False
ENV ANSIBLE_STDOUT_CALLBACK=yaml
ENV PYTHONUNBUFFERED=1
