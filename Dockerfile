FROM python:3.12-slim

# ── System dependencies ───────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        git \
        openssh-client \
        docker.io \
    && rm -rf /var/lib/apt/lists/*

# ── Python tools ─────────────────────────────────────────────────────────────
# Ansible + ansible-lint via pip (more up-to-date than apt)
RUN pip install --no-cache-dir \
        ansible-core==2.17.* \
        ansible-lint==24.* \
        checkov

# ── App Python dependencies ───────────────────────────────────────────────────
COPY backend/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# ── AGY CLI ───────────────────────────────────────────────────────────────────
# Try the official installer; fall back to a no-op so docker build doesn't
# break in environments without internet (e.g., sandbox). The app will still
# run and agy errors surface cleanly in the UI.
RUN curl -sSfL https://dl.antigravity.google/cli/install.sh | bash 2>/dev/null \
    || echo "INFO: agy installer not reachable — install manually or mount from host"

# Make sure agy is on PATH wherever the installer put it
ENV PATH="/root/.local/bin:/root/.antigravity/bin:${PATH}"

# ── Application files ─────────────────────────────────────────────────────────
WORKDIR /app
COPY backend/   /app/
COPY frontend/  /app/static/
COPY ansible-test-env/ /app/ansible-test-env/

# ── Session storage ───────────────────────────────────────────────────────────
RUN mkdir -p /tmp/playbook-fixer-sessions
ENV SESSIONS_DIR=/tmp/playbook-fixer-sessions

# ── Healthcheck ───────────────────────────────────────────────────────────────
HEALTHCHECK --interval=15s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

EXPOSE 8080

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080", "--log-level", "info"]
