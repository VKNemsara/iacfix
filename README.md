# Antigravity Playbook Fixer

A web application that automatically detects and semantically fixes Ansible playbooks using the Antigravity CLI, validated by `ansible-lint` and `checkov`, and tested with a real Docker-based deployment.

---

## Quick Start

### Prerequisites

- **Docker** and **Docker Compose** installed on the host
- An **Antigravity CLI (`agy`)** account

### 1. Clone / copy this directory

```bash
cd iacfix
```

### 2. Build and start

```bash
docker compose up --build
```

The first build takes ~5–8 minutes (downloading ansible, checkov, etc.).

### 3. Open the web UI

```
http://localhost:8080
```

### 4. Sign in

Click **"Sign in with Antigravity CLI"** — an OAuth URL will appear.
Open it in your browser, complete the login, and the UI will automatically advance.

### 5. Upload and fix

Drag-and-drop your Ansible playbook (`.yml` / `.yaml`) and click **"Analyze & Fix"**.
Watch the real-time pipeline:

| Stage | What happens |
|---|---|
| **Prepare** | Pre-builds the Ansible test container image |
| **Initial Checks** | `ansible-lint` + `checkov` run in parallel |
| **AGY Fix Loop** | Issues sent to Antigravity for semantic fixing (up to 5 tries) |
| **Re-validation** | Fixed playbook re-checked by lint + checkov |
| **Deploy Test** | Runs `ansible-playbook` inside a Docker container |
| **Result** | Download the verified playbook |

---

## Architecture

```
Browser ──SSE──▶ FastAPI (port 8080)
                    │
                    ├── agy CLI (stdin pipe)         ← semantic fixes
                    ├── ansible-lint (subprocess)    ← lint check
                    ├── checkov (subprocess)         ← security check
                    └── Docker SDK                  ← deployment test
                              │
                              └── sibling container (ansible-test-env image)
                                        └── ansible-playbook -i localhost, -c local
```

**Key design points:**
- Docker socket is mounted (`/var/run/docker.sock`) — no Docker-in-Docker
- Auth tokens persist in named volumes across restarts
- All pipeline events stream to the browser via Server-Sent Events
- Max 5 lint/fix iterations, max 3 deploy-fix iterations (configurable in `pipeline.py`)

---

## Directory Structure

```
playbook-fixer/
├── backend/
│   ├── main.py           # FastAPI app + all HTTP/SSE routes
│   ├── pipeline.py       # Async pipeline state machine
│   ├── agy_client.py     # Antigravity CLI wrapper (auth + fix prompts)
│   ├── linters.py        # ansible-lint + checkov subprocess wrappers
│   ├── docker_runner.py  # Docker deployment test runner
│   └── requirements.txt
├── frontend/
│   ├── index.html        # Single-page app shell
│   ├── app.js            # SSE client + UI logic
│   └── styles.css        # Dark blueprint theme
├── ansible-test-env/
│   └── Dockerfile        # Minimal Ansible execution environment
├── Dockerfile            # Main app image
└── docker-compose.yml    # Recommended launcher
```

---

## Configuration

All tunable constants are at the top of their respective files:

| File | Constant | Default | Description |
|---|---|---|---|
| `pipeline.py` | `MAX_FIX_ITERATIONS` | `5` | Max lint/checkov fix loops |
| `pipeline.py` | `MAX_DEPLOY_FIX_ITERATIONS` | `3` | Max deploy-fix loops |
| `docker_runner.py` | `ANSIBLE_TEST_IMAGE` | `playbook-fixer-ansible-test:latest` | Test image name |
| `agy_client.py` | timeout in `_run_agy_prompt` | `300s` | AGY response timeout |

---

## Troubleshooting

**`agy` not found**
The Dockerfile tries the official installer URL. If your environment can't reach it, mount your host's `agy` binary:
```yaml
volumes:
  - /usr/local/bin/agy:/usr/local/bin/agy:ro
```

**Docker permission denied**
Ensure the Docker socket is accessible:
```bash
ls -la /var/run/docker.sock
# Should show: srw-rw---- ... root docker
# Add your user to the docker group or run with sudo
```

**Auth tokens lost on restart**
Named volumes `agy_auth` and `agy_config` persist tokens.
If you recreate volumes (`docker compose down -v`), you'll need to sign in again.

**Playbook targets remote hosts**
The deploy test runs `ansible-playbook -i localhost, -c local`.
Playbooks targeting named hosts in an inventory file will need adjustment — add an optional **inventory file upload** field in the UI and pass `-i /workspace/inventory` to the deploy command in `docker_runner.py`.
