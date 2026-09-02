"""
docker_runner.py — Docker-based Ansible playbook deployment tester

Strategy:
  - Pre-build a lightweight 'ansible-test-env' image at app startup.
  - For each test, run a sibling container (via mounted Docker socket).
  - Mount the playbook file into /workspace/playbook.yml.
  - Run: ansible-playbook -i localhost, -c local /workspace/playbook.yml
  - Capture logs + exit code.
  - Tear down the container immediately after.
"""

import asyncio
import logging
import os
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

import docker
from docker.errors import BuildError, ContainerError, ImageNotFound, APIError

logger = logging.getLogger(__name__)

ANSIBLE_TEST_IMAGE = "playbook-fixer-ansible-test:latest"
CONTAINER_LABEL = "playbook-fixer-test"

# Path to the test env Dockerfile (relative to project root, mounted as /app)
_DOCKERFILE_DIR = Path("/app/ansible-test-env")


@dataclass
class DeployResult:
    success: bool
    logs: str
    exit_code: int
    error: str = ""


def _get_docker_client() -> docker.DockerClient:
    return docker.from_env(timeout=120)


async def ensure_test_image() -> str:
    """
    Build the ansible-test-env image if it doesn't exist yet.
    Returns the image tag.
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _ensure_test_image_sync)


def _ensure_test_image_sync() -> str:
    client = _get_docker_client()
    try:
        client.images.get(ANSIBLE_TEST_IMAGE)
        logger.info("ansible-test image already exists")
        return ANSIBLE_TEST_IMAGE
    except ImageNotFound:
        pass

    logger.info("Building ansible-test-env image...")
    dockerfile_path = str(_DOCKERFILE_DIR)

    # Fallback: use inline Dockerfile if directory not found
    if not _DOCKERFILE_DIR.exists():
        dockerfile_content = _INLINE_DOCKERFILE
        with tempfile.TemporaryDirectory() as tmpdir:
            df_path = Path(tmpdir) / "Dockerfile"
            df_path.write_text(dockerfile_content)
            image, build_logs = client.images.build(
                path=tmpdir,
                tag=ANSIBLE_TEST_IMAGE,
                rm=True,
                forcerm=True,
            )
    else:
        image, build_logs = client.images.build(
            path=dockerfile_path,
            tag=ANSIBLE_TEST_IMAGE,
            rm=True,
            forcerm=True,
        )

    for log in build_logs:
        if "stream" in log:
            logger.debug(log["stream"].strip())

    logger.info("ansible-test image built successfully")
    return ANSIBLE_TEST_IMAGE


_INLINE_DOCKERFILE = """\
FROM python:3.12-slim
RUN pip install --no-cache-dir ansible==9.* ansible-core==2.16.*
RUN apt-get update && apt-get install -y --no-install-recommends openssh-client && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace
"""


async def run_deployment_test(playbook_path: str | Path) -> DeployResult:
    """
    Run the playbook inside the test container and return the result.
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _run_deployment_sync, str(playbook_path))


def _run_deployment_sync(playbook_path: str) -> DeployResult:
    client = _get_docker_client()
    container = None
    playbook_abs = Path(playbook_path).resolve()

    try:
        container = client.containers.run(
            image=ANSIBLE_TEST_IMAGE,
            command=[
                "ansible-playbook",
                "-i", "localhost,",
                "-c", "local",
                "/workspace/playbook.yml",
                "-v",
            ],
            volumes={
                str(playbook_abs): {
                    "bind": "/workspace/playbook.yml",
                    "mode": "ro",
                }
            },
            environment={
                "ANSIBLE_HOST_KEY_CHECKING": "False",
                "ANSIBLE_RETRY_FILES_ENABLED": "False",
                "ANSIBLE_STDOUT_CALLBACK": "yaml",
                "PYTHONUNBUFFERED": "1",
            },
            detach=False,           # wait for completion
            remove=False,           # we remove manually after logging
            labels={"app": CONTAINER_LABEL},
            network_mode="bridge",
            mem_limit="512m",
            cpu_period=100000,
            cpu_quota=50000,        # 50% of one CPU
        )
        # container is bytes of logs when detach=False
        logs = container.decode("utf-8", errors="replace") if isinstance(container, bytes) else ""
        return DeployResult(success=True, logs=logs, exit_code=0)

    except ContainerError as e:
        logs = e.stderr.decode("utf-8", errors="replace") if isinstance(e.stderr, bytes) else str(e.stderr)
        stdout_logs = ""
        # Also try to get stdout
        if e.container:
            try:
                all_logs = e.container.logs(stdout=True, stderr=True).decode("utf-8", errors="replace")
                stdout_logs = all_logs
            except Exception:
                pass
        combined = (stdout_logs + "\n" + logs).strip()
        return DeployResult(
            success=False,
            logs=combined,
            exit_code=e.exit_status,
            error=combined,
        )
    except ImageNotFound:
        return DeployResult(
            success=False,
            logs="",
            exit_code=-1,
            error=f"Image {ANSIBLE_TEST_IMAGE} not found. Rebuild required.",
        )
    except APIError as e:
        return DeployResult(
            success=False,
            logs="",
            exit_code=-1,
            error=f"Docker API error: {e}",
        )
    finally:
        # Clean up the container
        if container is not None and not isinstance(container, bytes):
            try:
                container.remove(force=True)
            except Exception:
                pass


async def cleanup_test_containers():
    """Remove any leftover test containers (e.g., after a crash)."""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _cleanup_sync)


def _cleanup_sync():
    try:
        client = _get_docker_client()
        containers = client.containers.list(
            all=True, filters={"label": f"app={CONTAINER_LABEL}"}
        )
        for c in containers:
            try:
                c.remove(force=True)
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"Cleanup error: {e}")
