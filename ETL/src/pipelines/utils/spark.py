"""Helpers for building reusable Spark docker commands."""
from __future__ import annotations

from typing import Mapping, Sequence


def build_spark_command(
    job_name: str,
    script_path: str,
    *,
    container_name: str,
    network: str,
    ui_port: int,
    volumes: Sequence[tuple[str, str]],
    image: str,
    packages: Sequence[str],
    pythonpath: Sequence[str] | None = None,
    pip_packages: Sequence[str] | None = None,
    spark_args: Sequence[str] | None = None,
    env: Mapping[str, str] | None = None,
) -> str:
    """Create a reusable docker command that launches a Spark job."""

    pythonpath = pythonpath or []
    pip_packages = pip_packages or []
    spark_args = spark_args or []
    env = env or {}

    pre_cleanup = [
        f"docker container stop {container_name} >/dev/null 2>&1 || true",
        f"docker container rm {container_name} >/dev/null 2>&1 || true",
        (
            "while [ \"$(docker ps -aq -f name={name})\" ]; do "
            "  echo 'Chờ container cũ được xoá...'; sleep 1; done"
        ).format(name=container_name),
    ]

    volume_flags = " ".join(f"-v {host}:{container}" for host, container in volumes)
    env_flags = " ".join(f"-e {key}={value}" for key, value in env.items())

    inner_commands: list[str] = []
    if pip_packages:
        inner_commands.append("pip install " + " ".join(pip_packages))

    if pythonpath:
        inner_commands.append(
            "export PYTHONPATH=$PYTHONPATH:" + ":".join(pythonpath)
        )

    spark_submit = [
        "spark-submit",
        "--packages",
        ",".join(packages),
        "--executor-memory",
        "2g",
        "--driver-memory",
        "2g",
        "--name",
        job_name,
    ]
    spark_submit.extend(spark_args)
    spark_submit.append(script_path)
    inner_commands.append(" ".join(spark_submit))

    docker_run = (
        f"docker run --rm --name {container_name} --network {network} -p {ui_port}:4040 "
        f"{env_flags} {volume_flags} {image} bash -c '"
        + " && ".join(inner_commands)
        + "'"
    ).strip()

    return " && ".join(pre_cleanup + [docker_run])


__all__ = ["build_spark_command"]
