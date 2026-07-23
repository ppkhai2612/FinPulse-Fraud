"""Helpers for the Finpulse Airflow DAGs"""
import docker


def run_in(container_name: str, command, *, log_tail: int = 40) -> str:
    """Execute a command in a running container, raise on non-zero exit
    
    command can be str or list
    """
    client = docker.from_env()
    container = client.containers.get(container_name)
    exit_code, output = container.exec_run(command, demux=False)
    if exit_code != 0:
        raise RuntimeError(
            f"Command in {container_name} exited {exit_code}: {command}"
        )

    text = output.decode(encoding='utf-8', errors='replace') if output else ""
    lines = text.splitlines()
    tail = "\n".join(lines[-log_tail:]) if len(lines) > log_tail else text
    print(f"[{container_name}] exit={exit_code}\n{tail}")
    return text


def spark_submit(job_path: str, packages: str | None = None) -> list[str]:
    """Build a spark-submit argv for the standalone master"""
    cmd = [
        "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077"
    ]
    if packages:
        cmd += ["--packages", packages]
    cmd.append(job_path)
    return cmd