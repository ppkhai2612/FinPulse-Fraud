from pathlib import Path
import sys
import subprocess
import shlex


REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
REPLICATION_FACTOR = 2
DATASETS = [
    ("customer-profiles.json.gz", "/landing/customer-profiles"),
    ("merchant-directory.csv.gz", "/landing/merchant-directory"),
    ("fraud-reports.json.gz", "/landing/fraud-reports"),
    ("device-fingerprints.csv.gz", "/landing/device-fingerprints"),
]


def run(cmd: list[str], *, why: str, check: bool = True) -> subprocess.CompletedProcess:
    """Run a shell command, also showing the command and its purpose

    Args:
        cmd (list[str]): Command to run
        why (str): Describing what does the command do
        check (bool): Determining whether CalledProcessError will be raised

    Returns:
        subprocess.CompletedProcess: A compteled subprocess
    """
    print(f"$ {shlex.join(cmd)}")
    print(f"  -> {why}")
    result = subprocess.run(cmd, check=check, capture_output=True, text=True) # run subprocess
    if result.stdout.strip(): # print out STDOUT
        for line in result.stdout.rstrip().splitlines():
            print(f"{line}")
    return result


def nn(*args: str, why: str) -> subprocess.CompletedProcess:
    """Run a command inside the namenode container

    Args:
        *args: Command and its arguments to execute inside the namenode container
        why (str): Describing what does the command do

    Returns:
        subprocess.CompletedProcess: A compteled subprocess
    """
    return run(["docker", "compose", "exec", "-T", "namenode", *args], why=why)


def hdfs_exists(path: str) -> bool:
    """Check if the file is in HDFS

    Args:
        path (str): The path to the file in HDFS

    Returns:
        bool: True if the file exists in HDFS, otherwise False
    """
    result = run(
        [
            "docker", "compose", "exec", "-T", "namenode",
            "hdfs", "dfs", "-test", "-e", path
        ],
        why=f"Check whether {path} already exists in HDFS (if yes, return 0)",
        check=False # CalledProcessError exception will not be raised when the process exits with a non-zero exit code
    )
    return result.returncode == 0


def land_to_hdfs(filename: str, hdfs_dir: str, rep: int = REPLICATION_FACTOR) -> None:
    """Landing data files to HDFS

    Args:
        filename (str): Name of the file
        hdfs_dir (str): The directory containing file in HDFS
        rep (int, optional): The number of file replicas on the DataNodes. Defaults to REPLICATION_FACTOR
    """

    # check if data files in local fs
    local_file = DATA_DIR / filename
    if not local_file.exists():
        sys.exit(f"Missing local file: {local_file}")

    # check if HDFS exists,
    hdfs_path = f"{hdfs_dir}/{filename}"

    if hdfs_exists(hdfs_path):
        print(f"[SKIP] {hdfs_path} already exists in HDFS")
    else:
        nn(
            "hdfs", "dfs", "-mkdir", "-p", hdfs_dir,
            why=f"Create {hdfs_dir} (and any missing parent dirs) in HDFS"
        )
        run(
            ["docker", "compose", "cp", str(local_file), f"namenode:/tmp/{filename}"],
            why=f"Copy {filename} from host into the namenode container's /tmp"
        )
        nn(
            "hdfs", "dfs", "-put", f"/tmp/{filename}", hdfs_dir + "/",
            why=f"Copy /tmp/{filename} into {hdfs_dir}/ (in HDFS)"
        )
        nn(
            "rm", f"/tmp/{filename}",
            why="Remove the temporary copy"
        )

    # setrep is idempotent - safe to re-apply on every run
    nn(
        "hdfs", "dfs", "-setrep", str(rep), hdfs_dir,
        why=f"Set the replication factor of {hdfs_dir} to {str(rep)}",
    )


def main():
    print(f"Landing {len(DATASETS)} datasets to HDFS...")
    for filename, hdfs_dir in DATASETS:
        print(f"\nLanding {filename} -> {hdfs_dir}")
        land_to_hdfs(filename, hdfs_dir)


if __name__== "__main__":
    main()