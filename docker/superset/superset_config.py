import os
from sqlalchemy.pool import NullPool

# Metadata DB. SQLite file lives on the `superset-data` named volume so it
# survives container restarts but is wiped by `make nuke`.
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db?check_same_thread=False"

SQLALCHEMY_ENGINE_OPTIONS = {
    "connect_args": {
        "check_same_thread": False,  # disable thread prohibition for SQLite
        "timeout": 30,
    },
    "poolclass": NullPool,  # each request/thread will create a new connection and close it as soon as it's finished
}

# Required by Flask. Pulled from env (set in docker-compose.yml) so the value
# isn't hard-coded into the repo. Dev only — rotate for any non-local use.
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "dev-secret-change-me")

# Superset's web server sits behind no proxy in this setup; disable the
# X-Forwarded-* trust to silence a warning at boot.
ENABLE_PROXY_FIX = False

# Don't surface the example dashboards / dummy data in the UI.
FEATURE_FLAGS = {}