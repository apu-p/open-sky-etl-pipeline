"""Minimal Superset config for the local stack.

Superset's own metadata lives in a SQLite file on the persisted superset_home
volume (fine for a local demo). The warehouse connection (SUPERSET_DSN,
authenticating as the least-privilege superset_app login — Section 13) is added
once from the Superset UI: Settings -> Database Connections -> + Database, paste
$SUPERSET_DSN. See superset/README.md for the per-chart SQL."""
import os

# Required. Sourced from .env via the container environment.
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "change_me_generate_a_long_random_string")

# Superset metadata DB (not the warehouse) — SQLite on the persisted volume.
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
}

# Let a dashboard filter span a reasonable row count for this demo's scale.
ROW_LIMIT = 50000
SQLLAB_TIMEOUT = 120
