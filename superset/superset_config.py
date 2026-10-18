import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# Persist Superset's own metadata in the named Docker volume.
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"