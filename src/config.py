from pydantic import BaseSettings
from pathlib import Path
import os


class LakeFSSettings(BaseSettings):
    host: str
    access_key_id: str
    secret_access_key: str

    class Config:
        env_file = os.path.join('..','.env')
