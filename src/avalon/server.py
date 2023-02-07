from typing import Union
from functools import lru_cache

import uvicorn
from fastapi import FastAPI
from avalon.operations.LakeFsWrapper import LakeFsWrapper
import lakefs_client
# from src.avalon import config
from avalon.models.pipeline import PipelineInstance

app = FastAPI()

@lru_cache()
def get_lakefs_config():
    local_config = config.LakeFSSettings()
    configuration = lakefs_client.Configuration()
    configuration.username = local_config.access_key_id
    configuration.password = local_config.secret_access_key
    configuration.host = local_config.host
    return configuration


client = LakeFsWrapper(configuration=get_lakefs_config())


@app.get("/pipeline")
def list_pipelines(repo_name: str, branch: str = "main") -> PipelineInstance:
    return client.get_pipeline_commits(repository_name=repo_name, branch_name=branch)



if __name__ == '__main__':
    uvicorn.run(app, port=8002)