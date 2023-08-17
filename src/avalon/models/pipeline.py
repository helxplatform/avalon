from dataclasses import dataclass
from datetime import datetime
from pydantic import BaseModel, Field, conlist
from typing import List, Union


class CommitMetaData(BaseModel):
    # ID of pipeline that the commit originated from
    #
    pipeline_id: str
    task_name: str
    # docker image version and repo
    task_image: str
    args: List[str] = []
    pipeline_instance_id: str = ""



class Commit(BaseModel):
    message: str
    repo: str
    branch: str
    commit_date: datetime
    committer: str
    metadata: Union[CommitMetaData, None]
    files_added: List[str] = Field(default=[])
    files_removed: List[str] = Field(default=[])
    files_changed: List[str] = Field(default=[])
    id: str = Field(default="")


class Task(BaseModel):
    task_name: str
    task_image: str
    commit: Commit
    dependencies: List[str] = conlist(item_type=str, min_length=0)
    parameters: List[str] = Field(default_factory=List)


@dataclass
class Repository:
    Id: str
    StorageNamespace: str
