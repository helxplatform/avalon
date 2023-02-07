from datetime import datetime

from pydantic import BaseModel, Field, conlist
from typing import List, Union
from enum import Enum


class ExecutionState(Enum):
    scheduled = 0
    waiting = 1
    running = 2
    success = 3
    failed = 4


class CommitMetaData(BaseModel):
    # ID of pipeline that the commit originated from
    #
    pipeline_id: str
    task_name: str
    # docker image version and repo
    task_image: str
    args: List[str] = []
    pipeline_instance_id: str=""


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
    dependencies: List[str] = conlist(item_type=str, min_items=0)
    parameters: List[str] = Field(default_factory=List)


class PipelineDefinition(BaseModel):
    id: str
    name: str
    tasks: List[Task]


class PipelineInstance(BaseModel):
    id: str
    tasks: List[Task]
    # @TODO make this id an instance.
    pipeline_definition_id: str = ""
