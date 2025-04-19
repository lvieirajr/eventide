from logging import getLogger

from pydantic import BaseModel as PydanticBaseModel
from pydantic import ConfigDict


class BaseModel(PydanticBaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)


eventide_logger = getLogger(name="eventide")
queue_logger = getLogger(name="eventide.queue")
worker_logger = getLogger(name="eventide.worker")
