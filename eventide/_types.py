from typing import Any

from pydantic import BaseModel as PydanticBaseModel
from pydantic import ConfigDict

StrAnyDictType = dict[str, Any]


class BaseModel(PydanticBaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
