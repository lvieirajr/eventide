from pydantic import BaseModel, ConfigDict


class PydanticModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
