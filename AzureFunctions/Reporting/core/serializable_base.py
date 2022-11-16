from abc import (
    ABC,
    abstractclassmethod,
    abstractmethod,
)
import json


class SerializableBase(ABC):
    def __init__(self):
        pass

    @abstractclassmethod
    def from_dict(cls, d: dict, **kwargs) -> "SerializableBase":
        raise NotImplementedError()

    @abstractmethod
    def to_dict(self, **kwargs) -> dict:
        raise NotImplementedError()

    def to_json(self, **kwargs) -> str:
        return json.dumps(self.to_dict(**kwargs))
