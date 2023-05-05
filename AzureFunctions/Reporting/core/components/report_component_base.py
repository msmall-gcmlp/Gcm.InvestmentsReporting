from abc import abstractproperty
from gcm.inv.utils.misc.extended_enum import ExtendedEnum
from enum import auto
from ..serializable_base import SerializableBase


class ReportComponentType(ExtendedEnum):
    ReportTable = auto()
    ReportWorkBookHandler = auto()
    ReportWorksheet = auto()


class ReportComponentBase(SerializableBase):
    def __init__(self, component_name: str):
        self.component_name = component_name

    @abstractproperty
    def component_type(self) -> ReportComponentType:
        raise NotImplementedError()

    class RendererParams(SerializableBase):
        def __init__(self):
            pass

        def to_dict(self, **kwargs) -> dict:
            return self.__dict__

        @classmethod
        def from_dict(cls, d: dict):
            if d is not None:
                class_obj = cls.__new__(cls)
                for k, v in d.items():
                    setattr(class_obj, k, v)
                return class_obj
            return None
