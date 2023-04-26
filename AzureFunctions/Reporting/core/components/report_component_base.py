from abc import abstractproperty
from gcm.inv.utils.misc.extended_enum import ExtendedEnum
from enum import auto
from ..serializable_base import SerializableBase


class ReportComponentType(ExtendedEnum):
    ReportTable = auto()
    ReportWorkBookHandler = auto()


class ReportComponentBase(SerializableBase):
    def __init__(self, component_name: str):
        self.component_name = component_name

    @abstractproperty
    def component_type(self) -> ReportComponentType:
        raise NotImplementedError()
