from abc import abstractclassmethod, abstractmethod

from .components.report_component_base import (
    ReportComponentBase,
)
from typing import List
from gcm.inv.utils.misc.extended_enum import ExtendedEnum
from gcm.inv.utils.date.AggregateInterval import AggregateInterval
from gcm.inv.utils.date.Frequency import Frequency, FrequencyType
from gcm.inv.entityhierarchy.EntityDomain.entity_domain.entity_domain_types import (
    EntityDomainTypes,
)
import json
from gcm.inv.scenario import Scenario
from .serializable_base import SerializableBase
from .components.controller import convert_component_from_dict
import datetime as dt
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import (
    AzureDataLakeDao,
)
from gcm.Dao.DaoRunner import DaoSource
import pandas as pd


class ReportType(ExtendedEnum):
    Risk = "Risk"
    Capital_And_Exposure = "Capital & Exposure"
    Performance = "Performance"
    Commitments_and_Flows = "Commitments & Flows"
    Market = "Market"
    Other = "Other"


class ReportConsumer(SerializableBase):
    class Horizontal(ExtendedEnum):
        Risk = 0
        IC = 1
        CIO = 2
        PM = 3
        Research = 4
        FIRM = 5

    class Vertical(ExtendedEnum):
        FIRM = 0
        ARS = 1
        PEREI = 2
        SIG = 3
        Other = 4

    def __init__(self, horizontal: List[Horizontal], vertical: Vertical):
        self.horizontal = horizontal
        self.vertical = vertical

    @classmethod
    def from_dict(cls, d: dict) -> "ReportConsumer":
        horizontals = [
            ReportConsumer.Horizontal[i] for i in d["horizontal"]
        ]
        vertical = ReportConsumer.Vertical[d["vertical"]]
        return ReportConsumer(horizontals, vertical)

    def to_dict(self):
        return {
            "horizontal": [i.name for i in self.horizontal],
            "vertical": self.vertical.name,
        }

    def to_json(self):
        return json.dumps(self.to_dict())


class ReportMeta(SerializableBase):
    def __init__(
        self,
        type: ReportType,
        interval: AggregateInterval,
        frequency: Frequency,
        consumer: ReportConsumer,
        entity_domain: EntityDomainTypes = None,
        entity_info: pd.DataFrame = None
    ):
        self.type = type
        self.interval = interval
        self.frequency = frequency
        self.consumer = consumer
        self.entity_domain = entity_domain
        self.entity_info = entity_info

    @classmethod
    def from_dict(cls, d: dict) -> "ReportMeta":
        report_type = ReportType[d["report_type"]]
        interval = AggregateInterval[d["interval"]]
        frequency = Frequency(FrequencyType[d["frequency"]])
        consumer: ReportConsumer = ReportConsumer.from_dict(d["consumer"])
        e_domain = None
        e_info = None
        if "entity_domain" in d:
            e_domain = EntityDomainTypes[d['entity_domain']]
        if "entity_info" in d:
            e_info = pd.read_json(d['entity_info'])
        return ReportMeta(report_type, interval, frequency, consumer, e_domain, e_info)

    def to_dict(self):
        d = {
            "report_type": self.type.name,
            "interval": self.interval.name,
            "frequency": self.frequency.type.name,
            "consumer": self.consumer.to_dict(),
            
        }
        if self.entity_domain is not None:
            d['entity_domain'] = self.entity_domain.name
        if self.entity_info is not None:
            d['entity_info'] = self.entity_info.to_json()
        return d


class AvailableMetas(object):
    def __init__(
        self,
        report_type: ReportType,
        frequencies: List[Frequency],
        aggregate_intervals: List[AggregateInterval],
        consumer: ReportConsumer,
        entity_groups: List[EntityDomainTypes] = None,
    ):
        self.report_type = report_type
        self.frequencies = frequencies
        self.aggregate_intervals = aggregate_intervals
        self.consumer = consumer
        self.entity_groups = entity_groups


class ReportStructure(SerializableBase):
    _excel_template_folder = (
        "/".join(["raw", "investmentsreporting", "exceltemplates"]) + "/"
    )
    _output_directory = (
        "/".join(["cleansed", "investmentsreporting", "printedexcels"])
        + "/"
    )

    def __init__(self, report_name, report_meta: ReportMeta):
        self.report_name = report_name
        self.report_meta = report_meta
        # this is to be set via overriding
        self._components = None
        self._excel_template = None

    @property
    def excel_template(self) -> str:
        if self._excel_template is None:
            self._excel_template = ""
        return self._excel_template

    @excel_template.setter
    def excel_template(self, excel_template):
        self._excel_template = excel_template

    @abstractclassmethod
    def available_metas(cls) -> AvailableMetas:
        pass

    def to_dict(self):
        return {
            "report_name": self.report_name.name,
            "report_meta": self.report_meta.to_dict(),
            "excel_template": self.excel_template,
            "report_components": [c.to_dict() for c in self.components],
        }

    @staticmethod
    def from_json(s: str) -> "ReportStructure":
        d = json.loads(s)
        return ReportStructure.from_dict(d)

    @classmethod
    def from_dict(cls, d: dict, **kwargs) -> "ReportStructure":
        report_name = kwargs["report_name"]
        components: List[dict] = d["report_components"]

        excel_template = d["excel_template"]
        report_meta: ReportMeta = ReportMeta.from_dict(d["report_meta"])
        c_list = []
        for i in components:
            c_list.append(convert_component_from_dict(i))
        p = cls.__new__(cls)
        p.components = c_list
        p.excel_template = excel_template
        p.report_meta = report_meta
        p.report_name = report_name
        return p

    @property
    def save_params(self) -> tuple[dict, DaoSource]:
        return (
            AzureDataLakeDao.create_get_data_params(
                f"{ReportStructure._output_directory}{self.report_meta.type.name}/",
                "testing.xlsx",
                metadata=self.metadata(),
            ),
            DaoSource.DataLake,
        )

    @property
    def base_json_name(self) -> str:
        return f'{self.report_name.name}_{Scenario.get_attribute("as_of_date").strftime("%Y-%m-%d")}.json'

    @property
    def components(self) -> List[ReportComponentBase]:
        if self._components is None:
            self._components = self.assign_components()
        return self._components

    @components.setter
    def components(self, components):
        self._components = components

    @abstractmethod
    def assign_components(self):
        pass

    def metadata(self) -> dict:
        # these are starting to become arbitray
        val = {
            "gcm_report_name": self.report_name.name,
            "gcm_as_of_date": Scenario.get_attribute(
                "as_of_date"
            ).strftime("%Y-%m-%d"),
            "gcm_business_group": self.report_meta.consumer.vertical.name,
            "gcm_report_frequency": self.report_meta.frequency.type.name,
            "gcm_report_period": self.report_meta.interval.name,
            "gcm_report_type": self.report_meta.type.value,
            "gcm_target_audience": json.dumps(
                list(
                    map(
                        lambda x: x.name,
                        self.report_meta.consumer.horizontal,
                    )
                )
            ),
            "gcm_modified_date": dt.date.today().strftime("%Y-%m-%d"),
        }
        return val
