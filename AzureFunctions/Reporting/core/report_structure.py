from abc import abstractmethod, abstractproperty, abstractclassmethod

from .components.report_component_base import (
    ReportComponentBase,
)
from openpyxl import Workbook
from typing import List
from gcm.inv.utils.misc.extended_enum import ExtendedEnum
from gcm.inv.utils.date.AggregateInterval import AggregateInterval
from gcm.inv.utils.date.Frequency import Frequency, FrequencyType
from gcm.inv.entityhierarchy.EntityDomain.entity_domain.entity_domain_types import (
    EntityDomainTypes,
)
from gcm.inv.entityhierarchy.EntityDomain.entity_domain import Standards
import json
from gcm.inv.scenario import Scenario
from .serializable_base import SerializableBase
from .components.controller import convert_component_from_dict
import datetime as dt
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import (
    AzureDataLakeDao,
)
from gcm.Dao.DaoRunner import DaoSource, DaoRunner
from gcm.Dao.daos.azure_datalake.azure_datalake_file import (
    AzureDataLakeFile,
    TabularDataOutputTypes,
)
import pandas as pd
from azure.core.exceptions import ResourceNotFoundError


class ReportingBlob(ExtendedEnum):
    performance = "performance"
    sharepoint = "sharepoint"
    unfiled = "unfiled"
    EOF = "EOF"


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
        entity_info: pd.DataFrame = None,
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
            e_domain = EntityDomainTypes[d["entity_domain"]]
        if "entity_info" in d:
            e_info = pd.read_json(d["entity_info"])
        return ReportMeta(
            report_type, interval, frequency, consumer, e_domain, e_info
        )

    def to_dict(self):
        d = {
            "report_type": self.type.name,
            "interval": self.interval.name,
            "frequency": self.frequency.type.name,
            "consumer": self.consumer.to_dict(),
        }
        if self.entity_domain is not None:
            d["entity_domain"] = self.entity_domain.name
        if self.entity_info is not None:
            d["entity_info"] = self.entity_info.to_json()
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
    def __init__(self, report_name, report_meta: ReportMeta):
        self.report_name = report_name
        self.report_meta = report_meta
        # this is to be set via overriding
        self._components = None

    _display_mapping_dict = {
        EntityDomainTypes.InvestmentGroup: "PFUND",
        EntityDomainTypes.Investment: "PFUND",
        EntityDomainTypes.Portfolio: "PORTFOLIO",
        EntityDomainTypes.NONE: "XENTITY",
    }

    def report_file_xlsx_name(self):
        report_name = self.report_name.name
        entity_name: str = None
        entity_type_display: str = None
        # no need to add "Other" to string
        report_type = (
            self.report_meta.type.name
            if self.report_meta.type != ReportType.Other
            else None
        )
        date: dt.date = Scenario.get_attribute("as_of_date")
        date_str = date.strftime("%Y-%m-%d")
        if (
            self.report_meta.entity_domain is not None
            and self.report_meta.entity_info is not None
            and type(self.report_meta.entity_info) == pd.DataFrame
        ):

            df: pd.DataFrame = self.report_meta.entity_info
            entity_names = list(df[Standards.EntityName].dropna().unique())
            if len(entity_names) == 1:
                entity_name = entity_names[0]
                domain: EntityDomainTypes = self.report_meta.entity_domain

                entity_type_display = (
                    ReportStructure._display_mapping_dict[domain]
                    if domain in ReportStructure._display_mapping_dict
                    else domain.name.upper()
                )
            else:
                raise RuntimeError(
                    "More than one entity. Can't construct file name"
                )

        s = [
            str(x)
            for x in [
                report_name,
                entity_name,
                entity_type_display,
                report_type,
                date_str,
            ]
            if x is not None
        ]
        file_name = f'{"_".join(s)}.xlsx'
        return file_name

    @abstractclassmethod
    def available_metas(cls, **kwargs) -> AvailableMetas:
        raise NotImplementedError()

    def save_params(self) -> tuple[dict, DaoSource]:
        date: dt.date = Scenario.get_attribute("as_of_date")
        date_str = date.strftime("%Y_%m_%d")
        assert date_str is not None
        [output_loc, source] = (
            AzureDataLakeDao.BlobFileStructure(
                # THE BELOW IS THE ARTIFACT OF BAD LEGACY DESIGN
                # From Mark Woodall:
                # Performance has become the default location
                # just because that is how it evolved
                # -- that wasn't the original intent.
                # Also our understanding of needs has changed since we created Performance.
                # So the Performance level controls access rights, so since we have it,
                # let's stick with that as our standard place for reports that should be
                # accessible to all of investments.
                # zone=ReportingBlob.performance,
                # sources="Risk",
                # entity="Testing",
                # path=[date_str, self.report_file_name],
                zone=ReportingBlob.performance,
                sources=self.report_meta.type.name,
                entity=date_str,
                path=[
                    self.report_file_xlsx_name(),
                ],
            ),
            DaoSource.ReportingStorage,
        )
        return (
            AzureDataLakeDao.create_blob_params(
                output_loc, metadata=self.storage_account_metadata()
            ),
            source,
        )

    def get_template(self) -> Workbook:
        dao: DaoRunner = Scenario.get_attribute("dao")
        try:
            params = AzureDataLakeDao.create_blob_params(
                self.excel_template_location,
            )
            file: AzureDataLakeFile = dao.execute(
                params=params,
                source=DaoSource.DataLake,
                operation=lambda d, p: d.get_data(p),
            )
            excel = file.to_tabular_data(
                TabularDataOutputTypes.ExcelWorkBook, params
            )
            return excel
        except ResourceNotFoundError:
            return None

    @abstractproperty
    def excel_template_location(self):
        pass

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

    class gcm_metadata:
        gcm_report_name = "gcm_report_name"
        gcm_as_of_date = "gcm_as_of_date"
        gcm_business_group = "gcm_business_group"
        gcm_report_frequency = "gcm_report_frequency"
        gcm_report_period = "gcm_report_period"
        gcm_report_type = "gcm_report_type"
        gcm_target_audience = "gcm_target_audience"
        gcm_modified_date = "gcm_modified_date"

    def get_entity_metadata(self) -> dict:
        if self.report_meta.entity_domain is not None:
            if (
                self.report_meta.entity_info is not None
                and type(self.report_meta.entity_info) == pd.DataFrame
            ):
                # TODO: figure this out with team
                return None
        return None

    def report_name_metadata(self):
        return self.report_name.name

    def storage_account_metadata(self) -> dict:
        # these are starting to become arbitrary
        # TODO: make a specific dataclass (instead of dict)
        class_type = ReportStructure.gcm_metadata
        val = {
            class_type.gcm_report_name: self.report_name_metadata(),
            class_type.gcm_as_of_date: Scenario.get_attribute(
                "as_of_date"
            ).strftime("%Y-%m-%d"),
            class_type.gcm_business_group: self.report_meta.consumer.vertical.name,
            class_type.gcm_report_frequency: self.report_meta.frequency.type.name,
            class_type.gcm_report_period: self.report_meta.interval.name,
            class_type.gcm_report_type: self.report_meta.type.value,
            class_type.gcm_target_audience: json.dumps(
                list(
                    map(
                        lambda x: x.name,
                        self.report_meta.consumer.horizontal,
                    )
                )
            ),
            class_type.gcm_modified_date: dt.date.today().strftime(
                "%Y-%m-%d"
            ),
        }
        entity_metadata = self.get_entity_metadata()
        val: dict = (
            val if entity_metadata is None else (val | entity_metadata)
        )
        return val

    # serialization logic
    def to_dict(self):
        return {
            "report_name": self.report_name.name,
            "report_meta": self.report_meta.to_dict(),
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

        report_meta: ReportMeta = ReportMeta.from_dict(d["report_meta"])
        c_list = []
        for i in components:
            c_list.append(convert_component_from_dict(i))
        p = cls.__new__(cls)
        p.components = c_list
        p.report_meta = report_meta
        p.report_name = report_name
        return p
