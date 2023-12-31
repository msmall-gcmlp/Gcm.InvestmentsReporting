from abc import abstractmethod, abstractproperty, abstractclassmethod
from enum import Enum

from .components.report_component_base import (
    ReportComponentBase,
)
from gcm.inv.entityhierarchy.NodeHierarchy import (
    Standards as EntityStandardNames,
)
from gcm.inv.dataprovider.entity_provider.hierarchy_controller.hierarchy_handler import (
    HierarchyHandler,
)
from .entity_handler import EntityReportingMetadata
from openpyxl import Workbook
from typing import List, Optional, Callable
from gcm.inv.utils.misc.extended_enum import ExtendedEnum
from gcm.inv.utils.date.AggregateInterval import (
    AggregateInterval,
    AggregateIntervalReportHandler,
)
from gcm.inv.utils.date.Frequency import Frequency, FrequencyType, Calendar
from gcm.inv.entityhierarchy.EntityDomain.entity_domain.entity_domain_types import (
    EntityDomainTypes,
)
import re
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
from gcm.inv.dataprovider.entity_provider.controller import (
    EntityDomainProvider,
)
import pandas as pd
from azure.core.exceptions import ResourceNotFoundError
from functools import cached_property
from gcm.inv.utils.parsed_args.parsed_args import ParsedArgs


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
        FIRM = "FIRM"
        PE = "Private Equity"
        Infrastructure = "Infrastructure"
        Real_Estate = "Real Estate"
        ARS = "ARS"
        ARS_EOF = "ARS-EOF"
        SIG = "SIG"

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
        intervals: AggregateIntervalReportHandler,
        frequency: Frequency,
        consumer: ReportConsumer,
        entity_domain: EntityDomainTypes = None,
        entity_info: pd.DataFrame = None,
    ):
        self.type = type
        self.intervals = intervals
        self.frequency = frequency
        self.consumer = consumer
        self.entity_domain = entity_domain
        self.entity_info = entity_info

    @classmethod
    def from_dict(cls, d: dict) -> "ReportMeta":
        report_type = ReportType[d["report_type"]]
        intervals = AggregateIntervalReportHandler(
            [AggregateInterval[x] for x in d["intervals"]]
        )
        frequency = Frequency(FrequencyType[d["frequency"]])
        consumer: ReportConsumer = ReportConsumer.from_dict(d["consumer"])
        e_domain = None
        e_info = None
        if "entity_domain" in d:
            e_domain = EntityDomainTypes[d["entity_domain"]]
        if "entity_info" in d:
            e_info = pd.read_json(d["entity_info"])
        return ReportMeta(
            report_type, intervals, frequency, consumer, e_domain, e_info
        )

    def to_dict(self):
        d = {
            "report_type": self.type.name,
            "intervals": [
                x.name for x in self.intervals.aggregate_intervals
            ],
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
        aggregate_intervals: List[AggregateIntervalReportHandler],
        consumer: ReportConsumer,
        frequencies: List[Frequency] = [
            Frequency(FrequencyType.Once, Calendar.AllDays)
        ],
        entity_groups: List[EntityDomainTypes] = None,
    ):
        self.report_type = report_type
        self.frequencies = frequencies
        self.aggregate_intervals = aggregate_intervals
        self.consumer = consumer
        self.entity_groups = entity_groups


class ReportStructure(SerializableBase):
    def __init__(self, report_name: Enum, report_meta: ReportMeta):
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

    def _get_entity_file_display_name(self) -> tuple[str, str]:
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
                # Keep alpha numeric and spaces
                regex = re.compile("[^A-Za-z0-9 ]+")
                entity_name = regex.sub("", entity_name)
            else:
                raise RuntimeError(
                    "More than one entity. Can't construct file name"
                )
            return entity_type_display, entity_name

    def get_report_name_for_file(self):
        return self.report_name.name

    @cached_property
    def base_file_name(self):
        report_name = self.get_report_name_for_file()
        (
            entity_type_display,
            entity_name_display,
        ) = self._get_entity_file_display_name()
        # no need to add "Other" to string
        report_type = (
            self.report_meta.type.name
            if self.report_meta.type != ReportType.Other
            else None
        )
        date: dt.date = Scenario.get_attribute("as_of_date")
        date_str = date.strftime("%Y-%m-%d")
        aggregate_intervals = self.report_meta.intervals.to_reporting_tag()
        frequency_id = (
            self.report_meta.frequency.type.name
            if len(self.available_metas().frequencies) > 1
            else None
        )
        s = [
            str(x)
            for x in [
                report_name,
                entity_name_display,
                entity_type_display,
                report_type,
                aggregate_intervals,
                frequency_id,
                date_str,
            ]
            if x is not None
        ]
        return "_".join(s)

    @cached_property
    def report_file_xlsx_name(self):
        file_name = self.base_file_name
        file_name = f"{file_name}.xlsx"
        return file_name

    @abstractclassmethod
    def available_metas(cls, **kwargs) -> AvailableMetas:
        raise NotImplementedError()

    @cached_property
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
                    self.report_file_xlsx_name,
                ],
            ),
            DaoSource.ReportingStorage,
        )
        return (
            AzureDataLakeDao.create_blob_params(
                output_loc, metadata=self.storage_account_metadata
            ),
            source,
        )

    @classmethod
    def standard_entity_get_callable(
        cls, domain: EntityDomainProvider, pargs: ParsedArgs
    ) -> Callable[..., pd.DataFrame]:
        return domain.get_all

    def get_template(self) -> Optional[Workbook]:
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
        return f"{self.base_file_name}.json"

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

    # as provided by IT
    class gcm_metadata:
        gcm_report_name = "gcm_report_name"
        gcm_as_of_date = "gcm_as_of_date"
        gcm_business_group = "gcm_business_group"
        gcm_report_frequency = "gcm_report_frequency"
        gcm_report_period = "gcm_report_period"
        gcm_report_type = "gcm_report_type"
        gcm_target_audience = "gcm_target_audience"
        gcm_modified_date = "gcm_modified_date"

    @staticmethod
    def _generate_entity_metadata():
        pass

    def get_reportinghub_entity_metadata(self) -> dict:
        if self.report_meta.entity_domain is not None:
            if (
                self.report_meta.entity_info is not None
                and type(self.report_meta.entity_info) == pd.DataFrame
            ):
                entity_info = self.report_meta.entity_info
                coerced_dict = EntityReportingMetadata.generate(
                    entity_info
                )
                return coerced_dict
        return None

    @cached_property
    def related_entities(
        self,
    ) -> HierarchyHandler:
        domain = self.report_meta.entity_domain
        entity_info: pd.DataFrame = self.report_meta.entity_info
        if domain != EntityDomainTypes.NONE and entity_info.shape[0] > 0:
            current_entity_name: str = (
                entity_info[EntityStandardNames.EntityName]
                .drop_duplicates()
                .to_list()
            )[0]
            val = HierarchyHandler(domain, current_entity_name)
            return val
        else:
            return None

    def report_name_metadata(self):
        return self.report_name.name

    @cached_property
    def storage_account_metadata(self) -> dict:
        # these are starting to become arbitrary
        # TODO: make a specific dataclass (instead of dict)
        class_type = ReportStructure.gcm_metadata
        val = {
            class_type.gcm_report_name: self.report_name_metadata(),
            class_type.gcm_as_of_date: Scenario.get_attribute(
                "as_of_date"
            ).strftime("%Y-%m-%d"),
            class_type.gcm_business_group: self.report_meta.consumer.vertical.value,
            class_type.gcm_report_frequency: self.report_meta.frequency.type.name,
            class_type.gcm_report_period: self.report_meta.intervals.to_reporting_tag(),
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
        entity_metadata = self.get_reportinghub_entity_metadata()
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
            "storage_account_metadata": self.storage_account_metadata,
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
        storage_account_metadata: dict = d["storage_account_metadata"]
        c_list = []
        for i in components:
            c_list.append(convert_component_from_dict(i))
        p = cls.__new__(cls)
        p.components = c_list
        p.report_meta = report_meta
        p.report_name = report_name
        p.storage_account_metadata = storage_account_metadata
        return p
