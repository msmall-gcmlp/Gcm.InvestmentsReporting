from ....core.report_structure import (
    ReportStructure,
    ReportMeta,
    AvailableMetas,
)
from gcm.Dao.DaoRunner import DaoRunner, AzureDataLakeDao
from gcm.inv.utils.date.AggregateInterval import AggregateInterval
from ....core.report_structure import (
    ReportType,
    ReportConsumer,
    EntityDomainTypes,
)
from gcm.inv.utils.date.Frequency import Frequency, FrequencyType
from ....core.components.report_table import ReportTable
import pandas as pd
from ...report_names import ReportNames
from gcm.inv.scenario import Scenario
import datetime as dt


class BrinsonAttributionReport(ReportStructure):
    def __init__(self, report_meta: ReportMeta):
        super().__init__(ReportNames.BrinsonAttributionReport, report_meta)

    @property
    def excel_template_location(self):
        file = (
            "PFUND_Attributes_Template.xlsx"
            if self.report_meta.entity_domain
            == EntityDomainTypes.Portfolio
            else "BBA_Template_Portfolio.xlsx"
        )
        return AzureDataLakeDao.BlobFileStructure(
            zone=AzureDataLakeDao.BlobFileStructure.Zone.raw,
            sources="investmentsreporting",
            entity="exceltemplates",
            path=[file],
        )

    @classmethod
    def available_metas(cls):
        return AvailableMetas(
            report_type=ReportType.Performance,
            frequencies=[
                Frequency(FrequencyType.Monthly),
            ],
            aggregate_intervals=[AggregateInterval.Multi],
            consumer=ReportConsumer(
                horizontal=[ReportConsumer.Horizontal.PM],
                vertical=ReportConsumer.Vertical.ARS,
            ),
            entity_groups=[
                EntityDomainTypes.Portfolio,
                EntityDomainTypes.Vertical,
            ],
        )

    def assign_components(self):
        dao: DaoRunner = Scenario.get_attribute("dao")
        as_of_date: dt.date = Scenario.get_attribute("as_of_date")
        assert as_of_date is not None
        domain = self.report_meta.entity_domain
        entity_info: pd.DataFrame = self.report_meta.entity_info
        assert entity_info is not None
        if domain == EntityDomainTypes.InvestmentGroup:
            print("yay!")
        if domain == EntityDomainTypes.Portfolio:
            print("wooo!")
        assert dao is not None
        return [
            ReportTable(
                "MyComponent",
                pd.DataFrame({"V1": [1.0, 2.0], "V2": [1.0, 2.0]}),
            ),
            ReportTable(
                "MyComponent_2",
                pd.DataFrame({"V1": [1.0, 2.0], "V2": [1.0, 2.0]}),
            ),
        ]
