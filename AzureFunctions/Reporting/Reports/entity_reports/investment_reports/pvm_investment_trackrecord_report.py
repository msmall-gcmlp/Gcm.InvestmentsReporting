from ....core.report_structure import (
    ReportStructure,
    ReportMeta,
    AvailableMetas,
)
from gcm.Dao.DaoRunner import AzureDataLakeDao
from gcm.inv.utils.date.AggregateInterval import AggregateInterval
from ....core.report_structure import (
    ReportType,
    ReportConsumer,
    EntityDomainTypes,
)
from ....core.components.report_table import ReportTable
import pandas as pd
from gcm.inv.utils.date.Frequency import Frequency, FrequencyType
from ...report_names import ReportNames
from gcm.inv.scenario import Scenario
import datetime as dt


# http://localhost:7071/orchestrators/ReportOrchestrator?as_of_date=2022-09-30&ReportName=PvmManagerTrackRecordReport&frequency=Once&save=True&aggregate_interval=ITD&EntityDomainTypes=InvestmentManager&EntityNames=[%22ExampleManagerName%22]


class PvmInvestmentTrackRecordReport(ReportStructure):
    def __init__(self, report_meta: ReportMeta):
        super().__init__(
            ReportNames.PvmInvestmentTrackRecordReport, report_meta
        )

    @property
    def excel_template_location(self):
        return AzureDataLakeDao.BlobFileStructure(
            zone=AzureDataLakeDao.BlobFileStructure.Zone.raw,
            sources="investmentsreporting",
            entity="exceltemplates",
            path=["PvmInvestmentTrackRecordTemplate.xlsx"],
        )

    @classmethod
    def available_metas(cls):
        return AvailableMetas(
            report_type=ReportType.Performance,
            frequencies=[
                Frequency(FrequencyType.Once),
            ],
            aggregate_intervals=[AggregateInterval.ITD],
            consumer=ReportConsumer(
                horizontal=[ReportConsumer.Horizontal.IC],
                vertical=ReportConsumer.Vertical.PEREI,
            ),
            entity_groups=[
                EntityDomainTypes.Investment,
            ],
        )

    def assign_components(self):
        as_of_date: dt.date = Scenario.get_attribute("as_of_date")
        assert as_of_date is not None
        # domain = self.report_meta.entity_domain
        # entity_info: pd.DataFrame = self.report_meta.entity_info
        # aggregate_interval: AggregateInterval = Scenario.get_attribute(
        #     "aggregate_interval"
        # )
        # if aggregate_interval == AggregateInterval.ITD:
        #     # do something!
        #     pass

        tables = [
            ReportTable(
                "MyComponent",
                pd.DataFrame({"V1": [1.0, 2.0], "V2": [1.0, 2.0]}),
            ),
            ReportTable(
                "MyComponent_2",
                pd.DataFrame({"V1": [1.0, 2.0], "V2": [1.0, 2.0]}),
            ),
        ]
        return tables
        # name = "Test"
        # manager_data = ReportWorkBookHandler(
        #     name, tables, self.excel_template_location
        # )
        # # now get the investments managed by this manager

        # return [manager_data]
