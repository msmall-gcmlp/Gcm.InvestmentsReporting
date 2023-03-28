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


# http://localhost:7071/orchestrators/ReportOrchestrator?as_of_date=2022-09-30&ReportName=AggregatedPortolioFundAttributeReport&frequency=Monthly&save=True&aggregate_interval=MTD&entity_domain='Portfolio'


class SampleArsPortfolioReport(ReportStructure):
    def __init__(self, report_meta: ReportMeta):
        super().__init__(
            ReportNames.Sample_Ars_Portfolio_Report, report_meta
        )

    @property
    def excel_template_location(self):
        return AzureDataLakeDao.BlobFileStructure(
            zone=AzureDataLakeDao.BlobFileStructure.Zone.raw,
            sources="investmentsreporting",
            entity="exceltemplates",
            path=["MyFancyTemplate.xlsx"],
        )

    @classmethod
    def available_metas(cls):
        return AvailableMetas(
            report_type=ReportType.Performance,
            frequencies=[
                Frequency(FrequencyType.Monthly),
            ],
            aggregate_intervals=[AggregateInterval.MTD, AggregateInterval.YTD],
            consumer=ReportConsumer(
                horizontal=[ReportConsumer.Horizontal.Risk],
                vertical=ReportConsumer.Vertical.ARS,
            ),
            entity_groups=[
                EntityDomainTypes.Portfolio,
                EntityDomainTypes.InvestmentGroup,
            ],
        )

    def assign_components(self):
        dao: DaoRunner = Scenario.get_attribute("dao")
        as_of_date: dt.date = Scenario.get_attribute("as_of_date")
        assert as_of_date is not None
        domain = self.report_meta.entity_domain
        entity_info: pd.DataFrame = self.report_meta.entity_info
        aggregate_interval : AggregateInterval = Scenario.get_attribute("aggregate_interval")
        if aggregate_interval == AggregateInterval.MTD:
            # do something!
            pass
        start_date = as_of_date.offset(-aggregate_interval, Calendar.US)
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
