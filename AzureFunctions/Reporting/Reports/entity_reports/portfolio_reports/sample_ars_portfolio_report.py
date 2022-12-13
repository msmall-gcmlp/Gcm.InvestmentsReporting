from ....core.report_structure import (
    ReportStructure,
    ReportMeta,
    AvailableMetas,
)
from gcm.Dao.DaoRunner import DaoRunner
from gcm.inv.utils.date.AggregateInterval import AggregateInterval
from ....core.report_structure import (
    ReportType,
    ReportConsumer,
    EntityDomainTypes,
)
from gcm.inv.utils.date.Frequency import Frequency, FrequencyType, Calendar
from ....core.components.report_table import ReportTable
import pandas as pd
from ...report_names import ReportNames
from gcm.inv.scenario import Scenario
import datetime as dt


class SampleArsPortfolioReport(ReportStructure):
    def __init__(self, report_meta: ReportMeta):
        super().__init__(
            ReportNames.Sample_Ars_Portfolio_Report, report_meta
        )
        self.excel_template = "MyFancyTemplate.xlsx"

    @classmethod
    def available_metas(cls):
        return AvailableMetas(
            report_type=ReportType.Performance,
            frequencies=[
                Frequency(FrequencyType.Monthly, Calendar.US_Business_GCM),
            ],
            aggregate_intervals=[AggregateInterval.MTD],
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
        domain: EntityDomainTypes = Scenario.get_attribute(
            "EntityDomainTypes"
        )
        assert as_of_date is not None
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
