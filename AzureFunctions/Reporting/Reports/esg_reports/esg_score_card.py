from ...core.report_structure import (
    ReportStructure,
    ReportMeta,
    AvailableMetas,
)
from gcm.Dao.DaoRunner import DaoRunner, DaoSource
from gcm.inv.utils.date.AggregateInterval import AggregateInterval
from ...core.report_structure import (
    ReportType,
    ReportConsumer,
)
from gcm.inv.utils.date.Frequency import Frequency, FrequencyType, Calendar
from ...core.components.report_table import ReportTable
import pandas as pd
from ..report_names import ReportNames
from gcm.inv.scenario import Scenario


class ESG_ScoreCard(ReportStructure):
    _excel_template_folder = (
        "/".join(["raw", "investmentsreporting", "exceltemplates"]) + "/"
    )
    _output_directory = (
        "/".join(["cleansed", "investmentsreporting", "printedexcels"])
        + "/"
    )

    def __init__(self, report_meta: ReportMeta):
        super().__init__(ReportNames.MarketPerformanceReport, report_meta)
        self.excel_template = "Market Performance_Template.xlsx"

    @classmethod
    def available_metas(cls):
        return AvailableMetas(
            report_type=ReportType.Market,
            frequencies=[
                Frequency(FrequencyType.Daily, Calendar.US_Business_GCM),
            ],
            aggregate_intervals=[AggregateInterval.Multi],
            consumer=ReportConsumer(
                horizontal=[ReportConsumer.Horizontal.FIRM],
                vertical=ReportConsumer.Vertical.FIRM,
            ),
        )

    def assign_components(self):
        dao: DaoRunner = Scenario.get_attribute("dao")
        assert dao is not None
        df: pd.DataFrame = dao.execute(
            params={
                "schema": "reporting21",
                "tables": "vContributions",
                "operation": lambda query, item: query.filter(
                    item.EntityId._in(911, 923)
                ),
            },
            source=DaoSource.InvestmentsDwh,
            operation=lambda d, p: d.get_data(p),
        )
        return [
            ReportTable(
                "MyComponent",
                df,
            ),
        ]
