from ...core.report_structure import (
    ReportStructure,
    ReportMeta,
    AvailableMetas,
)
from gcm.Dao.DaoRunner import DaoRunner, DaoSource, AzureDataLakeDao
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
        "/".join(["cleansed", "reporting21", "contributions"]) + "/"
    )

    def __init__(self, report_meta: ReportMeta):
        super().__init__(ReportNames.ESG_ScoreCard, report_meta)
        self.excel_template = (
            "Primaries_Initial_InternalReporting_vGS.xlsx"
        )

    @classmethod
    def available_metas(cls):
        return AvailableMetas(
            report_type=ReportType.Other,
            frequencies=[
                Frequency(FrequencyType.Daily, Calendar.US_Business_GCM),
            ],
            aggregate_intervals=[AggregateInterval.Multi],
            consumer=ReportConsumer(
                horizontal=[ReportConsumer.Horizontal.FIRM],
                vertical=ReportConsumer.Vertical.FIRM,
            ),
        )

    @property
    def save_params(self) -> tuple[dict, DaoSource]:
        return (
            AzureDataLakeDao.create_get_data_params(
                f"{ESG_ScoreCard._output_directory}/",
                "testing.xlsx",
                metadata=self.metadata(),
            ),
            DaoSource.DataLake,
        )

    def assign_components(self):
        dao: DaoRunner = Scenario.get_attribute("dao")
        assert dao is not None
        df: pd.DataFrame = dao.execute(
            params={
                "schema": "reporting21",
                "table": "vContributions",
                "operation": lambda query, item: query.filter(
                    item.EntityId.in_([911, 923])
                ),
            },
            source=DaoSource.InvestmentsDwh,
            operation=lambda d, p: d.get_data(p),
        )
        return [
            ReportTable(
                "OutputDataFrame",
                df,
            ),
        ]
