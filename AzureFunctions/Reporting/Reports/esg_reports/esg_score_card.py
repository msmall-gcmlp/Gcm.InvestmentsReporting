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
    def __init__(self, report_meta: ReportMeta):
        super().__init__(ReportNames.ESG_ScoreCard, report_meta)

    @classmethod
    def available_metas(cls):
        available_metas = AvailableMetas(
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
        return available_metas

    @property
    def excel_template_location(self):
        return AzureDataLakeDao.BlobFileStructure(
            zone=AzureDataLakeDao.BlobFileStructure.Zone.raw,
            sources="investmentsreporting",
            entity="exceltemplates",
            path=["Primaries_Initial_InternalReporting.xlsx"],
        )

    def assign_components(self):
        # ALL COMPLICATED LOGIC IS CALLED HERE
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
