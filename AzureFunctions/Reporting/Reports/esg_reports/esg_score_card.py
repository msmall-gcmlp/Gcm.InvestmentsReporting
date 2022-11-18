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
        self.excel_template = "Primaries_Initial_InternalReporting.xlsx"

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
        dt_name = Scenario.get_attribute("as_of_date").strftime("%Y-%m-%d")
        runner: DaoRunner = Scenario.get_attribute("dao")
        data: pd.DataFrame = runner.execute(
            params={
                "table": "contributions",
                "schema": "reporting21",
            },
            source=DaoSource.InvestmentsDwh,
            operation=lambda dao, runner: dao.get_data(runner),
        )
        data = data[data["EntityId"].isin([911, 923])]
        data = data[data["IndicatorId"].isin([255, 253])]
        data = data[["IndicatorId", "Value"]]
        data.drop_duplicates(inplace=True)
        data["Value"] = data["Value"].apply(
            lambda x: x.replace("\n", "").replace("\r", "")
        )
        data.reset_index(inplace=True, drop=True)
        temp_dict = {
            253: "Primary_GCM_Coverage_Email",
            255: "Alternate_GCM_Coverage_Email",
        }
        d_final = {}
        for i in range(0, data.shape[0]):
            item = data.loc[i].to_dict()
            assert item is not None
            d_final[temp_dict[item["IndicatorId"]]] = item["Value"]
        metadata = d_final | self.metadata()
        return (
            AzureDataLakeDao.create_get_data_params(
                f"{ESG_ScoreCard._output_directory}/",
                f"ESG_Report_Test_{dt_name}.xlsx",
                metadata=metadata,
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
