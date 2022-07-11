from re import S
from ...reporting_runner_base import ReportingRunnerBase
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import (
    AzureDataLakeFile,
    TabularDataOutputTypes,
)
import datetime as dt
from gcm.Dao.DaoRunner import DaoRunner
from gcm.Dao.DaoSources import DaoSource
from gcm.inv.reporting.core.ReportStructure.report_structure import (
    ReportType,
    AggregateInterval,
)
from gcm.inv.reporting.core.Runners.investmentsreporting import (
    InvestmentsReportRunner,
)
from gcm.Scenario.scenario import Scenario
import openpyxl
import pandas as pd
import copy


class PerformanceConcentrationReport(ReportingRunnerBase):
    def __init__(
        self,
        runner: DaoRunner,
        asofdate: dt.datetime,
        underwriting: str,
        managername: str,
        vertical: str,
    ):
        super().__init__(runner)
        self.asofdate = asofdate
        self.underwriting = underwriting
        self.managername = managername
        self.vertical = vertical

    def _executing_in_scenario(self, **kwargs):
        return None

    def get_sheet_as_df(self, raw_file_stream, sheet_name) -> pd.DataFrame:
        return pd.read_excel(raw_file_stream, sheet_name=sheet_name)

    def consolidate_data(self, wb_name, data):
        assert wb_name.lower() in ["all", "realized"]
        assert type(data) == AzureDataLakeFile
        wb: openpyxl.Workbook = data.to_tabular_data(
            TabularDataOutputTypes.ExcelWorkBook, {}
        )
        sheet_names = wb.get_sheet_names()
        top_items = []
        total = None
        other = None
        top_deals = None
        dist_returns = None
        for s in sheet_names:
            if "Top " in s:
                temp_i = self.get_sheet_as_df(data.content, s)
                temp_i["InvestedCapital"] = abs(
                    temp_i["InvestedCapital"]
                )
                top_items.append(temp_i)
            elif "Other" in s:
                other = self.get_sheet_as_df(data.content, s)
                other["InvestedCapital"] = abs(other["InvestedCapital"])
            elif "Total" in s:
                total = self.get_sheet_as_df(data.content, s)
                total["InvestedCapital"] = abs(total["InvestedCapital"])
            elif "TopDeals" in s:
                top_deals = self.get_sheet_as_df(data.content, s)
                top_deals["InvestedCapital"] = abs(
                    top_deals["InvestedCapital"]
                )
            elif "Dist" in s:
                dist_returns = self.get_sheet_as_df(data.content, s)
                dist_returns["InvestedCapital"] = abs(
                    dist_returns["InvestedCapital"]
                )
        # now clean up columns
       
        other_asset_copy = copy.deepcopy(other)
        other_asset_copy["AssetName"] = "Other"
       
        top_deals = pd.concat([top_deals, other_asset_copy])
        top_deals.reset_index(drop=True, inplace=True)
       
        total["Bucket"] = (
            total["Bucket"]
            + " (Deals: "
            + total.PositionCount.map(str)
            + ")"
        )
        if wb_name.lower() == "all":
            top_deals = top_deals[
                [
                    "AssetName",
                    "PositionInvestmentDate",
                    "InvestedCapital",
                    "UnrealizedValue",
                    "TotalValue",
                    "InvestmentGain",
                    "MOIC",
                    "IRR",
                ]
            ]
            total = total[
                [
                    "Bucket",
                    "PositionInvestmentDate",
                    "InvestedCapital",
                    "UnrealizedValue",
                    "TotalValue",
                    "InvestmentGain",
                    "MOIC",
                    "IRR",
                ]
            ]
        elif wb_name.lower() == "realized":
            top_deals = top_deals[
                [
                    "AssetName",
                    "PositionInvestmentDate",
                    "InvestedCapital",
                    "PositionExitDate",
                    "TotalValue",
                    "InvestmentGain",
                    "MOIC",
                    "IRR",
                ]
            ]
            total = total[
                [
                    "Bucket",
                    "PositionInvestmentDate",
                    "InvestedCapital",
                    "PositionExitDate",
                    "TotalValue",
                    "InvestmentGain",
                    "MOIC",
                    "IRR",
                ]
            ]
        total_capital = total["InvestedCapital"].sum()
        dist_returns = dist_returns[
            ["Distribution", "PositionCount", "IRR", "InvestedCapital"]
        ]
        dist_returns["InvestedCapital"] = (
            dist_returns["InvestedCapital"] / total_capital
        )
        top_items = pd.concat(top_items)
        top_items.reset_index(drop=True, inplace=True)
        top_items["Bucket"] = "Top " + top_items.Bucket.map(str)
        top_items = pd.concat([top_items, other])
        top_items.reset_index(drop=True, inplace=True)
        top_items = top_items[["Bucket", "IRR", "MOIC"]]
        return {
            f"TopDeals_{wb_name.lower()}": top_deals,
            f"Total_{wb_name.lower()}": total,
            f"DistRet_{wb_name.lower()}": dist_returns,
            f"TopBuckets_{wb_name.lower()}": top_items,
        }

    def generate_performance_concentration_report(self, **kwargs):
        input_data = kwargs["data"]
        final = {
            "AsOfDate": pd.DataFrame({"AsOfDate": [self.asofdate]}),
            "ManagerName": pd.DataFrame(
                {"ManagerName": [self.managername]}
            ),
            "Vertical": pd.DataFrame({"Vertical": [self.vertical]}),
            "Underwriting": pd.DataFrame(
                {"Underwriting": [self.underwriting]}
            ),
        }
        for d in input_data:
            temp = self.consolidate_data(d, input_data[d])
            for k in temp:
                final[k] = temp[k]
        assert final is not None
        with Scenario(asofdate=self.asofdate).context():
            InvestmentsReportRunner().execute(
                data=final,
                template="Pvm.ReturnConcentration.xlsx",
                save=True,
                runner=self._runner,
                report_name="PvmReturnConcentration",
                report_type=ReportType.Performance,
                aggregate_intervals=AggregateInterval.Daily,
                output_dir="raw/investmentsreporting/printedexcels/",
                report_output_source=DaoSource.DataLake,
            )

    def run(self, **kwargs):
        self.generate_performance_concentration_report(**kwargs)
        return True
