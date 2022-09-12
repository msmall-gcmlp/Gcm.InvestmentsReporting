import ast
from datetime import datetime
from gcm.Scenario.scenario import Scenario
from Reports.reports.performance_quality_report_data import (
    PerformanceQualityReportData,
)
from Reports.reports.performance_quality_peer_summary_report import (
    PerformanceQualityPeerSummaryReport,
)
from Reports.reports.performance_quality_report import (
    PerformanceQualityReport,
)
from gcm.Dao.DaoRunner import DaoRunner
from dateutil.relativedelta import relativedelta


def main(requestBody) -> str:
    params = requestBody["params"]
    run = params["run"]
    asofdate = params["asofdate"]
    as_of_date = datetime.strptime(asofdate, "%Y-%m-%d").date()
    runner = DaoRunner()

    if run == "PerformanceQualityReportData":
        if params.get("investment_group_ids") is None:
            investment_group_ids = None
        else:
            investment_group_ids = ast.literal_eval(
                params.get("investment_group_ids")
            )

        with Scenario(runner=runner, as_of_date=as_of_date).context():
            perf_quality_data = PerformanceQualityReportData(
                start_date=as_of_date - relativedelta(years=10),
                end_date=as_of_date,
                investment_group_ids=investment_group_ids,
            )
            return perf_quality_data.execute()

    elif run == "PerformanceQualityPeerSummaryReport":
        peer_group = params["peer_group"]

        return PerformanceQualityPeerSummaryReport(
            runner=runner, as_of_date=as_of_date, peer_group=peer_group
        ).execute()

    elif run == "PerformanceQualityReport":
        fund_name = params["fund_name"]

        return PerformanceQualityReport(
            runner=runner, as_of_date=as_of_date, fund_name=fund_name
        ).execute()
