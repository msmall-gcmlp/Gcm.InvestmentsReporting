import ast
from datetime import datetime
from gcm.inv.scenario import Scenario
from _legacy.Reports.reports.performance_quality.report_data import (
    PerformanceQualityReportData,
)
from _legacy.Reports.reports.performance_quality.peer_level_analytics import (
    PerformanceQualityPeerLevelAnalytics,
)
from _legacy.Reports.reports.performance_quality.report import (
    PerformanceQualityReport,
)
from dateutil.relativedelta import relativedelta


def main(requestBody) -> str:
    params = requestBody["params"]
    run = params["run"]
    as_of_date = params["as_of_date"]
    as_of_date = datetime.strptime(as_of_date, "%Y-%m-%d").date()

    with Scenario(as_of_date=as_of_date).context():

        if run == "PerformanceQualityReportData":
            if params.get("investment_group_ids") is None:
                investment_group_ids = None
            else:
                investment_group_ids = ast.literal_eval(params.get("investment_group_ids"))

            perf_quality_data = PerformanceQualityReportData(
                start_date=as_of_date - relativedelta(years=10),
                end_date=as_of_date,
                investment_group_ids=investment_group_ids,
            )
            return perf_quality_data.execute()

        elif run == "PerformanceQualityPeerSummaryReport":
            return PerformanceQualityPeerLevelAnalytics(peer_group=params["peer_group"]).execute()

        elif run == "PerformanceQualityReport":
            return PerformanceQualityReport(fund_name=params["fund_name"]).execute()
