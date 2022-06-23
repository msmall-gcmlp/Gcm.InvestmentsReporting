import ast
import datetime as dt
from gcm.Scenario.scenario import Scenario
from gcm.inv.reporting.reports.performance_quality_report_data import PerformanceQualityReportData
from gcm.inv.reporting.reports.performance_quality_peer_summary_report import PerformanceQualityPeerSummaryReport
from gcm.inv.reporting.reports.performance_quality_report import PerformanceQualityReport
from gcm.Dao.DaoRunner import DaoRunner, DaoSource, DaoRunnerConfigArgs
from dateutil.relativedelta import relativedelta


def main(requestBody) -> str:
    params = requestBody["params"]
    run = params["run"]

    config_params = {
        DaoRunnerConfigArgs.dao_global_envs.name: {
            DaoSource.PubDwh.name: {
                "Environment": "uat",
                "Subscription": "nonprd",
            },
            DaoSource.ReportingStorage.name: {
                "Environment": "uat",
                "Subscription": "nonprd",
            },
        }
    }

    runner = DaoRunner(
        container_lambda=lambda b, i: b.config.from_dict(i),
        config_params=config_params,
    )

    as_of_date = dt.date(2022, 3, 31)

    if run == 'PerformanceQualityReportData':
        if params.get('investment_group_ids') is None:
            investment_group_ids = None
        else:
            investment_group_ids = ast.literal_eval(params.get('investment_group_ids'))

        with Scenario(runner=runner, as_of_date=dt.date(2022, 3, 31)).context():
            perf_quality_data = PerformanceQualityReportData(
                start_date=as_of_date - relativedelta(years=10),
                end_date=as_of_date,
                investment_group_ids=investment_group_ids)
            return perf_quality_data.execute()

    elif run == 'PerformanceQualityPeerSummaryReport':
        peer_group = params['peer_group']

        return PerformanceQualityPeerSummaryReport(
            runner=runner,
            as_of_date=as_of_date,
            peer_group=peer_group).execute()

    elif run == 'PerformanceQualityReport':
        fund_name = params['fund_name']

        return PerformanceQualityReport(
            runner=runner,
            as_of_date=as_of_date,
            fund_name=fund_name
        ).execute()
