import pandas as pd
import datetime as dt
from gcm.Dao.DaoRunner import DaoRunner, DaoRunnerConfigArgs
from gcm.Dao.DaoSources import DaoSource
from gcm.inv.reporting.core.ReportStructure.report_structure import ReportingEntityTypes, ReportType, AggregateInterval
from gcm.inv.reporting.core.Runners.investmentsreporting import InvestmentsReportRunner
from gcm.Scenario.scenario import Scenario


def write_report_to_reporting_hub():
    runner = DaoRunner(
        container_lambda=lambda b, i: b.config.from_dict(i),
        config_params={
            DaoRunnerConfigArgs.dao_global_envs.name: {
                DaoSource.PubDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                DaoSource.ReportingStorage.name: {
                    "Environment": "uat",
                    "Subscription": "nonprd",
                }
            }
        })

    test_data = {"header_info": pd.DataFrame({'peer_group_heading': ['TEST']})}
    as_of_date = dt.datetime.now()

    with Scenario(asofdate=as_of_date).context():
        InvestmentsReportRunner().execute(
            data=test_data,
            template="PFUND_PerformanceQuality_Template.xlsx",
            save=True,
            runner=runner,
            entity_type=ReportingEntityTypes.manager_fund_group,
            report_name='Performance Quality',
            report_type=ReportType.Risk,
            aggregate_intervals=AggregateInterval.MTD
        )

    return True
