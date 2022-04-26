import datetime as dt
import pandas as pd
from gcm.Dao.DaoSources import DaoSource
from gcm.InvestmentsReporting.ReportStructure.report_structure import ReportingEntityTypes
from gcm.InvestmentsReporting.Runners.investmentsreporting import InvestmentsReportRunner
from gcm.Scenario.scenario import Scenario
from gcm.inv.quantlib.enum_source import PeriodicROR
from gcm.inv.quantlib.timeseries.analytics import Analytics
from .reporting_runner_base import ReportingRunnerBase


class PerformanceQualityReport(ReportingRunnerBase):

    def __init__(self, runner, as_of_date, params):
        super().__init__(runner=runner)
        self._as_of_date = as_of_date
        self._analytics = Analytics()
        self._entity_type = 'ARS PFUND'
        self._fund_dimn = pd.read_json(params['fund_dimn'], orient='index')
        self._returns = pd.read_json(params['fund_monthly_returns'], orient='index')
        self._fund_name = params['fund_name']

    def get_header_info(self):
        header = pd.DataFrame({'header_info': [self._fund_name, self._entity_type, self._as_of_date]})
        return header

    def _get_return_summary(self, returns, return_type):
        mtd_return = self._analytics.compute_periodic_return(ror=returns, period=PeriodicROR.MTD,
                                                             as_of_date=self._as_of_date, method='geometric')
        qtd_return = self._analytics.compute_periodic_return(ror=returns, period=PeriodicROR.QTD,
                                                             as_of_date=self._as_of_date, method='geometric')
        ytd_return = self._analytics.compute_periodic_return(ror=returns, period=PeriodicROR.YTD,
                                                             as_of_date=self._as_of_date, method='geometric')

        summary = pd.DataFrame({return_type: [mtd_return, qtd_return, ytd_return]})
        return summary

    def build_benchmark_summary(self):
        if any(self._returns.columns == self._fund_name):
            fund_returns_summary = self._get_return_summary(returns=self._returns[self._fund_name], return_type='FundReturn')
            summary = fund_returns_summary
        else:
            summary = pd.DataFrame()
        return summary

    def generate_performance_quality_report(self):
        header_info = self.get_header_info()
        return_summary = self.build_benchmark_summary()
        input_data = {
            "header_info": header_info,
            "benchmark_summary": return_summary,
        }

        with Scenario(asofdate=dt.date(2021, 12, 31)).context():
            report_name = "PFUND_PerformanceQuality_" + self._fund_name

            InvestmentsReportRunner().execute(
                data=input_data,
                template="PFUND_PerformanceQuality_Template.xlsx",
                save=True,
                report_name=report_name,
                runner=self._runner,
                entity_name="EOFMF",
                entity_display_name="EOF",
                entity_type=ReportingEntityTypes.portfolio,
                entity_source=DaoSource.PubDwh,
            )

    def run(self, **kwargs):
        self.generate_performance_quality_report()
        return True
