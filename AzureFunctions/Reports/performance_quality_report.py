import datetime as dt
import pandas as pd
from gcm.Dao.DaoSources import DaoSource
from gcm.InvestmentsReporting.ReportStructure.report_structure import ReportingEntityTypes
from gcm.InvestmentsReporting.Runners.investmentsreporting import InvestmentsReportRunner
from gcm.Scenario.scenario import Scenario
from gcm.inv.dataprovider.attribution import Attribution
from gcm.inv.dataprovider.benchmarking import Benchmarking
from gcm.inv.dataprovider.factors import Factors
from gcm.inv.dataprovider.investments import Investments
from gcm.inv.quantlib.enum_source import PeriodicROR
from gcm.inv.quantlib.timeseries.analytics import Analytics
from .reporting_runner_base import ReportingRunnerBase


class PerformanceQualityReport(ReportingRunnerBase):

    def __init__(self, dao_runner, start_date, end_date, as_of_date, params):
        super().__init__(runner=dao_runner)
        self._dao_runner = dao_runner
        self._start_date = start_date
        self._end_date = end_date
        self._as_of_date = as_of_date
        self._attribution = Attribution(dao_runner=dao_runner, as_of_date=as_of_date)
        self._investments = Investments(dao_runner=dao_runner, as_of_date=as_of_date)
        self._benchmarking = Benchmarking(dao_runner=dao_runner, as_of_date=as_of_date)
        self._factors = Factors(dao_runner=dao_runner, as_of_date=as_of_date)
        self._analytics = Analytics()
        self._entity_type = 'ARS PFUND'
        self._funds = None
        self._fund_name = params['fund_name']

    def get_funds(self):
        filters = dict(vertical=['ARS', 'EOF'], status='EMM', strategy='Equities')
        # filters = dict(vertical=['EOF'])
        funds = self._investments.get_filtered_funds_universe(filters=filters)
        funds = funds.sort_values('Fund')
        self._funds = funds
        return funds

    def get_header_info(self, holding_name):
        header = pd.DataFrame({'header_info': [holding_name, self._entity_type, self._as_of_date]})
        return header

    def get_fund_returns(self):
        monthly_returns, daily_returns = \
            self._investments.get_returns_from_source_ids(external_ids=self._funds,
                                                          start_date=self._start_date,
                                                          end_date=self._end_date)
        return monthly_returns, daily_returns

    def _get_return_summary(self, returns, return_type):
        mtd_return = self._analytics.compute_periodic_return(ror=returns, period=PeriodicROR.MTD,
                                                             as_of_date=self._as_of_date, method='geometric')
        qtd_return = self._analytics.compute_periodic_return(ror=returns, period=PeriodicROR.QTD,
                                                             as_of_date=self._as_of_date, method='geometric')
        ytd_return = self._analytics.compute_periodic_return(ror=returns, period=PeriodicROR.YTD,
                                                             as_of_date=self._as_of_date, method='geometric')

        summary = pd.DataFrame({return_type: [mtd_return, qtd_return, ytd_return]})
        return summary

    def build_benchmark_summary(self, holding, fund_returns):
        fund_returns_summary = self._get_return_summary(returns=fund_returns[holding], return_type='FundReturn')
        summary = fund_returns_summary
        return summary

    def generate_performance_quality_report(self):
        self.get_funds()['Fund']
        fund_monthly_returns, _ = self.get_fund_returns()

        header_info = self.get_header_info(holding_name=self._fund_name)
        return_summary = self.build_benchmark_summary(holding=self._fund_name, fund_returns=fund_monthly_returns)
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
                runner=self._dao_runner,
                entity_name="EOFMF",
                entity_display_name="EOF",
                entity_type=ReportingEntityTypes.portfolio,
                entity_source=DaoSource.PubDwh,
            )

    def run(self, **kwargs):
        self.generate_performance_quality_report()
        return True
