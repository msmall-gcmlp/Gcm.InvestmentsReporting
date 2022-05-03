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
        self._params = params
        self.__all_fund_dimn = None
        self.__all_fund_returns = None
        self.__all_abs_bmrk_returns = None
        self.__all_gcm_peer_returns = None
        self.__all_eurekahedge_returns = None

    @property
    def _all_fund_dimn(self):
        if self.__all_fund_dimn is None:
            self.__all_fund_dimn = pd.read_json(self._params['fund_dimn'], orient='index')
        return self.__all_fund_dimn

    @property
    def _all_fund_returns(self):
        if self.__all_fund_returns is None:
            self.__all_fund_returns = pd.read_json(self._params['fund_returns'], orient='index')
        return self.__all_fund_returns

    @property
    def _all_abs_bmrk_returns(self):
        if self.__all_abs_bmrk_returns is None:
            self.__all_abs_bmrk_returns = pd.read_json(self._params['abs_bmrk_returns'], orient='index')
        return self.__all_abs_bmrk_returns

    @property
    def _all_gcm_peer_returns(self):
        if self.__all_gcm_peer_returns is None:
            self.__all_gcm_peer_returns = pd.read_json(self._params['gcm_peer_returns'], orient='index')
        return self.__all_gcm_peer_returns

    @property
    def _all_eurekahedge_returns(self):
        if self.__all_eurekahedge_returns is None:
            self.__all_eurekahedge_returns = pd.read_json(self._params['eurekahedge_returns'], orient='index')
        return self.__all_eurekahedge_returns

    @property
    def _entity_type(self):
        return self._params['vertical'] + ' ' + self._params['entity']

    @property
    def _fund_name(self):
        return self._params['fund_name']

    @property
    def _fund_dimn(self):
        return self._all_fund_dimn[self._all_fund_dimn['InvestmentGroupName'] == self._fund_name]

    @property
    def _fund_id(self):
        return self._fund_dimn['InvestmentGroupId']

    @property
    def _fund_returns(self):
        return self._all_fund_returns[self._fund_name]

    @property
    def _abs_bmrk_returns(self):
        if any(self._fund_id.squeeze() == list(self._all_abs_bmrk_returns.columns)):
            returns = self._all_abs_bmrk_returns[self._fund_id].squeeze()
        else:
            returns = pd.DataFrame()

        return returns

    @property
    def _peer_group(self):
        return self._fund_dimn['ReportingPeerGroup'].squeeze()

    @property
    def _gcm_peer_returns(self):
        return self._all_gcm_peer_returns[self._peer_group].squeeze()

    @property
    def _eurekahedge_benchmark(self):
        return self._fund_dimn['EurekahedgeBenchmark'].squeeze()

    @property
    def _abs_return_benchmark(self):
        return self._fund_dimn['AbsoluteBenchmarkName'].squeeze()

    @property
    def _ehi50_returns(self):
        return self._all_eurekahedge_returns[self._eurekahedge_benchmark].squeeze()

    @property
    def _ehi200_returns(self):
        return self._all_eurekahedge_returns['Eurekahedge Institutional 200'].squeeze()

    def get_header_info(self):
        header = pd.DataFrame({'header_info': [self._fund_name, self._entity_type, self._as_of_date]})
        return header

    def get_peer_group_heading(self):
        return pd.DataFrame({'peer_group_heading': ['v. GCM Peer ' + self._peer_group]})

    def get_absolute_return_benchmark(self):
        return pd.DataFrame({'absolute_return_benchmark': [self._abs_return_benchmark]})

    def get_eurekahedge_benchmark_heading(self):
        return pd.DataFrame({'eurekahedge_benchmark_heading': ['v. ' + self._eurekahedge_benchmark]})

    def _get_return_summary(self, returns, return_type):
        returns = returns.copy()
        mtd_return = self._analytics.compute_periodic_return(ror=returns, period=PeriodicROR.MTD,
                                                             as_of_date=self._as_of_date, method='geometric')
        qtd_return = self._analytics.compute_periodic_return(ror=returns, period=PeriodicROR.QTD,
                                                             as_of_date=self._as_of_date, method='geometric')
        ytd_return = self._analytics.compute_periodic_return(ror=returns, period=PeriodicROR.YTD,
                                                             as_of_date=self._as_of_date, method='geometric')

        # rounding to 2 so that Excess Return matches optically
        summary = pd.DataFrame({return_type: [round(mtd_return, 2),
                                              round(qtd_return, 2),
                                              round(ytd_return, 2)]},
                               index=['MTD', 'QTD', 'YTD'])
        return summary

    def _build_benchmark_summary(self, fund_returns, benchmark_returns, benchmark_name):
        fund_returns = fund_returns.copy()
        benchmark_returns = benchmark_returns.copy()
        if benchmark_returns.shape[0] > 0:
            benchmark_returns = self._get_return_summary(returns=benchmark_returns, return_type=benchmark_name)
            summary = fund_returns.merge(benchmark_returns, left_index=True, right_index=True)
            summary[benchmark_name + 'Excess'] = summary['Fund'] - summary[benchmark_name]
        else:
            summary = fund_returns.copy()
            summary[benchmark_name] = 'N/A'
            summary[benchmark_name + 'Excess'] = 'N/A'

        return summary

    def build_benchmark_summary(self):
        fund_returns = self._get_return_summary(returns=self._fund_returns, return_type='Fund')
        absolute_return_summary = self._build_benchmark_summary(fund_returns=fund_returns,
                                                                benchmark_returns=self._abs_bmrk_returns,
                                                                benchmark_name='AbsoluteReturnBenchmark')
        gcm_peer_summary = self._build_benchmark_summary(fund_returns=fund_returns,
                                                         benchmark_returns=self._gcm_peer_returns,
                                                         benchmark_name='GcmPeer')
        ehi_50_summary = self._build_benchmark_summary(fund_returns=fund_returns,
                                                       benchmark_returns=self._ehi50_returns,
                                                       benchmark_name='EHI50')
        ehi_200_summary = self._build_benchmark_summary(fund_returns=fund_returns,
                                                        benchmark_returns=self._ehi200_returns,
                                                        benchmark_name='EHI200')
        summary = absolute_return_summary.copy()
        summary = summary.merge(gcm_peer_summary.drop(columns={'Fund'}), left_index=True, right_index=True)
        summary = summary.merge(ehi_50_summary.drop(columns={'Fund'}), left_index=True, right_index=True)
        summary = summary.merge(ehi_200_summary.drop(columns={'Fund'}), left_index=True, right_index=True)
        return summary

    def generate_performance_quality_report(self):
        header_info = self.get_header_info()
        return_summary = self.build_benchmark_summary()
        absolute_return_benchmark = self.get_absolute_return_benchmark()
        peer_group_heading = self.get_peer_group_heading()
        eurekahedge_benchmark_heading = self.get_eurekahedge_benchmark_heading()

        input_data = {
            "header_info": header_info,
            "benchmark_summary": return_summary,
            "absolute_return_benchmark": absolute_return_benchmark,
            "peer_group_heading": peer_group_heading,
            "eurekahedge_benchmark_heading": eurekahedge_benchmark_heading,
        }

        with Scenario(asofdate=dt.date(2021, 12, 31)).context():
            report_name = "PFUND_PerformanceQuality_" + self._fund_name

            #TODO GIP is placeholder
            InvestmentsReportRunner().execute(
                data=input_data,
                template="PFUND_PerformanceQuality_Template.xlsx",
                save=True,
                report_name=report_name,
                runner=self._runner,
                entity_name='GIP',
                entity_display_name=self._entity_type,
                entity_type=ReportingEntityTypes.portfolio,
                entity_source=DaoSource.PubDwh,
            )

    def run(self, **kwargs):
        self.generate_performance_quality_report()
        return True
