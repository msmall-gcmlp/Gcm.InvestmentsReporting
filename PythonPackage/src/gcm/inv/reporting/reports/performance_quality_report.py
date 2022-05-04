import datetime as dt
import pandas as pd
import ast
from gcm.Dao.DaoSources import DaoSource
from gcm.inv.reporting.core.ReportStructure.report_structure import ReportingEntityTypes
from gcm.inv.reporting.core.Runners.investmentsreporting import InvestmentsReportRunner
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
        self.__all_gcm_peer_constituent_returns = None
        self.__all_eurekahedge_constituent_returns = None

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
    def _all_gcm_peer_constituent_returns(self):
        if self.__all_gcm_peer_constituent_returns is None:
            returns = pd.read_json(self._params['gcm_peer_constituent_returns'], orient='index')
            returns_columns = [ast.literal_eval(x) for x in returns.columns]
            returns_columns = pd.MultiIndex.from_tuples(returns_columns,
                                                        names=['PeerGroupName', 'SourceInvestmentId'])
            returns.columns = returns_columns
            self.__all_gcm_peer_constituent_returns = returns
        return self.__all_gcm_peer_constituent_returns

    @property
    def _all_eurekahedge_constituent_returns(self):
        if self.__all_eurekahedge_constituent_returns is None:
            returns = pd.read_json(self._params['eurekahedge_constituent_returns'], orient='index')
            returns_columns = [ast.literal_eval(x) for x in returns.columns]
            returns_columns = pd.MultiIndex.from_tuples(returns_columns,
                                                        names=['EurekahedgeBenchmark', 'SourceInvestmentId'])
            returns.columns = returns_columns
            self.__all_eurekahedge_constituent_returns = returns
        return self.__all_eurekahedge_constituent_returns

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
    def _primary_peer_group(self):
        return self._fund_dimn['ReportingPeerGroup'].squeeze()

    @property
    def _secondary_peer_group(self):
        return ' '

    @property
    def _primary_peer_returns(self):
        return self._all_gcm_peer_returns[self._primary_peer_group].squeeze()

    @property
    def _secondary_peer_returns(self):
        return self._all_gcm_peer_returns[self._secondary_peer_group].squeeze()

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

    @property
    def _primary_peer_constituent_returns(self):
        peer_group_index = \
            self._all_gcm_peer_constituent_returns.columns.get_level_values(0) == self._primary_peer_group
        returns = self._all_gcm_peer_constituent_returns.loc[:, peer_group_index]
        returns = returns.droplevel(0, axis=1)
        return returns

    @property
    def _secondary_peer_constituent_returns(self):
        peer_group_index = self._all_gcm_peer_constituent_returns.columns.get_level_values(
            0) == self._secondary_peer_group
        if any(peer_group_index):
            returns = self._all_gcm_peer_constituent_returns.loc[:, peer_group_index]
            returns = returns.droplevel(0, axis=1)
        else:
            returns = pd.DataFrame()
        return returns

    @property
    def _eurekahedge_constituent_returns(self):
        eh_index = self._all_eurekahedge_constituent_returns.columns.get_level_values(0) == self._eurekahedge_benchmark
        returns = self._all_eurekahedge_constituent_returns.loc[:, eh_index]
        returns = returns.droplevel(0, axis=1)
        return returns

    @property
    def _ehi200_constituent_returns(self):
        ehi200 = 'Eurekahedge Institutional 200'
        ehi200_index = self._all_eurekahedge_constituent_returns.columns.get_level_values(0) == ehi200
        returns = self._all_eurekahedge_constituent_returns.loc[:, ehi200_index]
        returns = returns.droplevel(0, axis=1)
        return returns

    def get_header_info(self):
        header = pd.DataFrame({'header_info': [self._fund_name, self._entity_type, self._as_of_date]})
        return header

    def get_peer_group_heading(self):
        return pd.DataFrame({'peer_group_heading': ['v. ' + self._primary_peer_group + ' Peer']})

    def get_absolute_return_benchmark(self):
        return pd.DataFrame({'absolute_return_benchmark': [self._abs_return_benchmark]})

    def get_eurekahedge_benchmark_heading(self):
        return pd.DataFrame({'eurekahedge_benchmark_heading': ['v. ' + self._eurekahedge_benchmark]})

    def get_peer_ptile_1_heading(self):
        return pd.DataFrame({'peer_ptile_1_heading': [self._primary_peer_group]})

    def get_peer_ptile_2_heading(self):
        return pd.DataFrame({'peer_ptile_2_heading': [self._secondary_peer_group]})

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

    def _get_excess_return_summary(self, fund_returns, benchmark_returns, benchmark_name):
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

    def _calculate_percentile(self, constituent_returns, fund_periodic_return, period):
        periodic = self._analytics.compute_periodic_return(constituent_returns, period=period,
                                                           as_of_date=self._as_of_date, method='geometric')
        periodics = pd.concat([pd.Series([fund_periodic_return.squeeze()]), periodic], axis=0)
        ptile = periodics.rank(pct=True)[0:1].squeeze().round(2) * 100
        return ptile

    def _get_percentile_summary(self, fund_returns, constituent_returns, group_name):
        fund_returns = fund_returns.copy()
        constituent_returns = constituent_returns.copy()

        if len(constituent_returns) > 0:
            mtd_ptile = self._calculate_percentile(constituent_returns=constituent_returns,
                                                   fund_periodic_return=fund_returns.loc['MTD'],
                                                   period=PeriodicROR.MTD)
            qtd_ptile = self._calculate_percentile(constituent_returns=constituent_returns,
                                                   fund_periodic_return=fund_returns.loc['QTD'],
                                                   period=PeriodicROR.QTD)
            ytd_ptile = self._calculate_percentile(constituent_returns=constituent_returns,
                                                   fund_periodic_return=fund_returns.loc['YTD'],
                                                   period=PeriodicROR.YTD)

            summary = pd.DataFrame({group_name: [mtd_ptile,
                                                 qtd_ptile,
                                                 ytd_ptile]},
                                   index=['MTD', 'QTD', 'YTD'])
        else:
            summary = pd.DataFrame({group_name: [' ',
                                                 ' ',
                                                 ' ']},
                                   index=['MTD', 'QTD', 'YTD'])

        return summary

    def build_benchmark_summary(self):
        fund_returns = self._get_return_summary(returns=self._fund_returns, return_type='Fund')
        absolute_return_summary = self._get_excess_return_summary(fund_returns=fund_returns,
                                                                  benchmark_returns=self._abs_bmrk_returns,
                                                                  benchmark_name='AbsoluteReturnBenchmark')
        gcm_peer_summary = self._get_excess_return_summary(fund_returns=fund_returns,
                                                           benchmark_returns=self._primary_peer_returns,
                                                           benchmark_name='GcmPeer')
        ehi_50_summary = self._get_excess_return_summary(fund_returns=fund_returns,
                                                         benchmark_returns=self._ehi50_returns,
                                                         benchmark_name='EHI50')
        ehi_200_summary = self._get_excess_return_summary(fund_returns=fund_returns,
                                                          benchmark_returns=self._ehi200_returns,
                                                          benchmark_name='EHI200')

        primary_peer_percentiles = self._get_percentile_summary(fund_returns=fund_returns,
                                                            constituent_returns=self._primary_peer_constituent_returns,
                                                            group_name='Peer1Ptile')

        #TODO swap out with 2nd peer group (or ehi 50 prop)
        secondary_peer_percentiles = \
            self._get_percentile_summary(fund_returns=fund_returns,
                                         constituent_returns=self._secondary_peer_constituent_returns,
                                         group_name='Peer2Ptile')
        eurekahedge_percentiles = \
            self._get_percentile_summary(fund_returns=fund_returns,
                                         constituent_returns=self._eurekahedge_constituent_returns,
                                         group_name='EH50Ptile')

        ehi200_percentiles = \
            self._get_percentile_summary(fund_returns=fund_returns,
                                         constituent_returns=self._ehi200_constituent_returns,
                                         group_name='EHI200Ptile')

        summary = absolute_return_summary.copy()
        summary = summary.merge(gcm_peer_summary.drop(columns={'Fund'}), left_index=True, right_index=True)
        summary = summary.merge(ehi_50_summary.drop(columns={'Fund'}), left_index=True, right_index=True)
        summary = summary.merge(ehi_200_summary.drop(columns={'Fund'}), left_index=True, right_index=True)
        summary = summary.merge(primary_peer_percentiles, left_index=True, right_index=True)
        summary = summary.merge(secondary_peer_percentiles, left_index=True, right_index=True)
        summary = summary.merge(eurekahedge_percentiles, left_index=True, right_index=True)
        summary = summary.merge(ehi200_percentiles, left_index=True, right_index=True)
        return summary

    def generate_performance_quality_report(self):
        header_info = self.get_header_info()
        return_summary = self.build_benchmark_summary()
        absolute_return_benchmark = self.get_absolute_return_benchmark()
        peer_group_heading = self.get_peer_group_heading()
        eurekahedge_benchmark_heading = self.get_eurekahedge_benchmark_heading()
        peer_ptile_1_heading = self.get_peer_ptile_1_heading()
        peer_ptile_2_heading = self.get_peer_ptile_2_heading()

        input_data = {
            "header_info": header_info,
            "benchmark_summary": return_summary,
            "absolute_return_benchmark": absolute_return_benchmark,
            "peer_group_heading": peer_group_heading,
            "eurekahedge_benchmark_heading": eurekahedge_benchmark_heading,
            "peer_ptile_1_heading": peer_ptile_1_heading,
            "peer_ptile_2_heading": peer_ptile_2_heading,
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
