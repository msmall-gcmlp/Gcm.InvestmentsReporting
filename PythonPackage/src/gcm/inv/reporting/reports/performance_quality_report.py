import json
import pandas as pd
import ast
import numpy as np
from gcm.Dao.DaoSources import DaoSource
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import AzureDataLakeDao
from gcm.inv.reporting.core.ReportStructure.report_structure import ReportingEntityTypes
from gcm.inv.reporting.core.Runners.investmentsreporting import InvestmentsReportRunner
from gcm.Scenario.scenario import Scenario
from gcm.inv.quantlib.enum_source import PeriodicROR, Periodicity
from gcm.inv.quantlib.timeseries.analytics import Analytics
from gcm.inv.quantlib.timeseries.transformer.aggregate_from_daily import AggregateFromDaily
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
        self.__all_exposure = None
        self.__all_rba = None
        self.__inputs = None
        self.__market_factor_returns = None

    def download_performance_quality_report_inputs(self) -> dict:
        location = "lab/rqs/azurefunctiondata"
        read_params = AzureDataLakeDao.create_get_data_params(
            location,
            "performance_quality_report_inputs.json",
            retry=False,
        )
        file = self._runner.execute(
            params=read_params,
            source=DaoSource.DataLake,
            operation=lambda dao, params: dao.get_data(read_params)
        )
        return json.loads(file.content)

    @property
    def _inputs(self):
        if self.__inputs is None:
            self.__inputs = self.download_performance_quality_report_inputs()
        return self.__inputs

    @property
    def _all_fund_dimn(self):
        if self.__all_fund_dimn is None:
            self.__all_fund_dimn = pd.read_json(self._inputs['fund_dimn'], orient='index')
        return self.__all_fund_dimn

    @property
    def _all_fund_returns(self):
        if self.__all_fund_returns is None:
            self.__all_fund_returns = pd.read_json(self._inputs['fund_returns'], orient='index')
        return self.__all_fund_returns

    @property
    def _all_abs_bmrk_returns(self):
        if self.__all_abs_bmrk_returns is None:
            returns = pd.read_json(self._inputs['abs_bmrk_returns'], orient='index')
            if len(returns) > 0:
                self.__all_abs_bmrk_returns = AggregateFromDaily().transform(data=returns, method='geometric',
                                                                             period=Periodicity.Monthly)
            else:
                self.__all_abs_bmrk_returns = pd.DataFrame()
        return self.__all_abs_bmrk_returns

    @property
    def _all_gcm_peer_returns(self):
        if self.__all_gcm_peer_returns is None:
            self.__all_gcm_peer_returns = pd.read_json(self._inputs['gcm_peer_returns'], orient='index')
        return self.__all_gcm_peer_returns

    @property
    def _all_eurekahedge_returns(self):
        if self.__all_eurekahedge_returns is None:
            self.__all_eurekahedge_returns = pd.read_json(self._inputs['eurekahedge_returns'], orient='index')
        return self.__all_eurekahedge_returns

    @property
    def _all_gcm_peer_constituent_returns(self):
        if self.__all_gcm_peer_constituent_returns is None:
            returns = pd.read_json(self._inputs['gcm_peer_constituent_returns'], orient='index')
            returns_columns = [ast.literal_eval(x) for x in returns.columns]
            returns_columns = pd.MultiIndex.from_tuples(returns_columns,
                                                        names=['PeerGroupName', 'SourceInvestmentId'])
            returns.columns = returns_columns
            self.__all_gcm_peer_constituent_returns = returns
        return self.__all_gcm_peer_constituent_returns

    @property
    def _all_eurekahedge_constituent_returns(self):
        if self.__all_eurekahedge_constituent_returns is None:
            returns = pd.read_json(self._inputs['eurekahedge_constituent_returns'], orient='index')
            returns_columns = [ast.literal_eval(x) for x in returns.columns]
            returns_columns = pd.MultiIndex.from_tuples(returns_columns,
                                                        names=['EurekahedgeBenchmark', 'SourceInvestmentId'])
            returns.columns = returns_columns
            self.__all_eurekahedge_constituent_returns = returns
        return self.__all_eurekahedge_constituent_returns

    @property
    def _all_exposure(self):
        if self.__all_exposure is None:
            latest = pd.read_json(self._inputs['exposure_latest'], orient='index')
            latest['Period'] = 'Latest'
            three = pd.read_json(self._inputs['exposure_3y'], orient='index')
            three['Period'] = '3Y'
            five = pd.read_json(self._inputs['exposure_5y'], orient='index')
            five['Period'] = '5Y'
            ten = pd.read_json(self._inputs['exposure_10y'], orient='index')
            ten['Period'] = '10Y'
            all_exposure = pd.concat([latest, three, five, ten])
            self.__all_exposure = all_exposure
        return self.__all_exposure

    @property
    def _all_rba(self):
        if self.__all_rba is None:
            rba = pd.read_json(self._inputs['rba'], orient='index')
            rba_columns = [ast.literal_eval(x) for x in rba.columns]
            rba_columns = pd.MultiIndex.from_tuples(rba_columns,
                                                    names=['FactorGroup1', 'InvestmentGroupId'])
            rba.columns = rba_columns
            self.__all_rba = rba
        return self.__all_rba

    @property
    def _market_factor_returns(self):
        if self.__market_factor_returns is None:
            returns = pd.read_json(self._inputs['market_factor_returns'], orient='index')
            if len(returns) > 0:
                returns = AggregateFromDaily().transform(data=returns, method='geometric',
                                                         period=Periodicity.Monthly)
                returns.index = [pd.datetime(x.year, x.month, 1) for x in returns.index.tolist()]
                self.__market_factor_returns = returns
            else:
                self.__market_factor_returns = pd.DataFrame()
        return self.__market_factor_returns

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
        if any(self._all_fund_returns.columns == self._fund_name):
            return self._all_fund_returns[self._fund_name].to_frame()
        else:
            return pd.DataFrame()

    @property
    def _fund_rba(self):
        fund_index = self._all_rba.columns.get_level_values(1) == self._fund_id.squeeze()
        if any(fund_index):
            fund_rba = self._all_rba.iloc[:, fund_index]
            fund_rba.columns = fund_rba.columns.droplevel(1)
            return fund_rba
        else:
            return pd.DataFrame()

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
        if any(self._all_gcm_peer_returns.columns == self._primary_peer_group):
            return self._all_gcm_peer_returns[self._primary_peer_group].squeeze()
        else:
            return pd.Series()

    @property
    def _secondary_peer_returns(self):
        if any(self._all_gcm_peer_returns.columns == self._secondary_peer_returns):
            return self._all_gcm_peer_returns[self._secondary_peer_returns].squeeze()
        else:
            return pd.Series()

    @property
    def _eurekahedge_benchmark(self):
        return self._fund_dimn['EurekahedgeBenchmark'].squeeze()

    @property
    def _abs_return_benchmark(self):
        return self._fund_dimn['AbsoluteBenchmarkName'].squeeze()

    @property
    def _ehi50_returns(self):
        if any(self._all_eurekahedge_returns.columns == self._eurekahedge_benchmark):
            return self._all_eurekahedge_returns[self._eurekahedge_benchmark].squeeze()
        else:
            return pd.Series()

    @property
    def _ehi200_returns(self):
        return self._all_eurekahedge_returns['Eurekahedge Institutional 200'].squeeze()

    @property
    def _sp500_return(self):
        returns = self._market_factor_returns['SPXT Index']
        returns.name = 'SP500'
        return returns.to_frame()

    @property
    def _rf_return(self):
        returns = self._market_factor_returns['SBMMTB1 Index']
        returns.name = '1M_RiskFree'
        return returns.to_frame()

    @property
    def _primary_peer_constituent_returns(self):
        peer_group_index = \
            self._all_gcm_peer_constituent_returns.columns.get_level_values(0) == self._primary_peer_group
        if any(peer_group_index):
            returns = self._all_gcm_peer_constituent_returns.loc[:, peer_group_index]
            returns = returns.droplevel(0, axis=1)
        else:
            returns = pd.DataFrame()
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
        if any(self._all_eurekahedge_constituent_returns.columns.get_level_values(0) == self._eurekahedge_benchmark):
            eh_index = \
                self._all_eurekahedge_constituent_returns.columns.get_level_values(0) == self._eurekahedge_benchmark
            returns = self._all_eurekahedge_constituent_returns.loc[:, eh_index]
            returns = returns.droplevel(0, axis=1)
        else:
            returns = pd.Series()

        return returns

    @property
    def _ehi200_constituent_returns(self):
        ehi200 = 'Eurekahedge Institutional 200'
        ehi200_index = self._all_eurekahedge_constituent_returns.columns.get_level_values(0) == ehi200
        returns = self._all_eurekahedge_constituent_returns.loc[:, ehi200_index]
        returns = returns.droplevel(0, axis=1)
        return returns

    @property
    def _exposure(self):
        if any(self._all_exposure['InvestmentGroupName'] == self._fund_name):
            exposure = self._all_exposure[self._all_exposure['InvestmentGroupName'] == self._fund_name]
            exposure = exposure.set_index('Period')
            return exposure
        else:
            return pd.DataFrame(columns=['LongNotional', 'ShortNotional', 'GrossNotional', 'NetNotional'],
                                index=['Latest', '3Y', '5Y', '10Y'])

    @property
    def _latest_exposure_date(self):
        return self._exposure.loc['Latest']['Date']

    def get_header_info(self):
        header = pd.DataFrame({'header_info': [self._fund_name, self._entity_type, self._as_of_date]})
        return header

    def get_peer_group_heading(self):
        if self._primary_peer_group is not None:
            group = self._primary_peer_group + ' Peer'
            group = group.replace('GCM ', '')
            return pd.DataFrame({'peer_group_heading': ['v. ' + group]})
        else:
            return pd.DataFrame({'peer_group_heading': ['v. GCM Peer']})

    def get_absolute_return_benchmark(self):
        if self._abs_return_benchmark is not None:
            return pd.DataFrame({'absolute_return_benchmark': [self._abs_return_benchmark]})
        else:
            return pd.DataFrame({'absolute_return_benchmark': ['N/A']})

    def get_eurekahedge_benchmark_heading(self):
        if self._eurekahedge_benchmark is not None:
            return pd.DataFrame({'eurekahedge_benchmark_heading': ['v. ' + self._eurekahedge_benchmark]})
        else:
            return pd.DataFrame({'eurekahedge_benchmark_heading': ['v. EHI Index']})

    def get_peer_ptile_1_heading(self):
        if self._primary_peer_group is not None:
            return pd.DataFrame({'peer_ptile_1_heading': [self._primary_peer_group]})
        else:
            return pd.DataFrame({'peer_ptile_1_heading': ['']})

    def get_peer_ptile_2_heading(self):
        if self._secondary_peer_group is not None:
            return pd.DataFrame({'peer_ptile_2_heading': [self._secondary_peer_group]})
        else:
            return pd.DataFrame({'peer_ptile_2_heading': ['']})

    def get_latest_exposure_heading(self):
        if self._latest_exposure_date is not None:
            heading = 'Latest (' + self._latest_exposure_date.strftime('%b %Y') + ')'
            return pd.DataFrame({'latest_exposure_heading': [heading]})
        else:
            return pd.DataFrame({'peer_ptile_2_heading': ['']})

    def _get_return_summary(self, returns, return_type):
        returns = returns.copy()
        mtd_return = self._analytics.compute_periodic_return(ror=returns, period=PeriodicROR.MTD,
                                                             as_of_date=self._as_of_date, method='geometric')

        qtd_return = self._analytics.compute_periodic_return(ror=returns, period=PeriodicROR.QTD,
                                                             as_of_date=self._as_of_date, method='geometric')

        ytd_return = self._analytics.compute_periodic_return(ror=returns, period=PeriodicROR.YTD,
                                                             as_of_date=self._as_of_date, method='geometric')

        trailing_1y_return = self._analytics.compute_trailing_return(ror=returns, window=12,
                                                                     as_of_date=self._as_of_date, method='geometric',
                                                                     periodicity=Periodicity.Monthly,
                                                                     annualize=True)

        trailing_3y_return = self._analytics.compute_trailing_return(ror=returns, window=36,
                                                                     as_of_date=self._as_of_date, method='geometric',
                                                                     periodicity=Periodicity.Monthly,
                                                                     annualize=True)

        trailing_5y_return = self._analytics.compute_trailing_return(ror=returns, window=60,
                                                                     as_of_date=self._as_of_date, method='geometric',
                                                                     periodicity=Periodicity.Monthly,
                                                                     annualize=True)

        trailing_10y_return = self._analytics.compute_trailing_return(ror=returns, window=120,
                                                                      as_of_date=self._as_of_date, method='geometric',
                                                                      periodicity=Periodicity.Monthly,
                                                                      annualize=True)

        # rounding to 2 so that Excess Return matches optically
        stats = [mtd_return, qtd_return, ytd_return,
                 trailing_1y_return, trailing_3y_return, trailing_5y_return, trailing_10y_return]
        stats = [x.squeeze() for x in stats]
        summary = pd.DataFrame({return_type: [round(x, 2) if isinstance(x, float) else ' ' for x in stats]},
                               index=['MTD', 'QTD', 'YTD', 'TTM', '3Y', '5Y', '10Y'])
        return summary

    def _get_excess_return_summary(self, fund_returns, benchmark_returns, benchmark_name):
        fund_returns = fund_returns.copy()
        benchmark_returns = benchmark_returns.copy()
        if benchmark_returns.shape[0] > 0:
            benchmark_returns = self._get_return_summary(returns=benchmark_returns, return_type=benchmark_name)
            summary = fund_returns.merge(benchmark_returns, left_index=True, right_index=True)
            summary['IsNumeric'] = summary.applymap(np.isreal).all(1)
            excess = summary[summary['IsNumeric']]['Fund'] - summary[summary['IsNumeric']][benchmark_name]
            summary[benchmark_name + 'Excess'] = excess.round(2)
            summary.drop(columns={'IsNumeric'}, inplace=True)
            summary = summary.fillna('')
        else:
            summary = fund_returns.copy()
            summary[benchmark_name] = ''
            summary[benchmark_name + 'Excess'] = ''

        return summary

    def _calculate_periodic_percentile(self, constituent_returns, fund_periodic_return, period):
        periodic = self._analytics.compute_periodic_return(ror=constituent_returns,
                                                           period=period,
                                                           as_of_date=self._as_of_date,
                                                           method='geometric')
        periodics = pd.concat([pd.Series([fund_periodic_return.squeeze()]), periodic], axis=0)
        ptile = periodics.rank(pct=True)[0:1].squeeze().round(2) * 100
        return ptile

    def _calculate_trailing_percentile(self, constituent_returns, fund_periodic_return, trailing_months):
        returns = self._analytics.compute_trailing_return(ror=constituent_returns,
                                                          window=trailing_months,
                                                          as_of_date=self._as_of_date,
                                                          method='geometric',
                                                          annualize=True,
                                                          periodicity=Periodicity.Monthly)
        if isinstance(fund_periodic_return.squeeze(), float):
            returns = pd.concat([pd.Series([fund_periodic_return.squeeze()]), returns], axis=0)
            ptile = returns.rank(pct=True)[0:1].squeeze().round(2) * 100
        else:
            ptile = ''
        return ptile

    def _get_percentile_summary(self, fund_returns, constituent_returns, group_name):
        fund_returns = fund_returns.copy()
        constituent_returns = constituent_returns.copy()
        index = ['MTD', 'QTD', 'YTD', 'TTM', '3Y', '5Y', '10Y']

        if len(constituent_returns) > 0:
            mtd_ptile = self._calculate_periodic_percentile(constituent_returns=constituent_returns,
                                                            fund_periodic_return=fund_returns.loc['MTD'],
                                                            period=PeriodicROR.MTD)
            qtd_ptile = self._calculate_periodic_percentile(constituent_returns=constituent_returns,
                                                            fund_periodic_return=fund_returns.loc['QTD'],
                                                            period=PeriodicROR.QTD)
            ytd_ptile = self._calculate_periodic_percentile(constituent_returns=constituent_returns,
                                                            fund_periodic_return=fund_returns.loc['YTD'],
                                                            period=PeriodicROR.YTD)
            trailing_1y_ptile = self._calculate_trailing_percentile(constituent_returns=constituent_returns,
                                                                    fund_periodic_return=fund_returns.loc['TTM'],
                                                                    trailing_months=12)
            trailing_3y_ptile = self._calculate_trailing_percentile(constituent_returns=constituent_returns,
                                                                    fund_periodic_return=fund_returns.loc['3Y'],
                                                                    trailing_months=36)
            trailing_5y_ptile = self._calculate_trailing_percentile(constituent_returns=constituent_returns,
                                                                    fund_periodic_return=fund_returns.loc['5Y'],
                                                                    trailing_months=60)
            trailing_10y_ptile = self._calculate_trailing_percentile(constituent_returns=constituent_returns,
                                                                     fund_periodic_return=fund_returns.loc['10Y'],
                                                                     trailing_months=120)

            summary = pd.DataFrame({group_name: [mtd_ptile,
                                                 qtd_ptile,
                                                 ytd_ptile,
                                                 trailing_1y_ptile,
                                                 trailing_3y_ptile,
                                                 trailing_5y_ptile,
                                                 trailing_10y_ptile]},
                                   index=index)
        else:
            summary = pd.DataFrame({group_name: [''] * len(index)}, index=index)

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

        primary_peer_percentiles = \
            self._get_percentile_summary(fund_returns=fund_returns,
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

    @staticmethod
    def _get_exposure_summary(exposure):
        index = pd.DataFrame(index=['Latest', '3Y', '5Y', '10Y'])
        summary = exposure[['LongNotional', 'ShortNotional', 'GrossNotional', 'NetNotional']]
        summary = index.merge(summary, left_index=True, right_index=True, how='left')
        summary = summary.loc[['Latest', '3Y', '5Y', '10Y']]
        summary = summary.round(2)
        return summary

    def _get_rba_summary(self):
        factor_group_index = pd.DataFrame(index=['Market Beta', 'Region', 'Industries', 'Styles',
                                                 'Hedge Fund Technicals', 'Selection Risk', 'Unexplained'])
        mtd = self._analytics.compute_periodic_return(ror=self._fund_rba,
                                                      period=PeriodicROR.MTD,
                                                      as_of_date=self._as_of_date,
                                                      method='arithmetic')

        qtd = self._analytics.compute_periodic_return(ror=self._fund_rba,
                                                      period=PeriodicROR.QTD,
                                                      as_of_date=self._as_of_date,
                                                      method='arithmetic')

        ytd = self._analytics.compute_periodic_return(ror=self._fund_rba,
                                                      period=PeriodicROR.YTD,
                                                      as_of_date=self._as_of_date,
                                                      method='arithmetic')

        ttm = self._analytics.compute_trailing_return(ror=self._fund_rba,
                                                      window=12,
                                                      as_of_date=self._as_of_date,
                                                      method='arithmetic',
                                                      periodicity=Periodicity.Monthly,
                                                      annualize=True,
                                                      include_history=False)

        t3y = self._analytics.compute_trailing_return(ror=self._fund_rba,
                                                      window=36,
                                                      as_of_date=self._as_of_date,
                                                      method='arithmetic',
                                                      periodicity=Periodicity.Monthly,
                                                      annualize=True,
                                                      include_history=False)

        t5y = self._analytics.compute_trailing_return(ror=self._fund_rba,
                                                      window=60,
                                                      as_of_date=self._as_of_date,
                                                      method='arithmetic',
                                                      periodicity=Periodicity.Monthly,
                                                      annualize=True,
                                                      include_history=False)

        t10y = self._analytics.compute_trailing_return(ror=self._fund_rba,
                                                       window=120,
                                                       as_of_date=self._as_of_date,
                                                       method='arithmetic',
                                                       periodicity=Periodicity.Monthly,
                                                       annualize=True,
                                                       include_history=False)

        #TODO only fill na if some non na's
        summary = factor_group_index.merge(mtd, left_index=True, right_index=True, how='left')
        summary = summary.merge(qtd, left_index=True, right_index=True, how='left')
        summary = summary.merge(ytd, left_index=True, right_index=True, how='left')
        summary = summary.merge(ttm, left_index=True, right_index=True, how='left')
        summary = summary.merge(t3y, left_index=True, right_index=True, how='left')
        summary = summary.merge(t5y, left_index=True, right_index=True, how='left')
        summary = summary.merge(t10y, left_index=True, right_index=True, how='left')
        summary.columns = ['MTD', 'QTD', 'YTD', 'TTM', '3Y', '5Y', '10Y']
        summary = summary.T
        summary = summary.round(2)

        #fill na unless everything is NA
        summary[~summary.isna().all(axis=1)] = summary[~summary.isna().all(axis=1)].fillna(0)
        return summary

    def build_exposure_summary(self):
        summary = self._get_exposure_summary(self._exposure)
        return summary

    def build_rba_summary(self):
        if self._fund_rba.shape[0] > 0:
            fund_returns = self._get_return_summary(returns=self._fund_returns, return_type='Fund')
            fund_returns.rename(columns={'Fund': 'Total'}, inplace=True)
            rba = self._get_rba_summary()
            summary = fund_returns.merge(rba, left_index=True, right_index=True)
        else:
            summary = pd.DataFrame(index=['MTD', 'QTD', 'YTD', 'TTM', '3Y', '5Y', '10Y'],
                                   columns=['Total', 'Market Beta', 'Region', 'Industries', 'Styles',
                                            'Hedge Fund Technicals', 'Selection Risk', 'Unexplained'])
        return summary

    def _get_trailing_vol(self, returns, trailing_months):
        return self._analytics.compute_trailing_vol(ror=returns,
                                                    window=trailing_months,
                                                    as_of_date=self._as_of_date,
                                                    periodicity=Periodicity.Monthly,
                                                    annualize=True)

    def _get_trailing_beta(self, returns, trailing_months):
        return self._analytics.compute_trailing_beta(ror=returns,
                                                     benchmark_ror=self._sp500_return,
                                                     window=trailing_months,
                                                     as_of_date=self._as_of_date,
                                                     periodicity=Periodicity.Monthly)

    def _get_trailing_sharpe(self, returns, trailing_months):
        return self._analytics.compute_trailing_sharpe_ratio(ror=returns,
                                                             rf_ror=self._rf_return,
                                                             window=trailing_months,
                                                             as_of_date=self._as_of_date,
                                                             periodicity=Periodicity.Monthly)

    def _get_trailing_win_loss_ratio(self, returns, trailing_months):
        return self._analytics.compute_trailing_win_loss_ratio(ror=returns,
                                                               window=trailing_months,
                                                               as_of_date=self._as_of_date,
                                                               periodicity=Periodicity.Monthly)

    def _get_trailing_batting_avg(self, returns, trailing_months):
        return self._analytics.compute_trailing_batting_average(ror=returns,
                                                                window=trailing_months,
                                                                as_of_date=self._as_of_date,
                                                                periodicity=Periodicity.Monthly)

    def _get_rolling_return(self, returns, trailing_months):
        return self._analytics.compute_trailing_return(ror=returns,
                                                       window=trailing_months,
                                                       as_of_date=self._as_of_date,
                                                       method='geometric',
                                                       periodicity=Periodicity.Monthly,
                                                       annualize=True,
                                                       include_history=True)

    def _get_rolling_sharpe_ratio(self, returns, trailing_months):
        return self._analytics.compute_trailing_sharpe_ratio(ror=returns,
                                                             rf_ror=self._rf_return,
                                                             window=trailing_months,
                                                             as_of_date=self._as_of_date,
                                                             periodicity=Periodicity.Monthly,
                                                             include_history=True)

    @staticmethod
    def _summarize_rolling_data(rolling_data, trailing_months):
        rolling_data = rolling_data.iloc[-trailing_months:]
        index = ['min', '25%', '75%', 'max']
        if len(rolling_data) == trailing_months:
            summary = rolling_data.describe().loc[index].round(2)
        else:
            summary = pd.DataFrame({'Fund': [''] * len(index)}, index=index)

        return summary

    def _get_fund_trailing_vol_summary(self):
        returns = self._fund_returns.copy()
        trailing_1y_vol = self._get_trailing_vol(returns=returns, trailing_months=12)
        trailing_3y_vol = self._get_trailing_vol(returns=returns, trailing_months=36)
        trailing_5y_vol = self._get_trailing_vol(returns=returns, trailing_months=60)

        stats = [trailing_1y_vol, trailing_3y_vol, trailing_5y_vol]
        stats = [x.squeeze() for x in stats]
        summary = pd.DataFrame({'Vol': [round(x, 2) if isinstance(x, float) else ' ' for x in stats]},
                               index=['TTM', '3Y', '5Y'])

        return summary

    def _get_fund_trailing_beta_summary(self):
        returns = self._fund_returns.copy()
        trailing_1y_beta = self._get_trailing_vol(returns=returns, trailing_months=12)
        trailing_3y_beta = self._get_trailing_vol(returns=returns, trailing_months=36)
        trailing_5y_beta = self._get_trailing_vol(returns=returns, trailing_months=60)

        stats = [trailing_1y_beta, trailing_3y_beta, trailing_5y_beta]
        stats = [x.squeeze() for x in stats]
        summary = pd.DataFrame({'Beta': [round(x, 2) if isinstance(x, float) else ' ' for x in stats]},
                               index=['TTM', '3Y', '5Y'])

        return summary

    def _get_fund_trailing_sharpe_summary(self):
        returns = self._fund_returns.copy()
        trailing_1y_sharpe = self._get_trailing_sharpe(returns=returns, trailing_months=12)
        trailing_3y_sharpe = self._get_trailing_sharpe(returns=returns, trailing_months=36)
        trailing_5y_sharpe = self._get_trailing_sharpe(returns=returns, trailing_months=60)

        stats = [trailing_1y_sharpe, trailing_3y_sharpe, trailing_5y_sharpe]
        stats = [x.squeeze() for x in stats]
        summary = pd.DataFrame({'Sharpe': [round(x, 2) if isinstance(x, float) else ' ' for x in stats]},
                               index=['TTM', '3Y', '5Y'])

        return summary

    def _get_fund_trailing_batting_average_summary(self):
        returns = self._fund_returns.copy()
        trailing_1y_batting_avg = self._get_trailing_batting_avg(returns=returns, trailing_months=12)
        trailing_3y_batting_avg = self._get_trailing_batting_avg(returns=returns, trailing_months=36)
        trailing_5y_batting_avg = self._get_trailing_batting_avg(returns=returns, trailing_months=60)

        stats = [trailing_1y_batting_avg, trailing_3y_batting_avg, trailing_5y_batting_avg]
        stats = [x.squeeze() for x in stats]
        summary = pd.DataFrame({'BattingAvg': [round(x, 2) if isinstance(x, float) else ' ' for x in stats]},
                               index=['TTM', '3Y', '5Y'])

        return summary

    def _get_fund_trailing_win_loss_ratio_summary(self):
        returns = self._fund_returns.copy()
        trailing_1y_win_loss = self._get_trailing_win_loss_ratio(returns=returns, trailing_months=12)
        trailing_3y_win_loss = self._get_trailing_win_loss_ratio(returns=returns, trailing_months=36)
        trailing_5y_win_loss = self._get_trailing_win_loss_ratio(returns=returns, trailing_months=60)

        stats = [trailing_1y_win_loss, trailing_3y_win_loss, trailing_5y_win_loss]
        stats = [x.squeeze() for x in stats]
        summary = pd.DataFrame({'WinLoss': [round(x, 2) if isinstance(x, float) else ' ' for x in stats]},
                               index=['TTM', '3Y', '5Y'])

        return summary

    def _get_fund_rolling_return_summary(self):
        returns = self._fund_returns.copy()
        rolling_12m_returns = self._get_rolling_return(returns=returns, trailing_months=12)
        rolling_1y_summary = self._summarize_rolling_data(rolling_data=rolling_12m_returns, trailing_months=12)
        rolling_3y_summary = self._summarize_rolling_data(rolling_data=rolling_12m_returns, trailing_months=36)
        rolling_5y_summary = self._summarize_rolling_data(rolling_data=rolling_12m_returns, trailing_months=60)

        summary = pd.concat([rolling_1y_summary.T, rolling_3y_summary.T, rolling_5y_summary.T])
        summary.index = ['TTM', '3Y', '5Y']

        return summary

    def _get_fund_rolling_sharpe_summary(self):
        returns = self._fund_returns.copy()
        rolling_12m_sharpes = self._get_rolling_sharpe_ratio(returns=returns, trailing_months=12)
        rolling_1y_summary = self._summarize_rolling_data(rolling_data=rolling_12m_sharpes, trailing_months=12)
        rolling_3y_summary = self._summarize_rolling_data(rolling_data=rolling_12m_sharpes, trailing_months=36)
        rolling_5y_summary = self._summarize_rolling_data(rolling_data=rolling_12m_sharpes, trailing_months=60)

        summary = pd.concat([rolling_1y_summary.T, rolling_3y_summary.T, rolling_5y_summary.T])
        summary.index = ['TTM', '3Y', '5Y']

        return summary

    def _get_peer_rolling_return_summary(self):
        returns = self._primary_peer_constituent_returns.copy()
        rolling_12m_returns = self._get_rolling_return(returns=returns, trailing_months=12)

        rolling_1y_summary = self._summarize_rolling_data(rolling_data=rolling_12m_returns, trailing_months=12)
        rolling_1y_summary = rolling_1y_summary.mean(axis=1)

        rolling_3y_summary = self._summarize_rolling_data(rolling_data=rolling_12m_returns, trailing_months=36)
        rolling_3y_summary = rolling_3y_summary.mean(axis=1)

        rolling_5y_summary = self._summarize_rolling_data(rolling_data=rolling_12m_returns, trailing_months=60)
        rolling_5y_summary = rolling_5y_summary.mean(axis=1)

        summary = pd.concat([rolling_1y_summary, rolling_3y_summary, rolling_5y_summary], axis=1).T
        summary.index = ['TTM', '3Y', '5Y']
        summary = summary.round(2)

        return summary

    def _get_peer_rolling_sharpe_summary(self):
        returns = self._primary_peer_constituent_returns.copy()
        rolling_12m_sharpes = self._get_rolling_sharpe_ratio(returns=returns, trailing_months=12)

        rolling_1y_summary = self._summarize_rolling_data(rolling_data=rolling_12m_sharpes, trailing_months=12)
        rolling_1y_summary = rolling_1y_summary.mean(axis=1)

        rolling_3y_summary = self._summarize_rolling_data(rolling_data=rolling_12m_sharpes, trailing_months=36)
        rolling_3y_summary = rolling_3y_summary.mean(axis=1)

        rolling_5y_summary = self._summarize_rolling_data(rolling_data=rolling_12m_sharpes, trailing_months=60)
        rolling_5y_summary = rolling_5y_summary.mean(axis=1)

        summary = pd.concat([rolling_1y_summary, rolling_3y_summary, rolling_5y_summary], axis=1).T
        summary.index = ['TTM', '3Y', '5Y']
        summary = summary.round(2)

        return summary

    def _get_peer_trailing_vol_summary(self, returns):
        returns = returns.copy()
        trailing_1y_vol = self._get_trailing_vol(returns=returns, trailing_months=12)
        trailing_3y_vol = self._get_trailing_vol(returns=returns, trailing_months=36)
        trailing_5y_vol = self._get_trailing_vol(returns=returns, trailing_months=60)

        stats = [trailing_1y_vol.mean(), trailing_3y_vol.mean(), trailing_5y_vol.mean()]
        summary = pd.DataFrame({'AvgVol': [round(x, 2) if isinstance(x, float) else ' ' for x in stats]},
                               index=['TTM', '3Y', '5Y'])

        return summary

    def _get_peer_trailing_beta_summary(self, returns):
        returns = returns.copy()
        trailing_1y_beta = self._get_trailing_beta(returns=returns, trailing_months=12)
        trailing_3y_beta = self._get_trailing_beta(returns=returns, trailing_months=36)
        trailing_5y_beta = self._get_trailing_beta(returns=returns, trailing_months=60)

        stats = [trailing_1y_beta.mean(), trailing_3y_beta.mean(), trailing_5y_beta.mean()]
        summary = pd.DataFrame({'AvgBeta': [round(x, 2) if isinstance(x, float) else ' ' for x in stats]},
                               index=['TTM', '3Y', '5Y'])

        return summary

    def _get_peer_trailing_sharpe_summary(self, returns):
        returns = returns.copy()
        trailing_1y_sharpe = self._get_trailing_sharpe(returns=returns, trailing_months=12)
        trailing_3y_sharpe = self._get_trailing_sharpe(returns=returns, trailing_months=36)
        trailing_5y_sharpe = self._get_trailing_sharpe(returns=returns, trailing_months=60)

        stats = [trailing_1y_sharpe.mean(), trailing_3y_sharpe.mean(), trailing_5y_sharpe.mean()]
        summary = pd.DataFrame({'AvgSharpe': [round(x, 2) if isinstance(x, float) else ' ' for x in stats]},
                               index=['TTM', '3Y', '5Y'])

        return summary

    def _get_peer_trailing_batting_average_summary(self, returns):
        returns = returns.copy()
        trailing_1y = self._get_trailing_batting_avg(returns=returns, trailing_months=12)
        trailing_3y = self._get_trailing_batting_avg(returns=returns, trailing_months=36)
        trailing_5y = self._get_trailing_batting_avg(returns=returns, trailing_months=60)

        stats = [trailing_1y.mean(), trailing_3y.mean(), trailing_5y.mean()]
        summary = pd.DataFrame({'AvgBattingAvg': [round(x, 2) if isinstance(x, float) else ' ' for x in stats]},
                               index=['TTM', '3Y', '5Y'])

        return summary

    def _get_peer_trailing_win_loss_ratio_summary(self, returns):
        returns = returns.copy()
        trailing_1y = self._get_trailing_win_loss_ratio(returns=returns, trailing_months=12)
        trailing_3y = self._get_trailing_win_loss_ratio(returns=returns, trailing_months=36)
        trailing_5y = self._get_trailing_win_loss_ratio(returns=returns, trailing_months=60)

        stats = [trailing_1y.mean(), trailing_3y.mean(), trailing_5y.mean()]
        summary = pd.DataFrame({'AvgWinLoss': [round(x, 2) if isinstance(x, float) else ' ' for x in stats]},
                               index=['TTM', '3Y', '5Y'])

        return summary

    def build_performance_stability_fund_summary(self):
        vol = self._get_fund_trailing_vol_summary()
        beta = self._get_fund_trailing_beta_summary()
        sharpe = self._get_fund_trailing_sharpe_summary()
        batting_avg = self._get_fund_trailing_batting_average_summary()
        win_loss = self._get_fund_trailing_win_loss_ratio_summary()

        rolling_returns = self._get_fund_rolling_return_summary()
        rolling_returns.columns = ['Return_'] + rolling_returns.columns

        rolling_sharpes = self._get_fund_rolling_sharpe_summary()
        rolling_sharpes.columns = ['Sharpe_'] + rolling_sharpes.columns

        summary = vol.merge(beta, left_index=True, right_index=True)
        summary = summary.merge(sharpe, left_index=True, right_index=True)
        summary = summary.merge(batting_avg, left_index=True, right_index=True)
        summary = summary.merge(win_loss, left_index=True, right_index=True)
        summary = summary.merge(rolling_returns, left_index=True, right_index=True)
        summary = summary.merge(rolling_sharpes, left_index=True, right_index=True)

        summary = summary[['Vol', 'Beta', 'Sharpe', 'BattingAvg', 'WinLoss',
                           'Return_min', 'Return_25%', 'Return_75%', 'Return_max',
                           'Sharpe_min', 'Sharpe_25%', 'Sharpe_75%', 'Sharpe_max']]
        return summary

    def build_performance_stability_peer_summary(self):
        peer_returns = self._primary_peer_constituent_returns.copy()

        if peer_returns.shape[0] > 0:
            vol = self._get_peer_trailing_vol_summary(returns=peer_returns)
            beta = self._get_peer_trailing_beta_summary(returns=peer_returns)
            sharpe = self._get_peer_trailing_sharpe_summary(returns=peer_returns)
            batting_avg = self._get_peer_trailing_batting_average_summary(returns=peer_returns)
            win_loss = self._get_peer_trailing_win_loss_ratio_summary(returns=peer_returns)

            rolling_returns = self._get_peer_rolling_return_summary()
            rolling_returns.columns = ['AvgReturn_'] + rolling_returns.columns

            rolling_sharpes = self._get_peer_rolling_sharpe_summary()
            rolling_sharpes.columns = ['AvgSharpe_'] + rolling_sharpes.columns

            summary = vol.merge(beta, left_index=True, right_index=True)
            summary = summary.merge(sharpe, left_index=True, right_index=True)
            summary = summary.merge(batting_avg, left_index=True, right_index=True)
            summary = summary.merge(win_loss, left_index=True, right_index=True)
            summary = summary.merge(rolling_returns, left_index=True, right_index=True)
            summary = summary.merge(rolling_sharpes, left_index=True, right_index=True)

            summary = summary[['AvgVol', 'AvgBeta', 'AvgSharpe', 'AvgBattingAvg', 'AvgWinLoss',
                               'AvgReturn_min', 'AvgReturn_25%', 'AvgReturn_75%', 'AvgReturn_max',
                               'AvgSharpe_min', 'AvgSharpe_25%', 'AvgSharpe_75%', 'AvgSharpe_max']]

        else:
            summary = pd.DataFrame(columns=['AvgVol', 'AvgBeta', 'AvgSharpe', 'AvgBattingAvg', 'AvgWinLoss',
                                            'AvgReturn_min', 'AvgReturn_25%', 'AvgReturn_75%', 'AvgReturn_max',
                                            'AvgSharpe_min', 'AvgSharpe_25%', 'AvgSharpe_75%', 'AvgSharpe_max'],
                                   index=['TTM', '3Y', '5Y'])
        return summary

    def _validate_inputs(self):
        if self._fund_returns.shape[0] == 0:
            return False
        else:
            return True

    def generate_performance_quality_report(self):
        if not self._validate_inputs():
            return 'Invalid inputs'

        header_info = self.get_header_info()
        return_summary = self.build_benchmark_summary()
        absolute_return_benchmark = self.get_absolute_return_benchmark()
        peer_group_heading = self.get_peer_group_heading()
        eurekahedge_benchmark_heading = self.get_eurekahedge_benchmark_heading()
        peer_ptile_1_heading = self.get_peer_ptile_1_heading()
        peer_ptile_2_heading = self.get_peer_ptile_2_heading()

        rba_summary = self.build_rba_summary()

        performance_stability_fund_summary = self.build_performance_stability_fund_summary()
        performance_stability_peer_summary = self.build_performance_stability_peer_summary()

        exposure_summary = self.build_exposure_summary()
        latest_exposure_heading = self.get_latest_exposure_heading()

        input_data = {
            "header_info": header_info,
            "benchmark_summary": return_summary,
            "absolute_return_benchmark": absolute_return_benchmark,
            "peer_group_heading": peer_group_heading,
            "eurekahedge_benchmark_heading": eurekahedge_benchmark_heading,
            "peer_ptile_1_heading": peer_ptile_1_heading,
            "peer_ptile_2_heading": peer_ptile_2_heading,
            "performance_stability_fund_summary": performance_stability_fund_summary,
            "performance_stability_peer_summary": performance_stability_peer_summary,
            "rba_summary": rba_summary,
            "exposure_summary": exposure_summary,
            "latest_exposure_heading": latest_exposure_heading,
        }

        with Scenario(asofdate=self._as_of_date).context():
            report_name = "PFUND_PerformanceQuality_" + self._fund_name.replace('/', '')

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
