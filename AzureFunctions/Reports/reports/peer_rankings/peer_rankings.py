import pandas as pd
import datetime as dt
import calendar
import os
from gcm.Dao.DaoSources import DaoSource
from gcm.inv.dataprovider.factor import Factor
from gcm.inv.scenario import Scenario
from gcm.inv.quantlib.enum_source import Periodicity
from gcm.inv.quantlib.timeseries.transformer.aggregate_from_daily import AggregateFromDaily
from gcm.inv.quantlib.peer_ranking.ranking import PeerRank
from sklearn.preprocessing import MinMaxScaler
from gcm.inv.reporting.core.reporting_runner_base import ReportingRunnerBase

scaler = MinMaxScaler()


class PeerRankings(ReportingRunnerBase):
    def __init__(self):
        super().__init__(runner=Scenario.get_attribute("runner"))
        self._as_of_date = Scenario.get_attribute("as_of_date")
        self._end_date = dt.date(self._as_of_date.year, self._as_of_date.month,
                                 calendar.monthrange(self._as_of_date.year, self._as_of_date.month)[1])

    def _get_pubdwh_benchmark_returns(self):
        def my_dao_operation(dao, params):
            raw = '''
            SELECT a.PeriodDate, RateOfReturn, b.Name, b.ReportDisplayName, b.BloombergTicker, b.Description
            FROM [analyticsdata].[FinancialIndexReturnFact] a
            left join analyticsdata.FinancialIndexDimn b
            on a.FinancialIndexMasterId = b.MasterId
            order by Name, PeriodDate
            '''
            df = pd.read_sql(
                raw,
                dao.data_engine.session.bind,
            )
            return df

        df = self._runner.execute(
            params={},
            source=DaoSource.PubDwh,
            operation=my_dao_operation,
        )
        return df

    def _build_data(self, peer_group_name, peer_group_returns):
        peer_group_returns = peer_group_returns.reset_index()
        data_ = peer_group_returns.melt(id_vars=['Date'],
                                        value_vars=peer_group_returns.columns,
                                        value_name='Return',
                                        var_name='InvestmentGroupId')

        benchmark_mapping = pd.read_csv(os.path.dirname(__file__) + "/peer_benchmark_mapping.csv")
        benchmark = benchmark_mapping[benchmark_mapping['GCM Peer Group'] == peer_group_name]['Benchmark 1'].squeeze()
        data_['Benchmark'] = benchmark

        benchmark_returns = self._get_pubdwh_benchmark_returns()
        benchmark_returns = benchmark_returns[['PeriodDate', 'RateOfReturn', 'Name']]
        benchmark_returns.rename(
            columns={'PeriodDate': 'Date', 'RateOfReturn': 'BenchmarkReturn', 'Name': 'Benchmark'},
            inplace=True)
        benchmark_returns['Date'] = pd.to_datetime(benchmark_returns['Date'])
        data_ = data_.merge(benchmark_returns, how='left')

        data_ = data_.dropna(subset=['Return', 'BenchmarkReturn'])
        data_['Return'] = data_['Return'].astype(float)
        data_['Return'] = data_['Return'].clip(-0.75, 0.75)
        data_['XR'] = data_['Return'] - data_['BenchmarkReturn']
        data_['in_idx'] = None
        data_['Type'] = None
        data_['Foreign'] = None
        return data_

    def _get_equity_factor_returns(self):
        equity_factor_tickers = ['MSZZCDPV Index',
                                 'MSZZDYLD Index',
                                 'MSZZGRW Index',
                                 'MSZZGRWD Index',
                                 'MSZZGRVL Index',
                                 'MSZZLEV Index',
                                 'MSZZMOMO Index',
                                 'MSZZEVOL Index',
                                 'MSZZQLTY Index',
                                 'MSZZSIZE INDEX',
                                 'MSZZVAL Index']
        equity_factor = Factor(tickers=equity_factor_tickers)
        equity_factor_returns = equity_factor.get_returns(start_date=dt.date(1990, 1, 2),
                                                          end_date=self._end_date,
                                                          fill_na=True)
        equity_factor_returns = AggregateFromDaily().transform(
            data=equity_factor_returns,
            method="geometric",
            period=Periodicity.Monthly,
            first_of_day=True
        )
        equity_factor_returns = equity_factor_returns.reset_index()

        return equity_factor_returns

    def _get_macro_factor_returns(self):
        macro_factor_tickers = ['2S_10S_RATIO',
                                'BETA_ADJ_EM_DEV_FX',
                                'BETA_ADJ_EM_MINUS_DEV_EQUITY',
                                'BETA_ADJ_HY_IG_SPREAD',
                                'COMMODITY_ROLL',
                                'FX_CARRY',
                                'IG_TREASURY_SPREAD',
                                'IMPLIED_REALIZED_VOL',
                                'MOVE_VOLATILITY',
                                'MSCI_WORLD_LOG',
                                'MSXXH13F_EX_ACWI_BETA_ADJ',
                                'US_10Y_SWAP_SPREAD',
                                'US_HIGH_SHORT_INTEREST_MS_BASKET',
                                'VIX_LEVEL']
        macro_factor = Factor(tickers=[])
        inventory = macro_factor.get_factor_inventory()
        macro_factor_source_tickers = inventory[inventory['GcmTicker'].isin(macro_factor_tickers)][
            'SourceTicker']
        macro_factor = Factor(tickers=macro_factor_source_tickers.tolist())
        macro_factor_returns = macro_factor.get_returns(start_date=dt.date(1990, 1, 2),
                                                        end_date=self._end_date,
                                                        fill_na=True)
        macro_factor_returns = AggregateFromDaily().transform(
            data=macro_factor_returns,
            method="geometric",
            period=Periodicity.Monthly,
            first_of_day=True
        )

        source_ticker_mapping = dict(zip(inventory['SourceTicker'], inventory['GcmTicker']))
        macro_factor_returns.columns = macro_factor_returns.columns.map(source_ticker_mapping)
        macro_factor_returns = macro_factor_returns.reset_index()

        return macro_factor_returns

    def calculate_peer_rankings(self, peer_group_name, peer_group_returns):
        data_ = self._build_data(peer_group_name=peer_group_name,
                                 peer_group_returns=peer_group_returns)
        equity_factor_returns = self._get_equity_factor_returns()
        macro_factor_returns = self._get_macro_factor_returns()

        config = {'equity_cols': [],
                  'macro_cols': [],
                  'signals': [],
                  'end_month': self._as_of_date.strftime('%Y-%m-01'),
                  'num_months': 36,
                  'min_months': 24,
                  'multi': True,
                  'PGID': peer_group_name
                  }

        ranks = PeerRank(data_,
                         config,
                         equity_factor_returns=equity_factor_returns,
                         macro_factor_returns=macro_factor_returns)
        ranks = ranks.get_peer_ranks_zscore()

        return ranks

    def run(self, **kwargs):
        return self.calculate_peer_rankings(**kwargs)
