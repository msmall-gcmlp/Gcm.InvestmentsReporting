import datetime as dt
import numpy as np
import pandas as pd
from datetime import timedelta
from gcm.inv.dataprovider.factor import Factor
from gcm.inv.scenario import Scenario
from gcm.Scenario.scenario import AggregateInterval
from gcm.inv.reporting.core.ReportStructure.report_structure import ReportingEntityTypes, ReportType, ReportVertical
from gcm.inv.reporting.core.Runners.investmentsreporting import InvestmentsReportRunner
from gcm.inv.reporting.core.reporting_runner_base import ReportingRunnerBase
from gcm.Dao.DaoRunner import DaoRunner, DaoRunnerConfigArgs
from gcm.Dao.DaoSources import DaoSource
from gcm.inv.utils.date import DatePeriod
from pandas.tseries.offsets import BDay
from dateutil.relativedelta import relativedelta


class HkmaMarketPerformanceReport(ReportingRunnerBase):
    def __init__(self):
        super().__init__(runner=Scenario.get_attribute("runner"))
        self._as_of_date = Scenario.get_attribute("as_of_date")
        self._tickers = ['SPX Index',
                   'CCMP Index',
                   'RTY Index',
                   'SXXP Index',
                   'TPX Index',
                   'MXWO Index',
                   'MXAP INDEX',
                   'MSEUEGF Index',
                   'LBUSTRUU Index',
                   'USGG2YR Index',
                   'USGG10YR Index',
                   'USGG30YR Index',
                   'GUKG10 Index',
                   'GDBR10 Index',
                   'GFRN10 Index',
                   'USDJPY Curncy',
                   'EURUSD Curncy',
                   'VIX Index',
                   'GOLDS Comdty',
                   'CO1 Comdty',
                   'USCRWTIC  Index']
        self._level_tickers = ['USGG2YR Index',
                   'USGG10YR Index',
                   'USGG30YR Index',
                   'GUKG10 Index',
                   'GDBR10 Index',
                   'GFRN10 Index']
        self._total_return_tickers = {'SPX Index': 'SPXT Index',
                                      'CCMP Index': 'XCMP Index',
                                      'RTY Index': 'RU20INTR Index',
                                      'SXXP Index': 'SXXGR Index',
                                      'TPX Index': 'TPXDDVD Index'}

        self._day_start = self._previous_day(self._as_of_date)
        self._week_start = self._previous_week(self._as_of_date)
        self._month_start = self._previous_month(self._as_of_date)
        self._quarter_start = self._previous_quarter(self._as_of_date)
        self._year_start = self._previous_year(self._as_of_date)
        self._ttm_start = self._trailing_year(self._as_of_date)

    @staticmethod
    def _previous_day(ref):
        return ref - BDay(1)

    @staticmethod
    def _previous_week(ref):
        return ref - timedelta(days=ref.weekday()) - BDay(1)

    @staticmethod
    def _previous_month(ref):
        return dt.date(year=ref.year, month=ref.month, day=1) - timedelta(days=1)

    @staticmethod
    def _previous_quarter(ref):
        if ref.month < 4:
            return dt.date(ref.year - 1, 12, 31)
        elif ref.month < 7:
            return dt.date(ref.year, 3, 31)
        elif ref.month < 10:
            return dt.date(ref.year, 6, 30)
        return dt.date(ref.year, 9, 30)

    @staticmethod
    def _previous_year(ref):
        return dt.date(year=ref.year, month=1, day=1) - timedelta(days=1)

    @staticmethod
    def _trailing_year(ref):
        return ref - relativedelta(years=1) + BDay(1)

    def _get_current_level(self):
        factor = Factor(tickers=self._tickers)
        start_date = self._as_of_date
        end_date = self._as_of_date
        date_period = DatePeriod(start_date=start_date, end_date=end_date)
        data = factor.get_dimensions(date_period)
        data = data[['Ticker', 'PxLast']]
        data = data.set_index('Ticker').reindex(self._tickers)
        return data

    def _get_trailing_return(self, start_date):
        date_period_padded = DatePeriod(start_date=start_date - timedelta(days=5),
                                        end_date=self._as_of_date)
        total_return_tickers = pd.Series(self._tickers).replace(self._total_return_tickers).tolist()
        levels = Factor(tickers=total_return_tickers).get_dimensions(date_period_padded)
        levels = levels[['Date', 'Ticker', 'PxLast']].sort_values('Date')

        start_dates = levels.groupby('Ticker').apply(lambda x: x['Date'][x['Date'] <= start_date].max())

        levels = levels.merge(start_dates.to_frame('StartDate'), left_on='Ticker', right_index=True)

        end_level = levels[levels['Date'] == pd.to_datetime(self._as_of_date)]
        end_level = end_level[['Ticker', 'PxLast']].rename(columns={'PxLast': 'EndLevel'})

        start_level = levels[levels['Date'] == levels['StartDate']]
        start_level = start_level[['Ticker', 'PxLast']].rename(columns={'PxLast': 'StartLevel'})

        df = start_level.merge(end_level)
        df['IsLvl'] = df['Ticker'].isin(self._level_tickers)
        df['LvlChg'] = df['EndLevel'] - df['StartLevel']
        df['Return'] = np.where(df['IsLvl'], df['LvlChg'], df['LvlChg'] / df['StartLevel'])

        flipped_tr_tickers = {value: key for key, value in self._total_return_tickers.items()}
        df['Ticker'] = df['Ticker'].replace(flipped_tr_tickers)
        df = df[['Ticker', 'Return']].set_index('Ticker').reindex(self._tickers)

        return df

    def generate_report(self):
        result = self._get_current_level()

        periods = {'1Day': self._day_start,
                   'WTD': self._week_start,
                   'MTD': self._month_start,
                   'QTD': self._quarter_start,
                   'YTD': self._year_start,
                   'TTM': self._ttm_start}

        for period_name, start_date in periods.items():
            period_return = self._get_trailing_return(start_date=pd.to_datetime(start_date))
            period_return.rename(columns={"Return": period_name}, inplace=True)
            result = result.merge(period_return, left_index=True, right_index=True)

        as_of_date = dt.datetime.combine(self._as_of_date, dt.datetime.min.time())
        with Scenario(as_of_date=as_of_date).context():
            InvestmentsReportRunner().execute(
                data={'returns': result.reset_index(),
                      'as_of_date': pd.DataFrame({self._as_of_date})},
                template="HKMA_Market_Performance_Template.xlsx",
                save=True,
                runner=self._runner,
                entity_type=ReportingEntityTypes.cross_entity,
                entity_name='IFC',
                entity_display_name='IFC',
                entity_source=DaoSource.PubDwh,
                # entity_id=[158],
                report_name="HKMA Market Performance",
                report_type=ReportType.Market,
                report_frequency="Daily",
                report_vertical=ReportVertical.ARS,
                aggregate_intervals=AggregateInterval.Daily,
            )
        return 'Success'

    def run(self, **kwargs):
        return self.generate_report(**kwargs)


if __name__ == "__main__":
    runner = DaoRunner(
            container_lambda=lambda b, i: b.config.from_dict(i),
            config_params={
                DaoRunnerConfigArgs.dao_global_envs.name: {
                    DaoSource.InvestmentsDwh.name: {
                        "Environment": "prd",
                        "Subscription": "prd",
                    },
                }
            })

    with Scenario(runner=runner, as_of_date=dt.date(2022, 10, 28)).context():
        HkmaMarketPerformanceReport().execute()
