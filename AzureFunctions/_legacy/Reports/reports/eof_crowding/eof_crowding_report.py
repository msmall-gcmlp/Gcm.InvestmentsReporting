import pandas as pd
import datetime as dt
from gcm.Dao.DaoRunner import DaoRunnerConfigArgs, DaoRunner
from gcm.Dao.DaoSources import DaoSource
from gcm.inv.quantlib.timeseries.analytics import Analytics
from gcm.inv.quantlib.enum_source import Periodicity
from gcm.inv.scenario import Scenario


def get_eof_pm_returns(runner, as_of_date):
    def _query_eof_pm_returns(dao, params):
        raw = '''
        SELECT Date , Name, GrossRor
        FROM AnalyticsData.HoldingReturns hr
        LEFT JOIN AnalyticsData.HoldingDimn hd
        ON hr.HoldingId = hd.Id
        WHERE Frequency = 'D'
        AND NavType = 'ENAV'
        AND IsOverlay = 0
        AND Name != 'Rhino'
        AND HoldingId in (
            SELECT DISTINCT HoldingId
            FROM AnalyticsData.HoldingReturns
            WHERE Date = (SELECT MAX(Date) FROM AnalyticsData.PortfolioRor)
        )
        ORDER BY Date
        '''
        df = pd.read_sql(
            raw,
            dao.data_engine.session.bind,
        )
        return df

    result = runner.execute(
        params={},
        source=DaoSource.InvestmentsDwh,
        operation=_query_eof_pm_returns,
    )

    result = result[result["Date"] <= as_of_date]
    result = result.pivot(index='Date', columns='Name')
    result.columns = result.columns.droplevel(0)
    result.index = pd.to_datetime(result.index)
    return result


def get_eof_returns(runner, as_of_date):
    def _query_eof_returns(dao, params):
        raw = '''
        SELECT Date, Ror
        FROM AnalyticsData.PortfolioRor
        WHERE PortfolioId = 99999 AND RorType = 'Gross'
        ORDER by DATE
        '''
        df = pd.read_sql(
            raw,
            dao.data_engine.session.bind,
        )
        return df

    result = runner.execute(
        params={},
        source=DaoSource.InvestmentsDwh,
        operation=_query_eof_returns,
    )

    result = result[result["Date"] <= as_of_date]
    result = result.set_index("Date")
    result.columns = ['EOF']
    result.index = pd.to_datetime(result.index)
    return result


def get_hf_factor_returns(runner, as_of_date):
    def _query_factor_returns(dao, params):
        raw = '''
        SELECT Date, GcmTicker, Ror
        FROM AnalyticsData.FactorReturns fr
        LEFT JOIN factors.FactorInventory fi
        ON fr.Ticker = fi.SourceTicker
        WHERE Ticker in ('MSCBSSHU Index', 'MSXXH13F_MXWDIM_BETA_ADJ')
        AND Ror is not NULL
        ORDER BY Date
        '''
        df = pd.read_sql(
            raw,
            dao.data_engine.session.bind,
        )
        return df

    result = runner.execute(
        params={},
        source=DaoSource.InvestmentsDwh,
        operation=_query_factor_returns,
    )

    result = result[result["Date"] <= as_of_date]
    result = result.pivot(index='Date', columns='GcmTicker')
    result.columns = result.columns.droplevel(0)
    result.index = pd.to_datetime(result.index)
    return result


def _calculate_rolling_betas(eof_returns, pm_returns, hf_factor_returns, factor):
    eof = eof_returns.merge(hf_factor_returns, left_index=True, right_index=True, how='left')
    eof_betas = Analytics().compute_trailing_beta(ror=eof[['EOF']],
                                                  benchmark_ror=eof[[factor]],
                                                  window=100,
                                                  as_of_date=eof.index.max(),
                                                  periodicity=Periodicity.Daily,
                                                  include_history=True)

    pms = pm_returns.merge(hf_factor_returns, left_index=True, right_index=True, how='left')
    pm_betas = Analytics().compute_trailing_beta(ror=pms[pm_returns.columns],
                                                 benchmark_ror=pms[[factor]],
                                                 window=100,
                                                 as_of_date=eof.index.max(),
                                                 periodicity=Periodicity.Daily,
                                                 include_history=True)

    betas = pd.concat([eof_betas, pm_betas], axis=1)
    return betas


def _format_beta_summary(betas):
    summary = betas.describe().loc[['min', '25%', '50%', '75%', 'max']].T
    current_betas = betas.tail(1).T
    current_betas.columns = ['current beta']
    current_z = ((betas.tail(1) - betas.mean()) / (betas.std())).T
    current_z.columns = ['current z']
    summary = pd.concat([current_betas, current_z, summary], axis=1)
    summary = summary.round(2)
    summary = summary.reset_index()
    return summary


def generate_beta_summary(runner: DaoRunner, date: dt.date, long_or_short):
    pm_returns = get_eof_pm_returns(runner=runner, as_of_date=date)
    eof_returns = get_eof_returns(runner=runner, as_of_date=date)
    hf_factor_returns = get_hf_factor_returns(runner=runner, as_of_date=date)

    if long_or_short == 'long':
        factor = 'MSXXH13F_EX_ACWI_BETA_ADJ'
    elif long_or_short == 'short':
        factor = 'US_HIGH_SHORT_INTEREST_MS_BASKET'

    all_betas = _calculate_rolling_betas(eof_returns=eof_returns,
                                         pm_returns=pm_returns,
                                         hf_factor_returns=hf_factor_returns,
                                         factor=factor)

    beta_summary = _format_beta_summary(betas=all_betas)
    return beta_summary


def generate_eof_crowding_report_data(runner: DaoRunner, date: dt.date):
    with Scenario(dao=runner, as_of_date=date).context():
        crowded_longs = generate_beta_summary(runner=runner,
                                              date=date,
                                              long_or_short='long')

        crowded_shorts = generate_beta_summary(runner=runner,
                                               date=date,
                                               long_or_short='short')
    return crowded_longs, crowded_shorts


if __name__ == "__main__":
    dao_runner = DaoRunner(
        container_lambda=lambda b, i: b.config.from_dict(i),
        config_params={
            DaoRunnerConfigArgs.dao_global_envs.name: {
                DaoSource.InvestmentsDwh.name: {
                    "Environment": "dev",
                    "Subscription": "nonprd",
                },
            }
        },
    )
    date = dt.date(2022, 12, 31)
    # TODO move this to InvestmentsModel
    longs, shorts = generate_eof_crowding_report_data(runner=dao_runner, date=date)
