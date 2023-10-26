import pandas as pd
import numpy as np
import datetime as dt
import pandas_market_calendars as mcal
from gcm.Dao.DaoRunner import DaoRunnerConfigArgs, DaoRunner
from gcm.Dao.DaoSources import DaoSource
from gcm.inv.scenario import Scenario


def get_SPBetaDeltaNet(runner, date):
    date, date_yesterday, date_last_day_prior_month = _get_sql_dates(date)

    def _query_SPBetaDeltaNet(dao, params):
        raw = f"""
            SELECT
                a.date,
                SUM(a.betaadjexp / b.endingnav) AS value,
                LAG(SUM(a.betaadjexp / b.endingnav))
                    OVER (ORDER BY a.date DESC)
                    - SUM(a.betaadjexp / b.endingnav) AS ppt_change
            FROM
                reporting.securityexposurenolookthrough AS a
                LEFT JOIN analyticsdata.portfolio AS b
                ON a.date = b.date
            WHERE
                a.date IN ('{params['date1']}',
                           '{params['date2']}',
                           '{params['date3']}')
            GROUP BY
                a.date
            ORDER BY
                a.date DESC
            ;
        """
        df = pd.read_sql(
            raw,
            dao.data_engine.session.bind,
        )
        return df

    result = runner.execute(
        params={
            "date1": date,
            "date2": date_yesterday,
            "date3": date_last_day_prior_month,
        },
        source=DaoSource.InvestmentsDwh,
        operation=_query_SPBetaDeltaNet,
    )

    return result


def get_GrossExposure(runner, date):
    date, date_yesterday, date_last_day_prior_month = _get_sql_dates(date)

    def _query_GrossExposure(dao, params):
        raw = f"""
            WITH a AS (
                SELECT
                    date,
                    SecurityId,
                    sum(DeltaAdjExp) as DeltaAdjExp
                FROM
                    Reporting.SecurityExposureNoDisaggIndices
                WHERE
                    date IN ('{params['date1']}',
                             '{params['date2']}',
                             '{params['date3']}')
                GROUP BY Date, SecurityId
            ),
            b AS (
                SELECT
                    date,
                    endingnav
                FROM
                    analyticsdata.portfolio
                WHERE
                    date IN ('{params['date1']}',
                             '{params['date2']}',
                             '{params['date3']}')
            )
            SELECT
                a.date,
                SUM(ABS(DeltaAdjExp) / b.endingnav) AS value,
                LAG(SUM(ABS(DeltaAdjExp) / b.endingnav))
                    OVER (ORDER BY a.date DESC)
                    - SUM(ABS(DeltaAdjExp) / b.endingnav) AS ppt_change
            FROM
                a
                LEFT JOIN b
                ON a.date = b.date
            GROUP BY
                a.date
            ORDER BY
                a.date DESC
            ;
        """
        df = pd.read_sql(
            raw,
            dao.data_engine.session.bind,
        )
        return df

    result = runner.execute(
        params={
            "date1": date,
            "date2": date_yesterday,
            "date3": date_last_day_prior_month,
        },
        source=DaoSource.InvestmentsDwh,
        operation=_query_GrossExposure,
    )

    return result


def get_NetExposure(runner, date):
    date, date_yesterday, date_last_day_prior_month = _get_sql_dates(date)

    def _query_NetExposure(dao, params):
        raw = f"""
            WITH a AS (
                SELECT
                    date,
                    SecurityId,
                    sum(DeltaAdjExp) as DeltaAdjExp
                FROM
                    Reporting.SecurityExposureNoDisaggIndices
                WHERE
                    date IN ('{params['date1']}',
                             '{params['date2']}',
                             '{params['date3']}')
                GROUP BY Date, SecurityId
            ),
            b AS (
                SELECT
                    date,
                    endingnav
                FROM
                    analyticsdata.portfolio
                WHERE
                    date IN ('{params['date1']}',
                             '{params['date2']}',
                             '{params['date3']}')
            )
            SELECT
                a.date,
                SUM(DeltaAdjExp / b.endingnav) AS value,
                LAG(SUM(DeltaAdjExp / b.endingnav))
                    OVER (ORDER BY a.date DESC)
                    - SUM(DeltaAdjExp / b.endingnav) AS ppt_change
            FROM
                a
                LEFT JOIN b
                ON a.date = b.date
            GROUP BY
                a.date
            ORDER BY
                a.date DESC
            ;
        """
        df = pd.read_sql(
            raw,
            dao.data_engine.session.bind,
        )
        return df

    result = runner.execute(
        params={
            "date1": date,
            "date2": date_yesterday,
            "date3": date_last_day_prior_month,
        },
        source=DaoSource.InvestmentsDwh,
        operation=_query_NetExposure,
    )

    return result


def get_ExpFundModelVol(runner, date):
    date, date_yesterday, date_last_day_prior_month = _get_sql_dates(date)

    def _query_ExpFundModelVol(dao, params):
        raw = f"""
            SELECT
                HoldingDate AS date,
                SUM(PortRiskContrib) AS value,
                LAG(SUM(PortRiskContrib))
                    OVER (ORDER BY HoldingDate DESC)
                    - SUM(PortRiskContrib) AS ppt_change
            FROM
                AnalyticsData.HoldingFactorRiskContrib
            WHERE
                HoldingSourceIdentifier = 'EOF'
                AND HoldingDate IN ('{params['date1']}',
                                    '{params['date2']}',
                                    '{params['date3']}')
            GROUP BY
                HoldingDate
            ORDER BY
                HoldingDate DESC
            ;
        """
        df = pd.read_sql(
            raw,
            dao.data_engine.session.bind,
        )
        return df

    result = runner.execute(
        params={
            "date1": date,
            "date2": date_yesterday,
            "date3": date_last_day_prior_month,
        },
        source=DaoSource.InvestmentsDwh,
        operation=_query_ExpFundModelVol,
    )

    return result


def get_IdioRiskAlloc(runner, date):
    date, date_yesterday, date_last_day_prior_month = _get_sql_dates(date)

    def _query_IdioRiskAlloc(dao, params):
        raw = f"""
            SELECT
                HoldingDate as date,
                SUM(PctRiskContrib) AS value,
                LAG(SUM(PctRiskContrib))
                    OVER (ORDER BY HoldingDate DESC)
                    - SUM(PctRiskContrib) AS ppt_change
            FROM
                AnalyticsData.PortfolioContribRisk
            WHERE
                Factor IN (SELECT
                                ExternalDescription
                           FROM
                                Reporting.Factors
                           WHERE
                                FactorGroup1 = 'Selection Risk')
                AND HoldingDate IN ('{params['date1']}',
                                    '{params['date2']}',
                                    '{params['date3']}')
            GROUP BY
                HoldingDate
            ORDER BY
                HoldingDate DESC
            ;
        """
        df = pd.read_sql(
            raw,
            dao.data_engine.session.bind,
        )
        return df

    result = runner.execute(
        params={
            "date1": date,
            "date2": date_yesterday,
            "date3": date_last_day_prior_month,
        },
        source=DaoSource.InvestmentsDwh,
        operation=_query_IdioRiskAlloc,
    )

    return result


def get_eof_daily_summary_data(runner: DaoRunner, date: dt.date):
    """
    Pull all report data and format into output dataframe.
    """
    with Scenario(dao=runner, as_of_date=date).context():
        SPBetaDeltaNet = get_SPBetaDeltaNet(runner=runner, date=date)
        GrossExposure = get_GrossExposure(runner=runner, date=date)
        NetExposure = get_NetExposure(runner=runner, date=date)
        ExpFundModelVol = get_ExpFundModelVol(runner=runner, date=date)
        IdioRiskAlloc = get_IdioRiskAlloc(runner=runner, date=date)

        d = {
            "S&P betaDelta Net": format_eof_daily_summary(SPBetaDeltaNet),
            "Gross Exposure": format_eof_daily_summary(GrossExposure),
            "Net Exposure": format_eof_daily_summary(NetExposure),
            "Expected Fund Model Vol": format_eof_daily_summary(ExpFundModelVol),
            "Idio Risk Allocation": format_eof_daily_summary(IdioRiskAlloc),
        }

    return pd.DataFrame(d).T


def format_eof_daily_summary(df: pd.DataFrame) -> dict:
    """
    Pulling relevant data points for queries for report output.
    """
    d = {
        "Value": df.iloc[0, 1],
        "DoD Change": df.iloc[1, 2],
        "MTD Change": df.iloc[2, 2],
    }

    return d


def _get_sql_dates(date: dt.date):
    """
    Takes a dt.date and returns three dates relevant to this report:
        - date as str 'YYYY-MM-DD'
        - prior trading day relative to date as str 'YYYY-MM-DD'
        - last trading day of prior month relative to date as str 'YYYY-MM-DD'

    These three dates give us the ability to calculate the date's value,
    the DoD change, and the MTD change.
    """

    # Retrieve the NYSE holiday calendar.
    holidays = mcal.get_calendar("NYSE").holidays().holidays

    # Find the prior trading day relative to date.
    date_yesterday = np.busday_offset(
        date.strftime("%Y-%m-%d"),
        -1,
        roll="forward",
        weekmask="1111100",
        holidays=holidays,
    )

    # Get the first day of the month relative to date.
    date_first_day_this_month = date.replace(day=1)
    # Find the prior trading date relative to the first day of current month.
    # This gives us the last trading day of the prior month.
    date_last_day_prior_month = np.busday_offset(
        date_first_day_this_month.strftime("%Y-%m-%d"),
        -1,
        roll="forward",
        weekmask="1111100",
        holidays=holidays,
    )

    # Convert back to strings.
    date = pd.to_datetime(date).strftime("%Y-%m-%d")
    date_yesterday = pd.to_datetime(date_yesterday).strftime("%Y-%m-%d")
    date_last_day_prior_month = pd.to_datetime(date_last_day_prior_month).strftime(
        "%Y-%m-%d"
    )

    return date, date_yesterday, date_last_day_prior_month


if __name__ == "__main__":
    dao_runner = DaoRunner(
        container_lambda=lambda b, i: b.config.from_dict(i),
        config_params={
            DaoRunnerConfigArgs.dao_global_envs.name: {
                DaoSource.InvestmentsDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
            }
        },
    )

    # Report is run for yesterday's date.
    date = dt.datetime.today() - dt.timedelta(days=1)
    # Override date
    # date = dt.datetime(2023, 10, 23)

    report = get_eof_daily_summary_data(runner=dao_runner, date=date)

    print(f"Report for {date.strftime('%Y-%m-%d')}:")
    print(report)
