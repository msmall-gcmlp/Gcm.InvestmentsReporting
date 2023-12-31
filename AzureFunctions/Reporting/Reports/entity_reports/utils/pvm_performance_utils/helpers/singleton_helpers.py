from gcm.inv.entityhierarchy.EntityDomain.entity_domain import (
    pd,
)
from gcm.inv.scenario import Scenario, DaoRunner
from gcm.Dao.DaoSources import DaoSource
import numpy as np


def __runner() -> DaoRunner:
    return Scenario.get_attribute("dao")


def __as_of_date() -> DaoRunner:
    return Scenario.get_attribute("as_of_date")


def get_all_os_for_all_portfolios() -> pd.DataFrame:
    def my_dao_operation(dao, params):
        raw = """
        select distinct 
            PortfolioMasterId, 
            OperationalSeriesTicker, 
            PortfolioReportingName, 
            OperationalSeriesInvestmentType, 
            PortfolioTicker,
            PortfolioCurrency 
            from 
        analytics.MasterEntityDataPortfolioPortfolioSeriesOperationalSeries"""
        df = pd.read_sql(
            raw,
            dao.data_engine.session.bind,
        )
        return df

    portfolios = __runner().execute(
        params={},
        source=DaoSource.PvmDwh,
        operation=my_dao_operation,
    )
    return portfolios


def get_all_manager_holdings() -> pd.DataFrame:
    def my_dao_operation(dao, params):
        raw = """
                SELECT DISTINCT [Portfolio Ticker] PortfolioTicker, [Portfolio Reporting Name] PortfolioName, 
                [Portfolio Currency] PortfolioCurrency,
                [Operational Series Ticker] OperationalSeriesTicker, 
                [Operational Series Name] OsName, 
                [Operational Series Predominant Asset Class] OsAssetClass,
                [Holding Reporting Name] HoldingName, 
                [Holding Currency] HoldingCurrency,
                [Deal Name] DealName,
                [Deal Vintage Year] DealVintage, 
                [Investment Realization Type] Realizationtype,
                [Investment Manager Legal Name] InvestmentManagerName,
                [Investment Manager Master Id] InvestmentManagerId
                FROM [analytics].[MasterEntityDataInvestmentTrack]
                where [Investment Manager Legal Name] is not NULL
                order by [Portfolio Reporting Name], [Holding Reporting Name]"""
        df = pd.read_sql(
            raw,
            dao.data_engine.session.bind,
        )
        return df

    manager_df = __runner().execute(
        params={},
        source=DaoSource.PvmDwh,
        operation=my_dao_operation,
    )
    return manager_df


# all arguments are strings
def get_burgiss_bmark(
    report_date: str = None,
    vintage: str = "All",
    asset_group: str = "Buyout",
    geography_group: str = "All",
) -> pd.DataFrame:
    if report_date is None:
        report_date = str(Scenario.get_attribute("as_of_date"))

    def my_dao_operation(dao, params):
        # assumes max date is the same across groups strategy, vintage, or geography
        raw = f"""
        select * from burgiss.BenchmarkFact where
         AssetGroup='{asset_group}'
         and GeographyGroup='{geography_group}'
         and Vintage = '{vintage}'
         and Date =
            (
            select max(Date) from burgiss.BenchmarkFact where
            AssetGroup='{asset_group}'
            and GeographyGroup='{geography_group}'
            and Vintage = '{vintage}'
            and Date <= '{report_date}'
            )
        """

        burgiss_data = pd.read_sql(
            raw,
            dao.data_engine.session.bind,
        )
        return burgiss_data

    df = __runner().execute(
        params={},
        source=DaoSource.InvestmentsDwh,
        operation=my_dao_operation,
    )
    report_columns = [
        "PME - S&P 500 (TR)",
        "Direct Alpha - S&P 500 (TR)",
        "TVPI",
        "IRR",
        "DPI",
        "TWR - ITD",
        "CTR - ITD",
        "PME - S&P 500 (TR) - 5 Year",
        "Direct Alpha - S&P 500 (TR) - 5 Year",
        "TVPI - 5 Year",
        "IRR - 5 Year",
        "TWR - 5 Year",
        "CTR - 5 Year",
        "PME - S&P 500 (TR) - 3 Year",
        "Direct Alpha - S&P 500 (TR) - 3 Year",
        "TVPI - 3 Year",
        "IRR - 3 Year",
        "TWR - 3 Year",
        "CTR - 3 Year",
        "TWR - 1 Year",
        "CTR - 1 Year",
        "TWR - QTD",
        "CTR - QTD",
    ]
    df = df.set_index("Measure").T.reindex(
        [
            "Pooled",
            "TopFivePercentile",
            "TopQuartile",
            "Median",
            "BottomQuartile",
            "BottomFivePercentile",
        ]
    )

    for col in report_columns:
        if col not in list(df.columns):
            df[col] = None
    rslt = df[report_columns]
    # quick solution, should be done in dataprovider
    rslt[rslt.columns[rslt.columns.str.contains("IRR")]] = (
        rslt[rslt.columns[rslt.columns.str.contains("IRR")]] / 100
    )
    rslt[rslt.columns[rslt.columns.str.contains("Alpha")]] = (
        rslt[rslt.columns[rslt.columns.str.contains("Alpha")]] / 100
    )
    return rslt


def get_all_deal_attributes() -> pd.DataFrame:
    def my_dao_operation(dao, params):
        raw = """
        select distinct
        p.Ticker PortfolioTicker,
        os.Ticker OsTicker,
        h.ReportingName,
        im.LegalName InvestmentManagerName,
        hrpting.SmallAndEmerging,
        hrpting.DiverseManager,
        hrpting.VintageYear HoldingVintageYear,
        hrpting.PredominantSector,
        d.*
        from entitydata.OperationalSeries os
        left join entitydata.Investment i on os.MasterId = i.OperationalSeriesId
        left join entitydata.PortfolioSeries ps on os.PortfolioSeriesId=ps.MasterId
        left join entitydata.Portfolio p on ps.PortfolioId=p.MasterId
        left join entitydata.Deal d on i.DealId = d.MasterId
        left join entitydata.Holding h on i.HoldingId = h.MasterId
        left join entitydata.Holding hrpting on h.ReportingMasterId = hrpting.MasterId
        left join entitydata.InvestmentManager im on hrpting.InvestmentManagerId = im.MasterId
        where d.Name is not null
        """
        med_data = pd.read_sql(
            raw,
            dao.data_engine.session.bind,
        )

        return med_data

    df = __runner().execute(
        params={},
        source=DaoSource.PvmDwh,
        operation=my_dao_operation,
    )

    # TODO: move to perm location
    df.SmallAndEmerging = np.where(
        df.SmallAndEmerging.isnull(), 0, df.SmallAndEmerging
    )
    df.DiverseManager = np.where(
        df.DiverseManager.isnull(), 0, df.DiverseManager
    )
    df["SumEmergingDiverse"] = df.SmallAndEmerging + df.DiverseManager
    df["EmergingDiverseStatus"] = np.where(
        (df.SmallAndEmerging + df.DiverseManager) > 0,
        "SEM/DM",
        "Other",
    )

    # TODO: DT note: revisit
    df["VintageYear"] = df.VintageYear.astype(int).astype(str)
    df.rename(columns={"Name": "DealName"}, inplace=True)

    return df
