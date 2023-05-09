from gcm.inv.entityhierarchy.EntityDomain.entity_domain import (
    pd,
)
from gcm.inv.scenario import Scenario, DaoRunner
from gcm.Dao.DaoSources import DaoSource


def __runner() -> DaoRunner:
    return Scenario.get_attribute("dao")


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


def get_all_deal_attributes() -> pd.DataFrame:
    def my_dao_operation(dao, params):
        raw = """
                select distinct
                    os.Ticker OsTicker,
                    h.ReportingName,
                    hrpting.PredominantSector,
                    d.*  
                from entitydata.OperationalSeries os
                    left join entitydata.Investment i on os.MasterId = i.OperationalSeriesId
                    left join entitydata.Deal d on i.DealId = d.MasterId
                    left join entitydata.Holding h on i.HoldingId = h.MasterId
                    left join entitydata.Holding hrpting on h.ReportingMasterId = hrpting.MasterId
                """
        df = pd.read_sql(
            raw,
            dao.data_engine.session.bind,
        )
        return df

    attrib = __runner().execute(
        params={},
        source=DaoSource.PvmDwh,
        operation=my_dao_operation,
    )
    return attrib
