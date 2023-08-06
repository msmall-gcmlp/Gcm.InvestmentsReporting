import datetime as dt
import os

import pandas as pd
from gcm.inv.models.portfolio_construction.optimization.config.constraint import Constraints
from gcm.inv.models.portfolio_construction.optimization.config.objective import Objectives
from gcm.inv.models.portfolio_construction.optimization.config.portfolio_attribute import PortfolioAttributes
from gcm.inv.models.portfolio_construction.optimization.get_post_optim_inputs import download_optimization_inputs
from gcm.inv.models.portfolio_construction.optimization.optimization_inputs import (
    LongTermInputs,
)
from gcm.data import DataAccess, DataSource
from gcm.data.sql._sql_odbc_client import SqlOdbcClient
from gcm.data.storage import StorageQueryParams, DataLakeZone
from attr import define
from gcm.inv.models.portfolio_construction.portfolio_metrics.portfolio_metrics import _process_optim_fund_inputs, \
    ProcessedFundInputs


@define
class PortfolioInputs:
    acronym: str
    scenario: str
    as_of_date: dt.date


@define
class AdhocFundAttributes:
    multi_strat_lookthrough: pd.DataFrame
    stresses: pd.DataFrame
    fund_attributes: pd.DataFrame


@define
class RawData:
    weights: pd.DataFrame
    optim_inputs: LongTermInputs
    adhoc_portfolio_attributes: pd.DataFrame
    adhoc_fund_attributes: AdhocFundAttributes


def _get_sql_client(sub, env):
    return DataAccess().get(
        DataSource.Sql,
        target_name=f"gcm-elasticpoolinvestments-{sub}",
        database_name=f"investmentsdwh-{env}"
    )


def _get_dl_client(sub):
    return DataAccess().get(
        DataSource.DataLake,
        target_name=f"gcmdatalake{sub}",
    )


def _get_clients(sub, env):
    sql_client = _get_sql_client(sub=sub, env=env)
    dl_client = _get_dl_client(sub=sub)
    return sql_client, dl_client


def _query_optimal_weights(
    portfolio_acronym: str,
    scenario_name: str,
    as_of_date: dt.date,
    client: SqlOdbcClient,
) -> pd.DataFrame:
    query = (
        f"""
        WITH WeightsTbl
        AS (
        SELECT
            igm.EntityName as InvestmentGroupName,
            igm.EntityId as InvestmentGroupId,
            ow.LongTermWeight AS LongTermOptimal,
            ow.ShortTermWeight as ShortTermOptimal
        FROM PortfolioConstruction.OptimalWeights ow
        LEFT JOIN entitymaster.InvestmentGroupMaster igm
        ON ow.InvestmentGroupId = igm.EntityId
        INNER JOIN PortfolioConstruction.InputLocations il
        ON ow.InputLocationId = il.Id
        WHERE PortfolioAcronym='{portfolio_acronym}'
        AND ScenarioName='{scenario_name}'
        AND AsOfDate='{as_of_date.isoformat()}'
        )
        SELECT * FROM WeightsTbl
        """
    )
    return client.read_raw_sql(query)


def _query_portfolio_attributes(
    portfolio_acronym: str,
    client: SqlOdbcClient,
) -> pd.DataFrame:
    query = (
        f"""
        WITH AttributesTbl
        AS (
        SELECT
            Field, Value, FieldGroup, FieldDescription, ValueType
        FROM PortfolioConstruction.PortfolioAttributesLong
        WHERE PortfolioAcronym='{portfolio_acronym}'
        )
        SELECT * FROM AttributesTbl
        """
    )
    result = client.read_raw_sql(query)
    is_bit = result['ValueType'] == 'bit'
    result.loc[is_bit, 'Value'] = result[is_bit]['Value'].astype(int).astype(bool).astype(str)
    result.loc[result['Value'] == 'True', 'Value'] = "Yes"
    result.loc[result['Value'] == 'False', 'Value'] = "No"

    is_float = result['ValueType'] == 'float'
    result.loc[is_float, 'Value'] = result[is_float]['Value'].astype(float)

    return result


def _query_fund_attributes(sql_client: SqlOdbcClient) -> pd.DataFrame:
    query = (
        """
        WITH AttributesTbl
        AS (
        SELECT distinct
        b.InvestmentGroupName,
        d.BenchmarkStrategy,
        d.BenchmarkSubStrategy,
        ReturnPtileVsPeer as ExcessRtnPeerPtile,
        AccuracyScore as InputsAccuracy,
        PctIdio as PctActiveRisk,
        c.AvailableCapacity
        FROM RiskModel.FundExpectations a
        LEFT JOIN entitymaster.vInvestments b
        ON a.InvestmentGroupId = b.InvestmentGroupId
        LEFT JOIN (
            SELECT InvestmentGroupName, [Value] as 'AvailableCapacity'
            FROM PortfolioConstruction.InvestmentGroupAttributesLong
            WHERE Field = 'ExpectedCapacity'
        ) c
        ON b.InvestmentGroupName = c.InvestmentGroupName
        LEFT JOIN gcminputs.InvestmentReportingPeerBenchmark d
        ON a.InvestmentGroupId = d.InvestmentGroupId
        )
        SELECT * FROM AttributesTbl
        """
    )
    result = sql_client.read_raw_sql(query)
    result = result.set_index('InvestmentGroupName')
    substrategy_alias = {'Convertible Arbitrage': 'RV - Convert Arb',
                         'Global Macro': 'Macro - Global',
                         'Fixed Income Arbitrage': 'RV - FI Arb',
                         'Opportunistic': 'Multi-Strat - Opportunistic',
                         'Emerging Market Macro': 'Macro - EM',
                         'Diversified Multi-Strategy': 'Multi-Strat - Diversified',
                         'CTA/Managed Futures': 'Quant - CTA/Managed Futures',
                         'Quantitative': 'Quant - Non-Directional'}
    result['BenchmarkSubStrategy'] = result['BenchmarkSubStrategy'].replace(substrategy_alias)

    return result


# TODO move this to idwh
def _download_multi_strat_lookthrough_allocations(dl_client):

    prefix = "investmentsmodels/inputs/portfolio_construction/"
    query = StorageQueryParams(
        file_system=DataLakeZone.raw,
        path=f"{prefix}multi_strat_lookthrough.csv"
    )
    from io import StringIO
    df = pd.read_csv(StringIO(dl_client.get_blob(query).content.decode()))
    return df


# TODO get these from optimInputs and/or idwh
def _download_standalone_stresses(dl_client):
    prefix = "investmentsmodels/inputs/portfolio_construction/"
    query = StorageQueryParams(
        file_system=DataLakeZone.raw,
        path=f"{prefix}manager_standalone_stresses.csv"
    )
    from io import StringIO
    df = pd.read_csv(StringIO(dl_client.get_blob(query).content.decode()))
    return df


def _get_inv_group_id_map(inputs):
    inv_group_id_map = inputs.fundInputs.fundData.investment_group_ids
    inv_group_id_map.InvestmentGroupId = inv_group_id_map.InvestmentGroupId.astype(int)
    return inv_group_id_map


def _get_optimal_weights(
        portfolio_acronym: str,
        scenario_name: str,
        as_of_date: dt.date,
        client: SqlOdbcClient,
        inv_group_id_map: pd.DataFrame,
) -> pd.DataFrame:
    wts = _query_optimal_weights(
        portfolio_acronym=portfolio_acronym,
        scenario_name=scenario_name,
        as_of_date=as_of_date,
        client=client,
    )
    wts["InvestmentGroupName"] = wts[["InvestmentGroupId"]].merge(inv_group_id_map)["Fund"]
    return wts


def _get_current_weights(inputs: LongTermInputs):
    current_balances = inputs.config.portfolioAttributes.currentBalances
    current_weights = pd.DataFrame.from_dict(current_balances, orient="index", columns=["Current"]).reset_index(
        names="Fund"
    )
    return current_weights


def _combine_current_and_optimal_weights(current, optimal, id_map):
    weights = current.merge(id_map, on="Fund", how="inner").merge(
        optimal, on="InvestmentGroupId", how="outer"
    )

    weights.InvestmentGroupName = weights.InvestmentGroupName.combine_first(weights.Fund)
    weights = weights[
        ["InvestmentGroupName", "InvestmentGroupId", "Current", "ShortTermOptimal", "LongTermOptimal"]]
    weights = weights.fillna(0)
    weights = weights[weights.iloc[:, 2:].max(axis=1) > 0]
    return weights


def _get_allocations(
        portfolio: PortfolioInputs,
        sql_client: SqlOdbcClient,
        lt_inputs: LongTermInputs,
) -> pd.DataFrame:
    inv_group_id_map = _get_inv_group_id_map(lt_inputs)
    optimal_weights = _get_optimal_weights(
        portfolio_acronym=portfolio.acronym,
        scenario_name=portfolio.scenario,
        as_of_date=portfolio.as_of_date,
        client=sql_client,
        inv_group_id_map=inv_group_id_map,
    )

    current_weights = _get_current_weights(inputs=lt_inputs)

    weights = _combine_current_and_optimal_weights(current=current_weights,
                                                   optimal=optimal_weights,
                                                   id_map=inv_group_id_map)

    weights = weights.rename(columns={'InvestmentGroupName': 'Fund'}).set_index('Fund')
    weights.drop(columns={"InvestmentGroupId"}, inplace=True)
    return weights


def _get_adhoc_fund_attributes(dl_client, sql_client):
    return AdhocFundAttributes(multi_strat_lookthrough=_download_multi_strat_lookthrough_allocations(dl_client),
                               stresses=_download_standalone_stresses(dl_client=dl_client),
                               fund_attributes=_query_fund_attributes(sql_client=sql_client))


def _get_raw_report_data(portfolio: PortfolioInputs, sub, env):
    sql_client, dl_client = _get_clients(sub=sub, env=env)

    lt_optim_inputs = download_optimization_inputs(
        portfolio_acronym=portfolio.acronym,
        scenario_name=portfolio.scenario,
        as_of_date=portfolio.as_of_date,
        client=dl_client,
        is_short_term=False,
    )

    weights = _get_allocations(portfolio=portfolio,
                               sql_client=sql_client,
                               lt_inputs=lt_optim_inputs)

    adhoc_portfolio_attributes = _query_portfolio_attributes(
        portfolio_acronym=portfolio.acronym,
        client=sql_client
    )

    adhoc_fund_attributes = _get_adhoc_fund_attributes(dl_client=dl_client, sql_client=sql_client)

    report_data = RawData(weights=weights,
                          optim_inputs=lt_optim_inputs,
                          adhoc_portfolio_attributes=adhoc_portfolio_attributes,
                          adhoc_fund_attributes=adhoc_fund_attributes)
    return report_data


@define
class PortfolioData:
    optim_attributes: PortfolioAttributes
    objectives: Objectives
    constraints: Constraints
    eligible_roster: list[str]
    adhoc_attributes: pd.DataFrame


@define
class FundData:
    allocated_optim_inputs: ProcessedFundInputs
    eligible_optim_inputs: ProcessedFundInputs
    multi_strat_lookthrough: pd.DataFrame
    adhoc_stresses: pd.DataFrame
    adhoc_attributes: pd.DataFrame


@define
class ReportData:
    portfolio: PortfolioInputs
    allocations: pd.DataFrame
    portfolio_data: PortfolioData
    fund_data: FundData
    rf: float


def get_report_data(portfolio_acronym, scenario_name, as_of_date):
    env = os.environ.get("Environment", "dev").replace("local", "dev")
    sub = os.environ.get("Subscription", "nonprd").replace("local", "nonprd")
    env = 'prd'
    sub = 'prd'

    portfolio = PortfolioInputs(acronym=portfolio_acronym, scenario=scenario_name, as_of_date=as_of_date)
    raw_data = _get_raw_report_data(portfolio=portfolio, sub=sub, env=env)
    return ReportData(
        portfolio=portfolio,
        allocations=raw_data.weights,
        portfolio_data=PortfolioData(
            optim_attributes=raw_data.optim_inputs.config.portfolioAttributes,
            objectives=raw_data.optim_inputs.config.objectives,
            constraints=raw_data.optim_inputs.config.constraints,
            eligible_roster=raw_data.optim_inputs.config.fundSubset,
            adhoc_attributes=raw_data.adhoc_portfolio_attributes
        ),
        fund_data=FundData(
            allocated_optim_inputs=_process_optim_fund_inputs(inputs=raw_data.optim_inputs,
                                                              fund_order=raw_data.weights.index),
            eligible_optim_inputs=_process_optim_fund_inputs(inputs=raw_data.optim_inputs,
                                                             fund_order=raw_data.optim_inputs.config.fundSubset),
            multi_strat_lookthrough=raw_data.adhoc_fund_attributes.multi_strat_lookthrough,
            adhoc_stresses=raw_data.adhoc_fund_attributes.stresses,
            adhoc_attributes=raw_data.adhoc_fund_attributes.fund_attributes
        ),
        rf=raw_data.optim_inputs.config.expRf
    )
