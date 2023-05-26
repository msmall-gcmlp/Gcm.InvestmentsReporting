import datetime as dt
import os
import pandas as pd
from gcm.Dao.DaoSources import DaoSource
from gcm.inv.models.portfolio_construction.optimization.optimization_inputs import (
    OptimizationInputs,
    download_optimization_inputs,
)
from _legacy.core.Runners.investmentsreporting import InvestmentsReportRunner
from _legacy.core.reporting_runner_base import (
    ReportingRunnerBase,
)
from _legacy.core.ReportStructure.report_structure import (
    ReportingEntityTypes,
    ReportType,
    AggregateInterval,
    ReportVertical,
)
from gcm.Dao.DaoRunner import DaoRunner, DaoRunnerConfigArgs
from gcm.inv.scenario import Scenario
from gcm.data import DataAccess, DataSource
from gcm.data.sql._sql_odbc_client import SqlOdbcClient
from gcm.inv.models.portfolio_construction.portfolio_metrics.portfolio_metrics import collect_portfolio_metrics


def get_optimal_weights(
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
            ow.Weight AS Optimized
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


def get_portfolio_attributes(
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


class PortfolioConstructionReport(ReportingRunnerBase):
    def __init__(self):
        super().__init__(runner=Scenario.get_attribute("dao"))
        self._as_of_date = Scenario.get_attribute("as_of_date")

    @staticmethod
    def _format_weights(allocations, strategies, capacity_status):
        def _create_strategy_block(weights, strategy):
            wts_g = weights[weights['Strategy'] == strategy]
            wts_g.drop(columns={'Strategy'}, inplace=True)
            strategy_g = wts_g.sum().to_frame().T
            strategy_g['Fund'] = strategy
            spacer_row = strategy_g.copy()
            spacer_row[0:] = None
            return pd.concat([strategy_g, wts_g, spacer_row], axis=0)

        wts = allocations * 100
        wts = wts.reset_index()
        wts_formatted = pd.DataFrame(columns=wts.columns.tolist())

        wts = wts.merge(strategies)

        groups = sorted(wts['Strategy'].unique())

        for g in groups:
            wts_formatted = pd.concat([wts_formatted, _create_strategy_block(weights=wts, strategy=g)], axis=0)

        wts.drop(columns={'Strategy'}, inplace=True)

        unallocated_row = wts.sum()
        unallocated_row['Fund'] = 'Cash & Other'
        unallocated_row[1:] = (100 - unallocated_row[1:])
        unallocated_row = unallocated_row.to_frame().T

        total_row = unallocated_row.copy()
        total_row['Fund'] = 'Total'
        total_row.iloc[:, 1:] = 100

        wts_formatted = pd.concat([wts_formatted, unallocated_row, total_row], axis=0)

        wts_formatted = wts_formatted.merge(capacity_status, how='left', on='Fund')
        is_cc = wts_formatted['Capacity'] < 1
        wts_formatted.loc[is_cc, 'Fund'] = wts_formatted.loc[is_cc, 'Fund'] + ' *'
        is_closed = wts_formatted['Capacity'] == 0
        wts_formatted.loc[is_closed, 'Fund'] = wts_formatted.loc[is_closed, 'Fund'] + '*'
        wts_formatted.drop(columns={"Capacity"}, inplace=True)

        # leave current to 2 decimals
        wts_formatted.iloc[:, 2:] = wts_formatted.iloc[:, 2:].astype(float).round(0)
        wts_formatted['Delta'] = wts_formatted['Optimized'] - wts_formatted['Planned']
        wts_formatted['Delta - %Risk'] = wts_formatted['Optimized - %Risk'] - wts_formatted['Planned - %Risk']
        column_order = ['Current', 'Planned', 'Optimized', 'Delta']
        column_order = ['Fund'] + column_order + [x + ' - %Risk' for x in column_order]
        wts_formatted = wts_formatted[column_order]

        return wts_formatted

    def _format_allocations_summary(self, weights, metrics):
        weights = weights.sort_index()
        strategies = metrics['fund_strategies']
        strategies['Strategy'] = strategies['Current'].combine_first(strategies['Optimized'])
        strategies['Strategy'] = strategies.Strategy.combine_first(strategies.Planned)
        strategies = strategies.reset_index().rename(columns={'index': 'Fund'})[['Fund', 'Strategy']]

        capacity = metrics['fund_capacities']
        capacity['Capacity'] = capacity['Current'].combine_first(capacity['Optimized'])
        capacity['Capacity'] = capacity['Capacity'].combine_first(capacity['Planned'])
        capacity = capacity.reset_index().rename(columns={'index': 'Fund'})[['Fund', 'Capacity']]

        metrics['risk_allocations'].columns = metrics['risk_allocations'].columns + ' - %Risk'
        combined_weights = weights.merge(metrics['risk_allocations'], how='outer', left_index=True, right_index=True)
        combined_weights = combined_weights.fillna(0)
        formatted_weights = self._format_weights(allocations=combined_weights,
                                                 strategies=strategies,
                                                 capacity_status=capacity)
        dollar_weights = formatted_weights[['Fund',
                                            'Current',
                                            'Planned',
                                            'Optimized',
                                            'Delta']]
        risk_weights = formatted_weights[['Fund'] + [x + ' - %Risk' for x in dollar_weights.columns[1:]]]

        weights = {"dollar_allocation_summary": dollar_weights, "risk_allocation_summary": risk_weights}

        return weights

    @staticmethod
    def _combine_metrics_across_weights(weights, optim_inputs, rf):
        portfolio_metrics = {k: collect_portfolio_metrics(
            weights=weights[k],
            optim_inputs=optim_inputs,
            rf=rf) for k in weights.columns
        }

        metrics = {key: None for key in [f.name for f in portfolio_metrics['Current'].metrics.__attrs_attrs__]}
        for m in metrics.keys():
            metrics[m] = pd.concat([getattr(portfolio_metrics['Current'].metrics, m),
                                    getattr(portfolio_metrics['Planned'].metrics, m),
                                    getattr(portfolio_metrics['Optimized'].metrics, m)], axis=1)
            metrics[m].columns = weights.columns

        strategy_order = pd.DataFrame(index=['Credit', 'Long/Short Equity', 'Macro', 'Multi-Strategy', 'Quantitative'])
        strategy_weights = strategy_order.merge(metrics['strategy_weights'],
                                                left_index=True, right_index=True,
                                                how='left')
        cash_and_other = pd.DataFrame(1 - strategy_weights.sum()).T
        cash_and_other.index = ['Cash and Other']
        metrics['strategy_weights'] = pd.concat([strategy_weights, cash_and_other], axis=0)

        metrics["distribution_of_returns"] = metrics.pop('outcomes_distribution')
        metrics["exp_risk_adj_performance"] = metrics.pop('risk_adj_performance')
        metrics["strategy_allocation"] = metrics.pop('strategy_weights')

        return metrics

    def _format_header_info(self, acronym, scenario_name):
        header_info = pd.DataFrame(
            {
                "header_info": [
                    acronym,
                    scenario_name,
                    self._as_of_date,
                ]
            }
        )
        header_info = {"header_info": header_info}
        return header_info

    @staticmethod
    def _summarize_liquidity_cons(obs_cons):
        liq_class = obs_cons.liquidityConstraints
        liq_freq = list(obs_cons.liquidityConstraints.__annotations__.keys())
        liquidity = {x: liq_class.__getattribute__(x) for x in liq_freq if liq_class.__getattribute__(x) is not None}
        liq_display_names = {'daily': 'D',
                             'monthly': 'M',
                             'quarterly': 'Q',
                             'semiannual': 'SA',
                             'annual': 'A',
                             'eighteenMonths': '18M',
                             'twoYears': '2Y',
                             'threeYears': '3Y',
                             'greaterThanThreeYears': '4Y'}
        liquidity = {liq_display_names[k]: liquidity[k] for k in liquidity}
        liquidity = ' | '.join("{:.0f}".format(100 * y) + '% ' + str(x) for x, y in liquidity.items())
        return liquidity

    @staticmethod
    def _summarize_stress_limits(obs_cons):
        scenarios = obs_cons.stress_scenarios
        n_scenarios = range(len(scenarios))
        beta = [scenarios[x].limit for x in n_scenarios if scenarios[x].name == 'LongRunSelloff']
        max_mkt_exposure = beta[0] / -0.15

        credit_lim = [scenarios[x].limit for x in n_scenarios if scenarios[x].name == 'CreditLiquidityTechnicals']
        credit_lim = credit_lim[0]
        return max_mkt_exposure, credit_lim

    @staticmethod
    def _summarize_position_limits(obs_cons):
        stress_limits = pd.DataFrame(
            {
                "stress_limits": [
                    0.03,
                    obs_cons.maxLeverage if obs_cons.maxLeverage is not None else "Unconstrained",
                    obs_cons.minMaterialNonZero
                ]
            },
            index=['SingleName', 'MaxGrossExposure', 'MinMaterialAllocation']
        )
        return stress_limits

    def _format_config_attributes(self, weights, optim_inputs, rf):
        metrics = collect_portfolio_metrics(weights=weights['Current'],
                                            optim_inputs=optim_inputs,
                                            rf=rf)
        obs_cons = metrics.obs_cons
        target_excess = 10_000 * (obs_cons.target_return - rf)
        target_excess = "{:.0f}".format(target_excess) + 'bps over risk-free'
        liquidity = self._summarize_liquidity_cons(obs_cons)
        max_mkt_exposure, credit_lim = self._summarize_stress_limits(obs_cons)

        obs_cons_summary = dict({
            "TargetReturn": pd.DataFrame({obs_cons.target_return}),
            "TargetExcessReturn": pd.DataFrame({target_excess}),
            "MaxMktExposure": pd.DataFrame({max_mkt_exposure}),
            "LiquidityConstraint_Informal": pd.DataFrame({liquidity}),
            "MaxFundCount": pd.DataFrame({obs_cons.maxFundCount}),
            "MinAcceptableAllocation": pd.DataFrame({obs_cons.minMaterialNonZero}),
            "CreditLiquidityTechnicalsLimit": pd.DataFrame({credit_lim}),
            "ReallocateScarceCapacity": pd.DataFrame({"No"}),
            "BlankSlateOrRebalance": pd.DataFrame({"Blank Slate"})
        })

        return obs_cons_summary

    @staticmethod
    def _format_non_config_attributes(reference_attributes):
        reference_attributes = dict(zip(reference_attributes['Field'], reference_attributes['Value']))

        # format fees for display
        fee_items = ['MgmtFee', 'IncentiveFee', 'HurdleRate']
        fees = {key: str(round(100 * reference_attributes[key], 2)) + '%' for key in fee_items}
        reference_attributes['Fees'] = fees['MgmtFee'] + ' | ' + fees['IncentiveFee'] + ' | ' + fees['HurdleRate']

        # format beta for display
        beta = reference_attributes.get('MaxBeta')
        if beta is None:
            reference_attributes["FormalBetaConstraint"] = "--"
        else:
            beta = "{:.2f}".format(reference_attributes['MaxBeta'])
            reference_attributes["FormalBetaConstraint"] = beta + ' vs ' + reference_attributes['BetaBenchmark']

        formal_attributes = ['ClientName',
                             'StrategyFocus',
                             'FirstPrincipal',
                             'FirstAnalyst',
                             'Fees',
                             'MaxLineOfCreditAmount',
                             'ReturnObjective',
                             'VolatilityConstraint',
                             'FormalBetaConstraint',
                             'LiquidityConstraint',
                             'MaxSLFAllocation_Formal',
                             'MaxAllocationSingleFund_Formal',
                             'MaxAllocationSingleManager',
                             'MaxFundCount_Formal',
                             'MaxLeverageVolUnadj',
                             'MaxSclPositionImpact',
                             'MaxDesignatedAmount',
                             'MaxSLFAllocation',
                             'MaxAllocationSingleFund',
                             'MaxDrawdownConstraint',
                             'DrawdownFundsEligible']
        formal_attribute_subset = {key: pd.DataFrame({reference_attributes.get(key)}) for key in formal_attributes}

        for k, v in formal_attribute_subset.items():
            if v.squeeze() is None:
                formal_attribute_subset[k] = pd.DataFrame({"--"})
        return formal_attribute_subset

    def generate_excel_report(
        self,
        acronym: str,
        scenario_name: str,
        weights: pd.DataFrame,
        optim_inputs: OptimizationInputs,
        reference_attributes: pd.DataFrame
    ):
        #TODO map weights to ids
        weights = weights.rename(columns={'InvestmentGroupName': 'Fund'}).set_index('Fund')
        weights.drop(columns={"InvestmentGroupId"}, inplace=True)

        header_info = self._format_header_info(acronym=acronym, scenario_name=scenario_name)
        non_config_attributes = self._format_non_config_attributes(reference_attributes=reference_attributes)
        config_attributes = self._format_config_attributes(weights=weights, optim_inputs=optim_inputs,
                                                           rf=optim_inputs.config.expRf)
        metrics = self._combine_metrics_across_weights(weights=weights, optim_inputs=optim_inputs,
                                                       rf=optim_inputs.config.expRf)
        weights = self._format_allocations_summary(weights=weights, metrics=metrics)

        metric_subset = ("distribution_of_returns",
                         "exp_risk_adj_performance",
                         "strategy_allocation",
                         "objective_measures",
                         "liquidity")
        metrics = {k: metrics[k] for k in metric_subset}

        excel_data = {}
        excel_data.update(header_info)
        excel_data.update(non_config_attributes)
        excel_data.update(config_attributes)
        excel_data.update(metrics)
        excel_data.update(weights)

        # TODO set up entity names and ids
        with Scenario(as_of_date=self._as_of_date).context():
            InvestmentsReportRunner().run(
                data=excel_data,
                template="ARS_Portfolio_Construction_Report_Template.xlsx",
                save=True,
                runner=self._runner,
                entity_type=ReportingEntityTypes.portfolio,
                entity_name=acronym + ' ' + scenario_name,
                entity_display_name=acronym + ' ' + scenario_name,
                entity_ids='',
                report_name="ARS Portfolio Construction",
                report_type=ReportType.Risk,
                report_vertical=ReportVertical.ARS,
                report_frequency="Monthly",
                aggregate_intervals=AggregateInterval.MTD,
                output_dir="cleansed/investmentsreporting/printedexcels/",
                report_output_source=DaoSource.DataLake,
            )

    def run(self,
            portfolio_acronym: str,
            scenario_name: str,
            as_of_date: dt.date,
            **kwargs):
        env = os.environ.get("Environment", "prd").replace("local", "prd")
        sub = os.environ.get("Subscription", "prd").replace("local", "prd")

        sql_client = DataAccess().get(
            DataSource.Sql,
            target_name=f"gcm-elasticpoolinvestments-{sub}",
            database_name=f"investmentsdwh-{env}"
        )

        dl_client = DataAccess().get(
            DataSource.DataLake,
            target_name=f"gcmdatalake{sub}",
        )
        optim_inputs = download_optimization_inputs(
            portfolio_acronym=portfolio_acronym,
            scenario_name=scenario_name,
            as_of_date=as_of_date,
            client=dl_client
        )
        weights = get_optimal_weights(
            portfolio_acronym=portfolio_acronym,
            scenario_name=scenario_name,
            as_of_date=as_of_date,
            client=sql_client
        )

        inv_group_id_map = optim_inputs.fundInputs.fundData.investment_group_ids
        inv_group_id_map.InvestmentGroupId = inv_group_id_map.InvestmentGroupId.astype(int)
        weights["InvestmentGroupName"] = weights[["InvestmentGroupId"]].merge(
            inv_group_id_map,
        )["Fund"]

        current_balances = optim_inputs.config.portfolioAttributes.currentBalances
        current_weights = pd.DataFrame.from_dict(current_balances, orient="index", columns=["Current"]).reset_index(
            names="Fund"
        )
        weights = current_weights.merge(
            inv_group_id_map, on="Fund", how="inner"
        ).merge(
            weights, on="InvestmentGroupId", how="outer"
        )
        # TODO: get planned from appropriate locations
        weights["Planned"] = weights["Current"]
        weights.InvestmentGroupName = weights.InvestmentGroupName.combine_first(weights.Fund)
        weights = weights[["InvestmentGroupName", "InvestmentGroupId", "Current", "Planned", "Optimized"]]
        weights = weights.fillna(0)
        weights = weights[weights.iloc[:, 2:].max(axis=1) > 0]

        # these are formal attributes for reference only. they don't flow through to optimizer so are not
        # in the config
        reference_attributes = get_portfolio_attributes(
            portfolio_acronym=portfolio_acronym,
            client=sql_client
        )

        self.generate_excel_report(acronym=portfolio_acronym,
                                   scenario_name=scenario_name,
                                   weights=weights,
                                   optim_inputs=optim_inputs,
                                   reference_attributes=reference_attributes)
        return 'Complete'


if __name__ == "__main__":
    dao_runner = DaoRunner(
        container_lambda=lambda b, i: b.config.from_dict(i),
        config_params={
            DaoRunnerConfigArgs.dao_global_envs.name: {
                DaoSource.DataLake.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                DaoSource.PubDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                DaoSource.InvestmentsDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                DaoSource.DataLake_Blob.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                DaoSource.ReportingStorage.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
            }
        })

    portfolio_acronym = "GIP"
    acronyms = [
        'ALPHAOPP',
        'ANCHOR4',
        'ASUTY',
        'BALCERA',
        'BH2',
        'BUCKEYE',
        'BUTTER',
        'BUTTERB',
        'CARMEL',
        'CASCADE',
        'CHARLES2',
        'CHARTEROAK',
        'CICFOREST',
        'CLOVER2',
        'CORKTOWN',
        'CPA',
        'CPAT',
        'CTOM',
        'ELAR',
        'FALCON',
        'FARIA',
        'FATHUNTER',
        'FOB',
        'FTPAT',
        'GAIC',
        'GIP',
        'RAVEN1',
        'WILMORE'
    ]

    for portfolio_acronym in acronyms:
        as_of_date = dt.date(2023, 5, 1)
        scenario_name = "default_test"

        with Scenario(dao=dao_runner, as_of_date=as_of_date).context():
            PortfolioConstructionReport().run(
                portfolio_acronym=portfolio_acronym,
                scenario_name=scenario_name,
                as_of_date=as_of_date,
            )
