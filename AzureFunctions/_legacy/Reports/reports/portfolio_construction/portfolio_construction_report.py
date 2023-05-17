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
from gcm.Dao.DaoRunner import DaoRunner
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

        wts_formatted.iloc[:, 1:] = wts_formatted.iloc[:, 1:].astype(float).round(0)
        wts_formatted['Delta'] = wts_formatted['Optimized'] - wts_formatted['Planned']
        wts_formatted['Delta - %Risk'] = wts_formatted['Optimized - %Risk'] - wts_formatted['Planned - %Risk']
        column_order = ['Current', 'Planned', 'Optimized', 'Delta']
        column_order = ['Fund'] + column_order + [x + ' - %Risk' for x in column_order]
        wts_formatted = wts_formatted[column_order]

        return wts_formatted

    def _format_allocations_summary(self, weights, metrics):
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
        return formatted_weights

    def _combine_metrics_across_weights(self, weights, optim_inputs, rf):
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
        return metrics

    def _format_header_info(self, acronym):
        header_info = pd.DataFrame(
            {
                "header_info": [
                    acronym,
                    'ARS',
                    self._as_of_date,
                ]
            }
        )
        return header_info

    @staticmethod
    def _summarize_formal_mandate(obs_cons):
        # TODO: refactor with ScenarioAggregator
        stress_idx_to_beta_map = {
            115: 0.15,
            118: 0.18,
            125: 0.25,
            140: 0.40,
        }
        max_beta = stress_idx_to_beta_map[obs_cons.scenario_index]

        mandate = pd.DataFrame(
            {
                "mandate": [
                    "Broad Mandate",
                    "N/A",
                    max_beta,
                    obs_cons.maxFundCount
                ]
            },
            index=['MandateType', 'PerformanceObjective', 'BetaLimit', 'MaxFundCount']
        )
        return mandate

    @staticmethod
    def _summarize_liquidity_cons(obs_cons):
        liquidity_cons = pd.DataFrame(
            {
                "frequency": [
                    'Monthly',
                    'Quarterly',
                    'Semiannual',
                    'Annual',
                    'TwoYears',
                    'Other',
                ],
                "liquidity_cons": [
                    obs_cons.liquidityConstraints.monthly,
                    obs_cons.liquidityConstraints.quarterly,
                    obs_cons.liquidityConstraints.semiannual,
                    obs_cons.liquidityConstraints.annual,
                    obs_cons.liquidityConstraints.twoYears,
                    obs_cons.liquidityConstraints.other,
                ],
            }
        )
        liquidity_cons = liquidity_cons.ffill().fillna(0)
        slf_budget = 1 - liquidity_cons[liquidity_cons['frequency'] == 'Other']['liquidity_cons'].iloc[0]
        slf_budget = pd.DataFrame({'frequency': ['SLF Budget'], 'liquidity_cons': [slf_budget]})
        liquidity_cons = pd.concat([liquidity_cons[liquidity_cons['frequency'] != 'Other'], slf_budget], axis=0)
        liquidity_cons = liquidity_cons.set_index('frequency')
        return liquidity_cons

    @staticmethod
    def _summarize_portfolio_fees(obs_cons):
        fees = pd.DataFrame(
            {
                "fee_terms": [
                    obs_cons.fees.mgmtFee,
                    obs_cons.fees.incentiveFee,
                    obs_cons.fees.hurdleRate,
                ]
            },
            index=['MgmtFee', 'IncentiveFee', 'HurdleRate']
        )
        return fees

    @staticmethod
    def _summarize_stress_limits(obs_cons):
        # TODO: refactor with ScenarioAggregator
        stress_idx_to_beta_map = {
            115: 0.15,
            118: 0.18,
            125: 0.25,
            140: 0.40,
        }
        max_beta = stress_idx_to_beta_map[obs_cons.scenario_index]
        mkt_selloff = max_beta * 1500  # bps
        if obs_cons.scenario_index > 100:
            growth_selloff = 300
            credit_liquidity = 400
            expected_shortfall = 800

        stress_limits = pd.DataFrame(
            {
                "stress_limits": [
                    mkt_selloff,
                    growth_selloff,
                    credit_liquidity,
                    expected_shortfall,
                ]
            },
            index=['MktSelloff', 'GrowthSelloff', 'CreditLiquidity', 'ExpectedShortfall']
        )
        return stress_limits

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

    @staticmethod
    def _summarize_rebalancing_limits(obs_cons):
        rebalance_limits = pd.DataFrame(
            {
                "rebalancing_limits": [
                    "No",
                    "Blank Slate",
                ]
            },
            index=['ReallocateScarceCapacity', 'BlankSlateOrRebal']
        )
        return rebalance_limits

    def _summarize_obs_and_cons(self, weights, optim_inputs, rf):
        metrics = collect_portfolio_metrics(weights=weights['Current'],
                                            optim_inputs=optim_inputs,
                                            rf=rf)
        obs_cons = metrics.obs_cons
        formal_mandate = self._summarize_formal_mandate(obs_cons)
        liquidity_cons = self._summarize_liquidity_cons(obs_cons)
        fees = self._summarize_portfolio_fees(obs_cons)
        target_return = pd.DataFrame({'target_return': [obs_cons.target_return]})
        stress_scenario_limits = self._summarize_stress_limits(obs_cons)
        position_limits = self._summarize_position_limits(obs_cons)
        rebalancing_limits = self._summarize_rebalancing_limits(obs_cons)
        obs_cons_summary = dict({
            "formal_mandate": formal_mandate,
            "liquidity_cons": liquidity_cons,
            "fee_terms": fees,
            "target_return": target_return,
            "stress_limits": stress_scenario_limits,
            "position_limits": position_limits,
            "rebalancing_limits": rebalancing_limits
        })
        return obs_cons_summary

    def generate_excel_report(
        self,
        weights: pd.DataFrame,
        optim_inputs: OptimizationInputs
    ):
        #TODO map weights to ids
        weights = weights.rename(columns={'InvestmentGroupName': 'Fund'}).set_index('Fund')
        weights.drop(columns={"InvestmentGroupId"}, inplace=True)

        rf = optim_inputs.config.expRf
        obs_cons = self._summarize_obs_and_cons(weights=weights, optim_inputs=optim_inputs, rf=rf)
        metrics = self._combine_metrics_across_weights(weights=weights, optim_inputs=optim_inputs, rf=rf)
        allocation_summary = self._format_allocations_summary(weights=weights, metrics=metrics)

        acronym = optim_inputs.config.portfolioAttributes.acronym
        # TODO need to properly order strategy_allocation
        excel_data = {
            "header_info": self._format_header_info(acronym=acronym),
            "formal_mandate": obs_cons['formal_mandate'],
            "fee_terms": obs_cons['fee_terms'],
            "liquidity_constraints": obs_cons['liquidity_cons'],
            "target_return": obs_cons['target_return'],
            "stress_limits": obs_cons['stress_limits'],
            "position_limits": obs_cons['position_limits'],
            "rebalancing_limits": obs_cons["rebalancing_limits"],
            "objective_measures": metrics['objective_measures'],
            "distribution_of_returns": metrics['outcomes_distribution'],
            "exp_risk_adj_performance": metrics['risk_adj_performance'],
            "strategy_allocation": metrics['strategy_weights'],
            "liquidity": metrics['liquidity'],
            "allocation_summary": allocation_summary
        }

        # TODO set up entity names and ids
        with Scenario(as_of_date=self._as_of_date).context():
            InvestmentsReportRunner().run(
                data=excel_data,
                template="ARS_Portfolio_Construction_Report_Template.xlsx",
                save=True,
                runner=self._runner,
                entity_type=ReportingEntityTypes.portfolio,
                entity_name=acronym,
                entity_display_name=acronym,
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
        env = os.environ.get("Environment", "dev").replace("local", "dev")
        sub = os.environ.get("Subscription", "nonprd").replace("local", "nonprd")

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

        self.generate_excel_report(weights=weights,
                                   optim_inputs=optim_inputs)
        return 'Complete'


if __name__ == "__main__":
    portfolio_acronym = "ANCHOR4"
    as_of_date = dt.date(2023, 3, 1)
    scenario_name = "default_test"

    with Scenario(dao=DaoRunner(), as_of_date=as_of_date).context():
        PortfolioConstructionReport().run(
            portfolio_acronym=portfolio_acronym,
            scenario_name=scenario_name,
            as_of_date=as_of_date,
        )
