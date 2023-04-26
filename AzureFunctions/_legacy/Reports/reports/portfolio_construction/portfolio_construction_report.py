import datetime as dt
import pandas as pd
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
from gcm.inv.models.portfolio_construction.portfolio_metrics.portfolio_metrics import collect_portfolio_metrics
from dataclasses import fields


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

    def _combine_metrics_across_weights(self, weights, optim_config_name, rf):
        portfolio_metrics = {k: collect_portfolio_metrics(runner=self._runner,
                                                          weights=weights[k],
                                                          optim_config_name=optim_config_name,
                                                          rf=rf) for k in weights.columns}

        metrics = {key: None for key in [f.name for f in fields(portfolio_metrics['Current'])]}
        for m in metrics.keys():
            metrics[m] = pd.concat([getattr(portfolio_metrics['Current'], m),
                                    getattr(portfolio_metrics['Planned'], m),
                                    getattr(portfolio_metrics['Optimized'], m)], axis=1)
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

    def generate_excel_report(self, weights, optim_config_name, rf):
        acronym = 'Sample'

        #TODO map weights to ids
        weights = weights.rename(columns={'InvestmentGroupName': 'Fund'}).set_index('Fund')
        weights.drop(columns={"InvestmentGroupId"}, inplace=True)

        metrics = self._combine_metrics_across_weights(weights=weights, optim_config_name=optim_config_name, rf=rf)
        allocation_summary = self._format_allocations_summary(weights=weights, metrics=metrics)

        # TODO need to properly order strategy_allocation
        excel_data = {
            "header_info": self._format_header_info(acronym=acronym),
            "objective_measures": metrics['objective_measures'],
            "distribution_of_returns": metrics['outcomes_distribution'],
            "exp_risk_adj_performance": metrics['risk_adj_performance'],
            "strategy_allocation": metrics['strategy_weights'],
            "liquidity": metrics['liquidity'],
            "allocation_summary": allocation_summary
        }

        # TODO set up entity names and ids
        with Scenario(as_of_date=self._as_of_date).context():
            InvestmentsReportRunner().execute(
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
            )

    def run(self, weights, optim_config_name, rf, **kwargs):
        self.generate_excel_report(weights=weights,
                                   optim_config_name=optim_config_name,
                                   rf=rf)
        return 'Complete'


if __name__ == "__main__":
    weights = pd.read_csv('test_weights.csv').fillna(0)
    weights = weights[weights.iloc[:, 2:].max(axis=1) > 0]

    with Scenario(dao=DaoRunner(), as_of_date=dt.date(2023, 2, 28)).context():
        PortfolioConstructionReport().execute(weights=weights,
                                              optim_config_name='manager_inputs_2023_04_24',
                                              rf=0.04)
