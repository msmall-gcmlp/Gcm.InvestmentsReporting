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


class PortfolioConstructionReport(ReportingRunnerBase):
    def __init__(self):
        super().__init__(runner=Scenario.get_attribute("dao"))
        self._as_of_date = Scenario.get_attribute("as_of_date")

    def generate_excel_report(self, weights, optim_config_name, rf):
        portfolio_metrics = collect_portfolio_metrics(runner=self._runner,
                                                      weights=weights,
                                                      optim_config_name=optim_config_name,
                                                      rf=rf)

        # TODO need to properly order strategy_allocation
        excel_data = {
            "objective_measures": portfolio_metrics.objective_measures.to_frame(),
            "distribution_of_returns": portfolio_metrics.outcomes_distribution.to_frame(),
            "exp_risk_adj_performance": portfolio_metrics.risk_adj_performance.to_frame(),
            "strategy_allocation": portfolio_metrics.strategy_weights.to_frame(),
            "liquidity": portfolio_metrics.liquidity.to_frame()
        }

        # TODO set up entity names and ids
        with Scenario(as_of_date=self._as_of_date).context():
            InvestmentsReportRunner().execute(
                data=excel_data,
                template="ARS_Portfolio_Construction_Report_Template.xlsx",
                save=True,
                runner=self._runner,
                entity_type=ReportingEntityTypes.portfolio,
                entity_name='ARS',
                entity_display_name='ARS',
                entity_ids='',
                report_name="ARS Portfolio Construction",
                report_type=ReportType.Risk,
                report_vertical=ReportVertical.ARS,
                report_frequency="Monthly",
                aggregate_intervals=AggregateInterval.MTD,
            )

    def run(self, weights, optim_config_name, rf, **kwargs):
        self.generate_excel_report(weights=weights, optim_config_name=optim_config_name, rf=rf)
        return 'Complete'


if __name__ == "__main__":
    weights = pd.Series([0.01785714, 0.01785714, 0.01785714, 0.01785714, 0.01785714,
                         0.01785714, 0.01785714, 0.01785714, 0.01785714, 0.01785714,
                         0.01785714, 0.01785714, 0.01785714, 0.01785714, 0.01785714,
                         0.01785714, 0.01785714, 0.01785714, 0.01785714, 0.01785714,
                         0.01785714, 0.01785714, 0.01785714, 0.01785714, 0.01785714,
                         0.01785714, 0.01785714, 0.01785714, 0.01785714, 0.01785714,
                         0.01785714, 0.01785714, 0.01785714, 0.01785714, 0.01785714,
                         0.01785714, 0.01785714, 0.01785714, 0.01785714, 0.01785714,
                         0.01785714, 0.01785714, 0.01785714, 0.01785714, 0.01785714,
                         0.01785714, 0.01785714, 0.01785714, 0.01785714, 0.01785714,
                         0.01785714, 0.01785714, 0.01785714, 0.01785714, 0.01785714, 0.01785714],
                        index=['Alphadyne Global Rates II', 'Altimeter',
                               'Apollo Credit Strategies Fund', 'Aspex', 'Atlas Enhanced Fund',
                               'Avidity', 'BlackRock Strategic', 'Brevan Howard Alpha Strategies',
                               'Brevan Howard FG Macro', 'Brigade Structured Credit',
                               'Canyon VRF', 'Capula Tactical Macro', 'Citadel',
                               'Citadel Global Equities', 'Corre Opportunities', 'D1 Capital',
                               'DE Shaw', 'Deep Track', 'Diameter Main Fund',
                               'Dragoneer Global Fund', 'Element', 'Elliott', 'Exodus Point',
                               'Fidera', 'GCM Equity Opps Fund', 'GCM Special Opps Fund',
                               'Hel Ved', 'Kennedy Lewis Dislocation', 'Kinetic Partners',
                               'LMR Master', 'Lynrock Lake', 'Magnetar Constellation',
                               'Maplelane', 'Marshall Wace Eureka Fund', 'Pentwater Credit',
                               'Pentwater Equity Opp', 'Point72', 'QRT Torus', 'ReadyState',
                               'RedCo', 'Redmile', 'Rokos', 'Sculptor Credit Opps',
                               'Silver Point', 'Skye', 'Sona Credit', 'Steadfast', 'Tairen Alpha',
                               'Tiger Global', 'TPGEquity', 'Voloridge', 'Voyager',
                               'Waterfall Victoria', 'Whale Rock', 'Whale Rock Hybrid Long/Short',
                               'Woodline Partners'])

    with Scenario(dao=DaoRunner(), as_of_date=dt.date(2023, 2, 28)).context():
        PortfolioConstructionReport().execute(weights=weights,
                                              optim_config_name='test_optim_data',
                                              rf=0.04)
