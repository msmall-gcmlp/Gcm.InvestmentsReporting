import datetime as dt
from gcm.Dao.DaoSources import DaoSource

from _legacy.Reports.reports.portfolio_construction.portfolio_construction_report import generate_excel_report_data, \
    get_report_data
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
from gcm.inv.dataprovider.portfolio import Portfolio


class PortfolioConstructionReport(ReportingRunnerBase):
    def __init__(self):
        super().__init__(runner=Scenario.get_attribute("dao"))
        self._as_of_date = Scenario.get_attribute("as_of_date")

    def generate_excel_report(self, acronym, scenario_name, as_of_date):
        weights, optim_inputs, reference_attributes = get_report_data(portfolio_acronym=acronym,
                                                                      scenario_name=scenario_name,
                                                                      as_of_date=as_of_date)
        excel_data = generate_excel_report_data(acronym=acronym,
                                                scenario_name=scenario_name,
                                                as_of_date=as_of_date,
                                                weights=weights,
                                                optim_inputs=optim_inputs,
                                                reference_attributes=reference_attributes)
        portfolio_dimn = Portfolio(acronyms=[acronym]).get_dimensions()

        as_of_date = dt.datetime.combine(self._as_of_date, dt.datetime.min.time())
        if scenario_name == 'default_test':
            with Scenario(as_of_date=as_of_date).context():
                InvestmentsReportRunner().run(
                    data=excel_data,
                    template="ARS_Portfolio_Construction_Report_Template.xlsx",
                    save=True,
                    runner=self._runner,
                    entity_type=ReportingEntityTypes.portfolio,
                    entity_name=acronym,
                    entity_display_name=acronym,
                    entity_ids=[portfolio_dimn[portfolio_dimn.Acronym == acronym].MasterId.item()],
                    entity_source=DaoSource.PubDwh,
                    report_name="ARS Portfolio Construction",
                    report_type=ReportType.Risk,
                    report_vertical=ReportVertical.ARS,
                    report_frequency="Monthly",
                    aggregate_intervals=AggregateInterval.MTD,
                    # output_dir="cleansed/investmentsreporting/printedexcels/",
                    # report_output_source=DaoSource.DataLake,
                )
        else:
            with Scenario(as_of_date=as_of_date).context():
                InvestmentsReportRunner().run(
                    data=excel_data,
                    template="ARS_Portfolio_Construction_Report_Template.xlsx",
                    save=True,
                    runner=self._runner,
                    entity_type=ReportingEntityTypes.cross_entity,
                    entity_name=acronym + ' ' + scenario_name,
                    entity_display_name=acronym + ' ' + scenario_name,
                    entity_ids='',
                    entity_source=DaoSource.PubDwh,
                    report_name="ARS Portfolio Construction",
                    report_type=ReportType.Risk,
                    report_vertical=ReportVertical.ARS,
                    report_frequency="Monthly",
                    aggregate_intervals=AggregateInterval.MTD,
                    # output_dir="cleansed/investmentsreporting/printedexcels/",
                    # report_output_source=DaoSource.DataLake,
                )

    def run(self,
            portfolio_acronym: str,
            scenario_name: str,
            as_of_date: dt.date,
            **kwargs):

        self.generate_excel_report(acronym=portfolio_acronym,
                                   scenario_name=scenario_name,
                                   as_of_date=as_of_date)
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
