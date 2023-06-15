from copy import deepcopy

from Reporting.Reports.entity_reports.utils.pvm_performance_utils.pvm_performance_helper import (
    PvmPerformanceHelper,
    pd,
)
from gcm.inv.scenario import Scenario
from gcm.inv.utils.date.AggregateInterval import AggregateInterval
import datetime as dt
from gcm.inv.entityhierarchy.EntityDomain.entity_domain.entity_domain_types import (
    EntityDomainTypes,
    get_domain,
    EntityDomain,
)
from Reporting.Reports.entity_reports.xentity_reports.pvm_portfolio_performance_report import (
    PvmPerformanceBreakoutReport,
)
from Reporting.core.report_structure import (
    ReportMeta,
    Frequency,
    FrequencyType,
    ReportType,
    ReportConsumer,
    Calendar,
)
from utils.print_utils import print
import pytest
from gcm.Dao.DaoRunner import DaoSource, DaoRunnerConfigArgs
from gcm.inv.scenario import Scenario, DaoRunner


class TestPerformanceBreakDown(object):
    @staticmethod
    def get_entity(domain, name):
        entity_domain_table: EntityDomain = get_domain(domain)
        [r, s] = entity_domain_table.get_by_entity_names(
            [name],
        )
        entity_info: pd.DataFrame = EntityDomain.merge_ref_and_sources(
            r, s
        )
        return entity_info

    @staticmethod
    def get_pe_only_portfolios(active_only=True):
        def pvm_dao_operation(dao, params):
            raw = """
            select distinct
                [Portfolio Master Id] PortfolioMasterId, 
                [Operational Series Ticker] OperationalSeriesTicker,
                [Portfolio Reporting Name] PortfolioReportingName, 
                [Operational Series Investment Type] OperationalSeriesInvestmentType,
                [Portfolio Ticker] PortfolioTicker, 
                [Portfolio Currency] PortfolioCurrency, 
                [Deal Predominant Asset Class] DealPredominantAssetClass
            from analytics.MasterEntityDataInvestmentTrack
            where [Portfolio Master Id] not in 
                (
                    select distinct [Portfolio Master Id]
                    from analytics.MasterEntityDataInvestmentTrack
                    where [Deal Predominant Asset Class] != 'Private Equity'
                )
            """
            df = pd.read_sql(
                raw,
                dao.data_engine.session.bind,
            )
            return df

        def idw_dao_operation(dao, params):
            raw = f" select distinct OwnerName as OperationalSeriesTicker" \
                  f" from iLevel.vExtendedCollapsedCashflows" \
                  f" where TransactionDate = '{as_of_date}'" \
                  f" and TransactionType = 'Net Asset Value'" \
                  f" and BaseAmount > 0"
            df = pd.read_sql(
                raw,
                dao.data_engine.session.bind,
            )
            return df

        dao: DaoRunner = Scenario.get_attribute("dao")
        as_of_date: dt.date = Scenario.get_attribute("as_of_date")

        portfolios: pd.DataFrame = dao.execute(
            params={},
            source=DaoSource.PvmDwh,
            operation=pvm_dao_operation,
        )
        if active_only:
            active_os: pd.DataFrame = dao.execute(
                params={},
                source=DaoSource.InvestmentsDwh,
                operation=idw_dao_operation,
            )
            portfolios = portfolios[portfolios.OperationalSeriesTicker.isin(active_os.OperationalSeriesTicker)]

        # TODO: set up warning and or assertion error
        return portfolios

    def test_basic_helper_object(self):
        as_of_date = dt.date(2022, 12, 31)
        with Scenario(
            as_of_date=as_of_date,
            aggregate_interval=AggregateInterval.ITD,
            save=False,
        ).context():
            port_name = "The Consolidated Edison Pension Plan Master Trust - GCM PE Account"
            domain = EntityDomainTypes.Portfolio
            info = TestPerformanceBreakDown.get_entity(
                domain=domain, name=port_name
            )
            this_helper = PvmPerformanceHelper(domain, info)
            final_data = this_helper.generate_components_for_this_entity(
                as_of_date
            )
            assert final_data is not None

    def test_render_single_vertical_report(self):
        as_of_date = dt.date(2022, 3, 31)
        # as_of_date = dt.date(2023, 3, 31)
        with Scenario(
            as_of_date=as_of_date,
            aggregate_interval=AggregateInterval.ITD,
            save=True,
        ).context():
            # port_name = "The Consolidated Edison Pension Plan Master Trust - GCM PE Account"
            domain = EntityDomainTypes.Vertical
            info = TestPerformanceBreakDown.get_entity(
                domain=domain, name="ARS"
            )
            this_report = PvmPerformanceBreakoutReport(
                ReportMeta(
                    type=ReportType.Performance,
                    interval=Scenario.get_attribute("aggregate_interval"),
                    consumer=ReportConsumer(
                        horizontal=[ReportConsumer.Horizontal.IC],
                        vertical=ReportConsumer.Vertical.PE,
                    ),
                    frequency=Frequency(
                        FrequencyType.Once, Calendar.AllDays
                    ),
                    entity_domain=domain,
                    entity_info=info,
                )
            )
            output = print(report_structure=this_report, print_pdf=True)
        assert output is not None

    def test_render_single_port_report(self):
        as_of_date = dt.date(2022, 12, 31)
        with Scenario(
            as_of_date=as_of_date,
            aggregate_interval=AggregateInterval.ITD,
            save=True,
        ).context():
            for port_name in [
                "GCM Grosvenor Co-Investment Opportunities Fund, L.P.",
                "GCM Grosvenor Co-Investment Opportunities Fund II, L.P.",
                "GCM Grosvenor Co-Investment Opportunities Fund III, L.P.",

                # "GCM Grosvenor Secondary Opportunities Fund II, L.P.",
                # "GCM Grosvenor Customized Infrastructure Strategies II, L.P.",
                # "GCM Grosvenor Customized Infrastructure Strategies III, L.P.",
                # "Labor Impact Fund, L.P.",
                # "GCM Grosvenor Infrastructure Advantage Fund II, L.P."
            ]:
                # for port_name in ['The Consolidated Edison Pension Plan Master Trust - GCM PE Account']:
                # port_name = "The Consolidated Edison Pension Plan Master Trust - GCM PE Account"
                domain = EntityDomainTypes.Portfolio
                info = TestPerformanceBreakDown.get_entity(
                    domain=domain, name=port_name
                )

                this_report = PvmPerformanceBreakoutReport(
                    ReportMeta(
                        type=ReportType.Performance,
                        interval=Scenario.get_attribute(
                            "aggregate_interval"
                        ),
                        consumer=ReportConsumer(
                            horizontal=[ReportConsumer.Horizontal.IC],
                            vertical=ReportConsumer.Vertical.PE,
                        ),
                        frequency=Frequency(
                            FrequencyType.Once, Calendar.AllDays
                        ),
                        entity_domain=domain,
                        entity_info=info,
                    )
                )
                output = print(
                    report_structure=this_report, print_pdf=True
                )
            assert output is not None


    def test_render_single_port_report_combined(self):
        # as_of_date = dt.date(2023, 3, 31)
        as_of_date = dt.date(2022, 12, 31)

        with Scenario(
            as_of_date=as_of_date,
            aggregate_interval=AggregateInterval.ITD,
            save=True,
            dao_config={
                DaoRunnerConfigArgs.dao_global_envs.name: {
                    DaoSource.DataLake.name: {
                        "Environment": "prd",
                        "Subscription": "prd",
                    },
                    DaoSource.PubDwh.name: {
                        "Environment": "prd",
                        "Subscription": "prd",
                    },
                    # DaoSource.InvestmentsDwh.name: {
                    #     "Environment": "prd",
                    #     "Subscription": "prd",
                    # },
                    DaoSource.DataLake_Blob.name: {
                        "Environment": "prd",
                        "Subscription": "prd",
                    },
                    DaoSource.ReportingStorage.name: {
                        "Environment": "prd",
                        "Subscription": "prd",
                    },
                }
            }
        ).context():
            portfolios_to_run = TestPerformanceBreakDown.get_pe_only_portfolios(active_only=True)
            # for port_name in [
            #     # "GCM Grosvenor Co-Investment Opportunities Fund, L.P.",
            #     # "GCM Grosvenor Co-Investment Opportunities Fund II, L.P.",
            #     # "GCM Grosvenor Co-Investment Opportunities Fund III, L.P.",
            #     'The Consolidated Edison Pension Plan Master Trust - GCM PE Account'
            #     # "GCM Grosvenor Secondary Opportunities Fund II, L.P.",
            #     # "GCM Grosvenor Customized Infrastructure Strategies II, L.P.",
            #     # "GCM Grosvenor Customized Infrastructure Strategies III, L.P.",
            #     # "Labor Impact Fund, L.P.",
            #     # "GCM Grosvenor Infrastructure Advantage Fund II, L.P."
            # ]:
            error_df = pd.DataFrame()
            for port_name in portfolios_to_run.PortfolioReportingName.unique():
                # for port_name in ['The Consolidated Edison Pension Plan Master Trust - GCM PE Account']:
                # port_name = "The Consolidated Edison Pension Plan Master Trust - GCM PE Account"

                domain = EntityDomainTypes.Portfolio
                info = TestPerformanceBreakDown.get_entity(
                    domain=domain, name=port_name
                )

                # hard code testing
                recursion_iterate_controller = [
                    [
                        ["Portfolio"],
                        ["Portfolio", "VintageYear"],
                        ["Portfolio", "VintageYear", "PredominantRealizationTypeCategory"],
                        ["Portfolio", "VintageYear", "PredominantRealizationTypeCategory", "DealName"]
                    ],
                    [
                        ["Portfolio"],
                        ["Portfolio", "InvestmentManagerName"],
                        ["Portfolio", "InvestmentManagerName", "DealName"]
                    ],
                    [
                        ["Portfolio"],
                        ["Portfolio", "PredominantSector"],
                        ["Portfolio", "PredominantSector", "DealName"]
                    ],
                    [
                        ["Portfolio"],
                        ["Portfolio", "PredominantAssetRegion"],
                        ["Portfolio", "PredominantAssetRegion", "DealName"]
                    ],
                ]
                attributes_needed = [
                    [
                        "Name",
                        "VintageYear",
                        "PredominantRealizationTypeCategory",
                        "DealName"
                    ],
                    [
                        "Name",
                        "InvestmentManagerName",
                        "DealName"
                    ],
                    [
                        "Name",
                        "PredominantSector",
                        "DealName"
                    ],
                    [
                        "Name",
                        "PredominantAssetRegion",
                        "DealName"
                    ],
                ]
                try:
                    for i in range(len(recursion_iterate_controller)):

                        this_report = PvmPerformanceBreakoutReport(
                            ReportMeta(
                                type=ReportType.Performance,
                                interval=Scenario.get_attribute(
                                    "aggregate_interval"
                                ),
                                consumer=ReportConsumer(
                                    horizontal=[ReportConsumer.Horizontal.IC],
                                    vertical=ReportConsumer.Vertical.PE,
                                ),
                                frequency=Frequency(
                                    FrequencyType.Once, Calendar.AllDays
                                ),
                                entity_domain=domain,
                                entity_info=info,
                            ),
                            recursion_iterate_controller=recursion_iterate_controller[i],
                            attributes_needed=attributes_needed[i],
                            iteration_number=i
                        )
                        # val = [x.df for x in this_report.components[0].report_sheets[0].report_tables if 'Page' in x.component_name]
                        # named_range = [x.component_name for x in this_report.components[0].report_sheets[0].report_tables if
                        #        'Page' in x.component_name]


                        if i == 0:
                            final_report = deepcopy(this_report)
                        else:
                            final_report.components = final_report.components + this_report.components

                    # final_list = [this_report]
                    # components: List[ReportWorkBookHandler] = this_report_again.components
                    # this_report.components = this_report.components + this_report_again.components

                    output = print(
                        report_structure=final_report, print_pdf=True
                    )
                except Exception as e:
                    error_msg = getattr(e, "message", repr(e))
                    # print(error_msg)
                    error_df = pd.concat(
                        [
                            pd.DataFrame(
                                {
                                    "Portfolio": [port_name],
                                    "Date": [as_of_date],
                                    "ErrorMessage": [error_msg],
                                }
                            ),
                            error_df,
                        ]
                    )
            error_df.to_csv('C:/Tmp/error df port reports.csv')
            assert output is not None


    def test_render_single_mgr_report(self):
        as_of_date = dt.date(2022, 12, 31)
        with Scenario(
            as_of_date=as_of_date,
            aggregate_interval=AggregateInterval.ITD,
            save=True,
        ).context():
            mgr_name = "Trive Capital Management, LLC"
            domain = EntityDomainTypes.InvestmentManager
            info = TestPerformanceBreakDown.get_entity(
                domain=domain, name=mgr_name
            )
            this_report = PvmPerformanceBreakoutReport(
                ReportMeta(
                    type=ReportType.Performance,
                    interval=Scenario.get_attribute("aggregate_interval"),
                    consumer=ReportConsumer(
                        horizontal=[ReportConsumer.Horizontal.IC],
                        vertical=ReportConsumer.Vertical.PE,
                    ),
                    frequency=Frequency(
                        FrequencyType.Once, Calendar.AllDays
                    ),
                    entity_domain=domain,
                    entity_info=info,
                )
            )
            output = print(report_structure=this_report, print_pdf=False)
            assert output is not None
