import numpy as np
import pytest

from Reporting.Reports.entity_reports.utils.pvm_performance_utils.pvm_performance_helper import (
    PvmPerformanceHelper,
    pd,
)
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
from Reporting.Reports.report_names import ReportNames
from Reporting.core.report_structure import (
    ReportMeta,
    Frequency,
    FrequencyType,
    ReportType,
    ReportConsumer,
    Calendar,
)
from utils.print_utils import print
from gcm.Dao.DaoRunner import DaoSource, DaoRunnerConfigArgs
from gcm.inv.scenario import Scenario, DaoRunner


@pytest.mark.skip()
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
        entity_info.SourceName = np.where(
            entity_info.SourceName == "PVM.MED",
            "pvm-med",
            entity_info.SourceName,
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
            raw = (
                f" select distinct OwnerName as OperationalSeriesTicker"
                f" from iLevel.vExtendedCollapsedCashflows"
                f" where TransactionDate = '{as_of_date}'"
                f" and TransactionType = 'Net Asset Value'"
                f" and BaseAmount > 0"
            )
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
            portfolios = portfolios[
                portfolios.OperationalSeriesTicker.isin(
                    active_os.OperationalSeriesTicker
                )
            ]

        # TODO: set up warning and or assertion error
        return portfolios

    @pytest.mark.skip()
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

    @pytest.mark.skip()
    def test_new_report_run(self):
        as_of_date = dt.date(2023, 3, 31)
        # as_of_date = dt.date(2022, 12, 31)
        error_df = pd.DataFrame()

        with Scenario(
            as_of_date=as_of_date,
            aggregate_interval=AggregateInterval.ITD,
            save=False,
            dao_config={
                DaoRunnerConfigArgs.dao_global_envs.name: {
                    # DaoSource.DataLake.name: {
                    #     "Environment": "prd",
                    #     "Subscription": "prd",
                    # },
                    # DaoSource.PubDwh.name: {
                    #     "Environment": "prd",
                    #     "Subscription": "prd",
                    # },
                    # DaoSource.InvestmentsDwh.name: {
                    #     "Environment": "prd",
                    #     "Subscription": "prd",
                    # },
                    # DaoSource.DataLake_Blob.name: {
                    #     "Environment": "prd",
                    #     "Subscription": "prd",
                    # },
                    # DaoSource.ReportingStorage.name: {
                    #     "Environment": "prd",
                    #     "Subscription": "prd",
                    # },
                }
            },
        ).context():
            # set report name and dimension config
            reports_to_run = [
                ReportNames.PE_Portfolio_Performance_x_Investment_Manager,
                ReportNames.PE_Portfolio_Performance_x_Vintage_Realization_Status,
                ReportNames.PE_Portfolio_Performance_x_Sector,
                ReportNames.PE_Portfolio_Performance_x_Region,
            ]

            # get relevant portfolios to run
            portfolios_to_run = (
                TestPerformanceBreakDown.get_pe_only_portfolios(
                    active_only=True
                )
            )
            port_list = list(
                set(portfolios_to_run.PortfolioReportingName.to_list())
            )
            for port_name in port_list:
                for report_name_enum in reports_to_run:
                    domain = EntityDomainTypes.Portfolio
                    info = TestPerformanceBreakDown.get_entity(
                        domain=domain, name=port_name
                    )
                    try:
                        this_report = PvmPerformanceBreakoutReport(
                            report_name_enum=report_name_enum,
                            report_meta=ReportMeta(
                                type=ReportType.Performance,
                                interval=Scenario.get_attribute(
                                    "aggregate_interval"
                                ),
                                consumer=ReportConsumer(
                                    horizontal=[
                                        ReportConsumer.Horizontal.IC
                                    ],
                                    vertical=ReportConsumer.Vertical.PE,
                                ),
                                frequency=Frequency(
                                    FrequencyType.Quarterly,
                                    Calendar.AllDays,
                                ),
                                entity_domain=domain,
                                entity_info=info,
                            ),
                        )

                        output = print(
                            report_structure=this_report, print_pdf=True
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
                    # error_df.to_csv('C:/Tmp/error df port reports.csv')
                    assert output is not None
