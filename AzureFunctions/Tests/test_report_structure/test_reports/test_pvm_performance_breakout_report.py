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
)
from utils.conversion_tools.report_structure_to_excel import (
    save_report_structure_to_excel_return_virtual_book,
)


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

    def test_render_single_port_report(self):
        as_of_date = dt.date(2022, 12, 31)
        with Scenario(
            as_of_date=as_of_date,
            aggregate_interval=AggregateInterval.ITD,
            save=True,
        ).context():
            port_name = "The Consolidated Edison Pension Plan Master Trust - GCM PE Account"
            domain = EntityDomainTypes.Portfolio
            info = TestPerformanceBreakDown.get_entity(
                domain=domain, name=port_name
            )
            this_report = PvmPerformanceBreakoutReport(
                ReportMeta(
                    type=ReportType.Performance,
                    interval=Scenario.get_attribute("aggregate_interval"),
                    consumer=ReportConsumer(
                        horizontal=[ReportConsumer.Horizontal.IC],
                        vertical=ReportConsumer.Vertical.PEREI,
                    ),
                    frequency=Frequency(
                        FrequencyType.Once,
                        Scenario.get_attribute("as_of_date"),
                    ),
                    entity_domain=domain,
                    entity_info=info,
                )
            )
            template = this_report.get_template()
            virtual_workbook = (
                save_report_structure_to_excel_return_virtual_book(
                    template, this_report, Scenario.get_attribute("save")
                )
            )
            assert virtual_workbook is not None
