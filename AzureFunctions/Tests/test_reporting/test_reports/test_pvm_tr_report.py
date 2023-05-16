from gcm.inv.scenario import Scenario
from gcm.inv.utils.date.AggregateInterval import AggregateInterval
import datetime as dt
from gcm.inv.entityhierarchy.EntityDomain.entity_domain.entity_domain_types import (
    EntityDomainTypes,
    get_domain,
    EntityDomain,
)
import pandas as pd
from Reporting.core.report_structure import (
    ReportMeta,
    Frequency,
    FrequencyType,
    ReportType,
    ReportConsumer,
)
from Reporting.Reports.entity_reports.investment_manager_reports.pvm_manager_trackrecord_report import (
    PvmManagerTrackRecordReport,
)
from utils.print_utils import print


class TestPvmManagerTrReport(object):
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

    def test_run_local(self):
        with Scenario(
            as_of_date=dt.date(2022, 6, 30),
            aggregate_interval=AggregateInterval.ITD,
            save=True,
        ).context():
            d = EntityDomainTypes.InvestmentManager
            entity_info = TestPvmManagerTrReport.get_entity(
                d, "ExampleManagerName"
            )

            this_report = PvmManagerTrackRecordReport(
                ReportMeta(
                    type=ReportType.Performance,
                    interval=Scenario.get_attribute("aggregate_interval"),
                    consumer=ReportConsumer(
                        horizontal=[ReportConsumer.Horizontal.IC],
                        vertical=ReportConsumer.Vertical.PE,
                    ),
                    frequency=Frequency(
                        FrequencyType.Once,
                        Scenario.get_attribute("as_of_date"),
                    ),
                    entity_domain=d,
                    entity_info=entity_info,
                ),
            )
            assert this_report is not None
            output = print(report_structure=this_report, print_pdf=True)
            assert output is not None
