import pytest
pytest.skip(
    "import error in InvestmentsModels",
    allow_module_level=True
)
from gcm.inv.scenario import Scenario
from gcm.inv.utils.date.AggregateInterval import (
    AggregateInterval,
    AggregateIntervalReportHandler,
)
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
from gcm.inv.utils.pvm.perf_evaluation import PerformanceEvaluationType
from Reporting.Reports.entity_reports.xentity_reports.pvm_tr.aggregation import (
    PvmManagerTrackRecordReportAggregation,
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

    @pytest.mark.skip
    def test_run_local(self):
        a = AggregateInterval.ITD
        with Scenario(
            as_of_date=dt.date(2023, 3, 31),
            aggregate_interval=a,
            pvm_performance_type=PerformanceEvaluationType.RMV,
            save=True,
        ).context():
            d = EntityDomainTypes.InvestmentManager
            entity_info = TestPvmManagerTrReport.get_entity(
                d, "Mandrake Capital Partners"
            )
            this_meta = ReportMeta(
                type=ReportType.Performance,
                intervals=AggregateIntervalReportHandler([a]),
                consumer=ReportConsumer(
                    horizontal=[ReportConsumer.Horizontal.IC],
                    vertical=ReportConsumer.Vertical.Real_Estate,
                ),
                frequency=Frequency(
                    FrequencyType.Once,
                    Scenario.get_attribute("as_of_date"),
                ),
                entity_domain=d,
                entity_info=entity_info,
            )
            this_report = PvmManagerTrackRecordReportAggregation(this_meta)
            assert this_report is not None
            output = print(report_structure=this_report, print_pdf=False)
            assert output is not None
