from gcm.inv.scenario import Scenario
from gcm.inv.utils.date.AggregateInterval import AggregateInterval
import datetime as dt
from gcm.inv.entityhierarchy.EntityDomain.entity_domain.entity_domain_types import (
    EntityDomainTypes,
    get_domain,
    EntityDomain,
)
from Reporting.Reports.controller import validate_meta
import pandas as pd
from Reporting.core.report_structure import (
    ReportMeta,
    Frequency,
    FrequencyType,
)
from Reporting.Reports.entity_reports.vertical_reports.ars_pfund_attributes.aggregated_pfund_attribute_report import (
    AggregatedPortolioFundAttributeReport,
)
from utils.conversion_tools.report_structure_to_excel import (
    save_report_structure_to_excel_return_virtual_book,
)


class TestPFundAttReport(object):
    def test_run_local(self):
        with Scenario(
            as_of_date=dt.date(2022, 9, 30),
            aggregate_interval=AggregateInterval.Multi,
            save=True,
        ).context():
            domain = EntityDomainTypes.Vertical
            entity_domain_table: EntityDomain = get_domain(domain)
            [r, s] = entity_domain_table.get_by_entity_names(
                ["ARS"],
            )
            entity_info: pd.DataFrame = EntityDomain.merge_ref_and_sources(
                r, s
            )
            class_structure = AggregatedPortolioFundAttributeReport
            available_metas = class_structure.available_metas()
            this_meta = ReportMeta(
                type=available_metas.report_type,
                interval=Scenario.get_attribute("aggregate_interval"),
                consumer=available_metas.consumer,
                frequency=Frequency(
                    FrequencyType.Monthly,
                    Scenario.get_attribute("as_of_date"),
                ),
                entity_domain=domain,
                entity_info=entity_info,
            )
            validate_meta(class_structure, this_meta)
            this_report = class_structure(this_meta)
            template = this_report.get_template()
            virtual_workbook = (
                save_report_structure_to_excel_return_virtual_book(
                    template, this_report, Scenario.get_attribute("save")
                )
            )
            assert this_report is not None and virtual_workbook is not None
