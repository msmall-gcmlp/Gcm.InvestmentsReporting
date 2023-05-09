from gcm.Dao.DaoRunner import DaoRunnerConfigArgs
from gcm.Dao.DaoSources import DaoSource
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
from Reporting.Reports.entity_reports.vertical_reports.ars_pfund_attributes.aggregated_pfund_attribute_report import (
    AggregatedPortolioFundAttributeReport,
)


class TestPFundAttReport(object):
    def test_run_local(self):
        with Scenario(
            as_of_date=dt.date(2022, 9, 1),
            aggregate_interval=AggregateInterval.Multi,
            save=False,
            dao_config={
                DaoRunnerConfigArgs.dao_global_envs.name: {
                    DaoSource.PubDwh.name: {
                        "Environment": "prd",
                        "Subscription": "prd",
                    },
                }
            },
        ).context():
            domain = EntityDomainTypes.Vertical
            entity_domain_table: EntityDomain = get_domain(domain)
            [r, s] = entity_domain_table.get_by_entity_names(
                ["ARS"],
            )
            entity_info: pd.DataFrame = EntityDomain.merge_ref_and_sources(
                r, s
            )
            this_report = AggregatedPortolioFundAttributeReport(
                ReportMeta(
                    type=ReportType.Performance,
                    interval=Scenario.get_attribute("aggregate_interval"),
                    consumer=ReportConsumer(
                        horizontal=[ReportConsumer.Horizontal.PM],
                        vertical=ReportConsumer.Vertical.ARS,
                    ),
                    frequency=Frequency(
                        FrequencyType.Monthly,
                        Scenario.get_attribute("as_of_date"),
                    ),
                    entity_domain=domain,
                    entity_info=entity_info,
                )
            )
            template = this_report.get_template()
            assert this_report is not None and template is not None
