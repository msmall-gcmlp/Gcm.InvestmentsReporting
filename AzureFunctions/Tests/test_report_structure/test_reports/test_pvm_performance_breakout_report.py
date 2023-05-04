from Reporting.Reports.entity_reports.utils.pvm_performance_utils.pvm_performance_helper import (
    PvmPerformanceHelper,
    EntityDomainTypes,
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
