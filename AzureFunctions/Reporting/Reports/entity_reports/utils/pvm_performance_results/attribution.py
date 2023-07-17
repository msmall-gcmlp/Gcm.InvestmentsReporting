from . import PvmPerformanceResultsBase, AggregateInterval, PvmCashflows
from .aggregated import PvmAggregatedPerformanceResults
from typing import List
from gcm.inv.utils.pvm.investment_container import InvestmentContainerBase
from .position_attribution_results import PositionAttributionResults


class PvmTrackRecordAttribution(object):
    def __init__(self, investments: list[InvestmentContainerBase]):
        self.investments = investments

    def position_attribution(
        self,
        run_attribution_levels_by: List[str] = ["Industry category"],
        aggregate_interval: AggregateInterval = AggregateInterval.ITD,
    ) -> PositionAttributionResults:
        atom_type = list(
            set(
                [
                    i.gross_atom
                    for i in self.investments
                    if i.gross_atom is not None
                ]
            )
        )
        if len(atom_type) == 1:
            return PositionAttributionResults(
                investment_containers=self.investments,
                aggregate_interval=aggregate_interval,
                attribute_by=run_attribution_levels_by,
                gross_atom=atom_type[0],
            )
        else:
            raise RuntimeError()

    def net_performance_results(
        self, aggregate_interval: AggregateInterval = AggregateInterval.ITD
    ) -> PvmPerformanceResultsBase:
        # first do aggregated:
        items = {}
        for i in self.investments:
            net_cfs = PvmCashflows(i.investment_cashflows)
            results = PvmPerformanceResultsBase(net_cfs)
            items[i.name] = results
        aggregated = PvmAggregatedPerformanceResults(
            "Total", items, aggregate_interval
        )
        return aggregated
