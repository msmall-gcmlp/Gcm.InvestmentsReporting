from . import PvmPerformanceResultsBase, AggregateInterval, PvmCashflows
from .aggregated import PvmAggregatedPerformanceResults
import pandas as pd
from typing import List
from ..pvm_track_record.data_handler.investment_container import (
    InvestmentContainerBase,
)
from enum import Enum, auto


class PvmTrackRecordAttribution(object):
    def __init__(self, investments: list[InvestmentContainerBase]):
        self.investments = investments

    class AttributionResults(object):
        def __init__(
            self,
            position_cashflows: pd.DataFrame,
            position_dimn: pd.DataFrame,
            position_fact: pd.DataFrame,
            aggregate_interval: AggregateInterval,
            attribute_by: List[str],
        ):
            self.position_cashflows = position_cashflows
            self.position_dimn = position_dimn
            self.position_fact = position_fact
            self.aggregate_interval = aggregate_interval
            self.attribute_by = attribute_by

        class PerformanceConcentrationType(Enum):
            One_Three_Five = auto()

        def performance_concentration(
            self, run_on: PerformanceConcentrationType
        ) -> dict[str, PvmPerformanceResultsBase]:
            # for each thing that we run attribution on
            #   (example, RealizationStatus should give us "Realized","Unrealized","Partially Realized")
            # AND an "ALL"
            # run 1-3-5 analysis for each group
            pass

        def get_performance_details(
            self,
        ) -> dict[str, PvmPerformanceResultsBase]:
            # dict: results of realized/unrealized/all
            pass

    def run_position_attribution(
        self,
        run_attribution_levels_by: List[str] = ["RealizationStatus"],
        aggregate_interval: AggregateInterval = AggregateInterval.ITD,
    ) -> AttributionResults:
        position_cashflows: List[pd.DataFrame] = [
            x.position_cashflows for x in self.investments
        ]
        position_cashflows = pd.concat(position_cashflows)
        position_dimn: List[pd.DataFrame] = [
            x.position_dimn for x in self.investments
        ]
        position_dimn = pd.concat(position_dimn)
        return PvmTrackRecordAttribution.AttributionResults(
            position_cashflows=position_cashflows,
            position_dimn=position_dimn,
            aggregate_interval=aggregate_interval,
            attribute_by=run_attribution_levels_by,
        )

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
