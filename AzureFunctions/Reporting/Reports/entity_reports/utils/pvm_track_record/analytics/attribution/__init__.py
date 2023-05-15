from ...data_handler.investment_container import InvestmentContainerBase
from typing import List
from functools import cached_property
import pandas as pd
from enum import Enum, auto
from gcm.inv.utils.date.AggregateInterval import AggregateInterval


class PvmPerformanceResults(object):
    def __init__(
        self,
        cleaned_cashflows: pd.DataFrame,
        aggregate_interval: AggregateInterval,
    ):
        self.cleaned_cashflows = cleaned_cashflows
        self.aggregate_interval = aggregate_interval

    @cached_property
    def irr(self):
        pass

    @cached_property
    def moic(self):
        pass

    @cached_property
    def tvpi(self):
        return self.moic

    @cached_property
    def total_investment_gain(self):
        return sum(self.cleaned_cashflows["AmountUSD"])


class PvmPerformanceContribution(object):
    def __init__(self, performance_results: List[PvmPerformanceResults]):
        self.performance_results = performance_results

    @cached_property
    def contribution(self):
        total_pnl = sum(
            [x.total_investment_gain for x in self.performance_results]
        )
        final = []
        for k in self.performance_results:
            final.append(k.total_investment_gain / total_pnl)
        return final


class PvmTrackRecordAttribution(object):
    def __init__(self, investments: List[InvestmentContainerBase]):
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
        ) -> PvmPerformanceContribution:
            pass

        def get_performance_details(self):
            pass

    def run_position_attribution(
        self,
        run_attribution_levels_by: List[str] = ["RealizationStatus"],
        aggregate_interval: AggregateInterval = AggregateInterval.ITD,
        position_cashflows=None,
        position_dimn=None,
        position_fact=None,
    ) -> AttributionResults:
        if position_cashflows is None:
            position_cashflows: List[pd.DataFrame] = [
                x.position_cashflows for x in self.investments
            ]
            position_cashflows = pd.concat(position_cashflows)
        if position_dimn is None:
            position_dimn: List[pd.DataFrame] = [
                x.position_dimn for x in self.investments
            ]
            position_dimn = pd.concat(position_dimn)
        if position_fact is None:
            position_fact: List[pd.DataFrame] = [
                x.position_fact for x in self.investments
            ]
            position_fact = pd.concat(position_dimn)
        return PvmTrackRecordAttribution.AttributionResults(
            position_cashflows=position_cashflows,
            position_dimn=position_dimn,
            position_fact=position_fact,
            aggregate_interval=aggregate_interval,
            attribute_by=run_attribution_levels_by,
        )

    @property
    def top_line_performance_statistics(self) -> PvmPerformanceResults:
        pass
