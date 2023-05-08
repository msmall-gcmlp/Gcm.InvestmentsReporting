from ...data_handler.investment_container import InvestmentContainerBase
from typing import List
from functools import cached_property
import pandas as pd


class PvmPerformanceResults(object):
    def __init__(self, cleaned_cashflows: pd.DataFrame):
        self.cleaned_cashflows = cleaned_cashflows

    @cached_property
    def irr(self):
        pass

    @cached_property
    def moic(self):
        pass

    @cached_property
    def tvpi(self):
        return self.moic


class PvmTrackRecordAttribution(object):
    def __init__(self, investments: List[InvestmentContainerBase]):
        self.investments = investments

    class AttributionResults(object):
        def __init__(self):
            pass

    def run_position_attribution(
        self, run_attribution_levels_by: List[str]
    ) -> AttributionResults:
        position_cashflows: List[pd.DataFrame] = [
            x.position_cashflows for x in self.investments
        ]
        position_cashflows = pd.concat(position_cashflows)

    @property
    def top_line_performance_statistics(self) -> PvmPerformanceResults:
        pass
