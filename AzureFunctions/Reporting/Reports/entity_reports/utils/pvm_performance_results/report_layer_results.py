from gcm.inv.utils.date.AggregateInterval import AggregateInterval
from AzureFunctions.Reporting.Reports.entity_reports.utils.pvm_performance_results import (
    AggregateInterval,
    PvmPerformanceResultsBase,
)
from AzureFunctions.Reporting.Reports.entity_reports.utils.pvm_track_record.data_handler.investment_container import (
    InvestmentContainerBase,
)
from . import (
    PvmPerformanceResultsBase,
)
from . import AggregateInterval, PvmPerformanceResultsBase
from .aggregated import PvmAggregatedPerformanceResults
import pandas as pd
from typing import List, Union
from ..pvm_track_record.data_handler.investment_container import (
    InvestmentContainerBase,
    GrossAttributionAtom,
)
from functools import cached_property
from enum import Enum, auto
from .utils import get_concentration
from gcm.inv.scenario import Scenario
import datetime as dt


class ReportingLayerBase(PvmAggregatedPerformanceResults):
    def __init__(
        self,
        name: str,
        investment_obj: List[InvestmentContainerBase],
        components: dict[str, PvmPerformanceResultsBase],
        aggregate_interval: AggregateInterval,
    ):

        self.investment_obj = investment_obj
        super().__init__(name, components, aggregate_interval)

    class ReportLayerSpecificMeasure(Enum):
        investment_date = auto()
        exit_date = auto()
        holding_period = auto()

    def measure_cols(self):
        cols = super().measure_cols()
        cols = cols + [
            x for x in ReportingLayerBase.ReportLayerSpecificMeasure
        ]
        return cols

    @cached_property
    def atom_dimn(self) -> pd.DataFrame:
        if len(self.investment_obj) == 1:
            inv = self.investment_obj[0]
            if inv.gross_atom == GrossAttributionAtom.Position:
                position_dimn = inv.position_dimn
                filtered: pd.DataFrame = position_dimn[
                    position_dimn[f"{inv.gross_atom.name}Id"] == self.name
                ]
                return filtered

    @property
    def investment_date(self) -> dt.date:
        atom_dimn = self.atom_dimn
        return list(atom_dimn["InvestmentDate"].unique())[0]

    @property
    def exit_date(self) -> Union[dt.date, None]:
        atom_dimn = self.atom_dimn
        return list(atom_dimn["ExitDate"].unique())[0]

    @cached_property
    def holding_period(self):
        as_of_date: dt.date = Scenario.get_attribute("as_of_date")
        exit_date = (
            as_of_date if self.exit_date is None else self.exit_date
        )
        hp = (exit_date - self.investment_date) / 365.25
        return hp


class ReportingLayerAggregatedResults(ReportingLayerBase):
    def __init__(
        self,
        name: str,
        sub_layers: List[ReportingLayerBase],
        aggregate_interval: AggregateInterval,
    ):
        performance_results: dict[str, ReportingLayerBase] = {}
        flattened = []
        for i in sub_layers:
            performance_results[i.name] = i
            flattened = flattened + i.investment_obj
            flattened = list(set(flattened))
        super().__init__(
            name,
            investment_obj=flattened,
            components=performance_results,
            aggregate_interval=aggregate_interval,
        )
        self.sub_layers = sub_layers

    @property
    def investment_date(self):
        return None

    @property
    def exit_date(self):
        return None

    @classmethod
    def get_lowest_expansion_types(cls):
        return [ReportingLayerBase]

    @cached_property
    def holding_period(self):
        cost_basis_tracker = 0.0
        holding_period_tracker = 0.0
        for i in self.full_expansion:
            item: ReportingLayerBase = self.full_expansion[i]
            holding_period_tracker = holding_period_tracker + (
                item.holding_period * item.cost
            )
            cost_basis_tracker = cost_basis_tracker + item.cost
        return holding_period_tracker / cost_basis_tracker

    def get_position_performance_concentration_at_layer(
        self, top=1, ascending=False, return_other=False
    ) -> tuple[
        PvmAggregatedPerformanceResults,
        PvmAggregatedPerformanceResults,
    ]:
        return get_concentration(
            full_expansion=self.full_expansion,
            aggregate_interval=self.aggregate_interval,
            top=top,
            ascending=ascending,
            return_other=return_other,
        )
