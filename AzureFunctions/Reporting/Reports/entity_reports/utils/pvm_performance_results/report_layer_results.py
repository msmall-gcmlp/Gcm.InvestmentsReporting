from gcm.inv.utils.date.AggregateInterval import AggregateInterval
from AzureFunctions.Reporting.Reports.entity_reports.utils.pvm_performance_results import (
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


class ReportingLayerBase(PvmPerformanceResultsBase):
    def __init__(
        self,
        name: str,
        investment_obj: List[InvestmentContainerBase],
        performance_results: PvmPerformanceResultsBase,
    ):
        self.name = name
        self.investment_obj = investment_obj
        self.performance_results = performance_results
        cfs = performance_results.cfs
        agg = performance_results.aggregate_interval
        super().__init__(cfs, agg)

    class ReportLayerSpecificMeasure(Enum):
        investment_date = auto()

    # in theory, once you have overridden on item (performance results),
    # you should have overridden all.
    def get_measure(
        self,
        measure: Union[
            PvmPerformanceResultsBase.Measures,
            ReportLayerSpecificMeasure,
        ],
    ):
        if type(measure) == PvmPerformanceResultsBase.Measures:
            return self.performance_results.get_measure(measure)
        else:
            return super().get_measure(measure)

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
    def investment_date(self):
        atom_dimn = self.atom_dimn
        return list(atom_dimn['InvestmentDate'].unique())[0]

    @property
    def exit_date(self):
        atom_dimn = self.atom_dimn
        return list(atom_dimn['ExitDate'].unique())[0]

 


class ReportingLayerAggregatedResults(ReportingLayerBase):
    @staticmethod
    def _flatten(
        items: List[ReportingLayerBase],
    ) -> List[InvestmentContainerBase]:
        flattened = []
        for i in items:
            flattened = flattened + i.investment_obj
        return flattened

    def __init__(
        self,
        performance_results: PvmAggregatedPerformanceResults,
        sub_layers: List[ReportingLayerBase],
    ):
        invesment_objs = list(
            set(ReportingLayerAggregatedResults._flatten(sub_layers))
        )
        self.sub_layers = sub_layers
        super().__init__(
            performance_results.name, invesment_objs, performance_results
        )

    @property
    def investment_date(self):
        return 0.0


    @cached_property
    def expanded(self) -> dict[str, PvmPerformanceResultsBase]:
        return PvmAggregatedPerformanceResults._expand(
            self.performance_results
        )

    @staticmethod
    def _aggregate_other(
        amount: int, df: pd.DataFrame, interval: AggregateInterval
    ):
        other_df = df.iloc[amount:]
        if other_df.shape[0] > 0:
            other_final = {}
            in_count = 1
            for i, s in other_df.iterrows():
                other_final[f"Top {in_count}"] = s["obj"]
                in_count = in_count + 1
            other = PvmAggregatedPerformanceResults(
                f"Other [-{amount}]",
                other_final,
                interval,
            )
            return other
        return None

    def get_position_performance_concentration_at_layer(
        self, top=1, ascending=False, return_other=False
    ) -> tuple[
        PvmAggregatedPerformanceResults,
        PvmAggregatedPerformanceResults,
    ]:
        expanded = self.expanded
        pnl_list = []
        item_list = []
        for i in expanded:
            pnl_list.append(expanded[i].pnl)
            item_list.append(expanded[i])
        df = pd.DataFrame({"pnl": pnl_list, "obj": item_list})
        df.sort_values(by="pnl", ascending=ascending, inplace=True)
        sorted = df.head(top)
        final = {}
        in_count = 1
        for i, s in sorted.iterrows():
            final[f"Top {in_count}"] = s["obj"]
            in_count = in_count + 1
        # now get other if return_other
        other = None
        if return_other:
            other = ReportingLayerAggregatedResults._aggregate_other(
                top, df, self.performance_results.aggregate_interval
            )
        return [
            PvmAggregatedPerformanceResults(
                f"Top {top}",
                final,
                self.performance_results.aggregate_interval,
            ),
            other,
        ]
