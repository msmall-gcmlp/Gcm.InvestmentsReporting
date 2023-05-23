from ...utils.pvm_performance_results import (
    PvmPerformanceResultsBase,
)
from . import AggregateInterval, PvmPerformanceResultsBase
from .aggregated import PvmAggregatedPerformanceResults
import pandas as pd
from typing import List
from ..pvm_track_record.data_handler.investment_container import (
    InvestmentContainerBase,
)
from functools import cached_property
from ..pvm_track_record.data_handler.gross_atom import GrossAttributionAtom


class PositionAttributionResults(object):
    def __init__(
        self,
        investment_containers: List[InvestmentContainerBase],
        aggregate_interval: AggregateInterval,
        attribute_by: List[str],
        gross_atom: GrossAttributionAtom,
    ):
        self.investment_containers = investment_containers
        self.aggregate_interval = aggregate_interval
        self.attribute_by = attribute_by
        self.gross_atom = gross_atom

    # Could this be a subclass of PvmPerformanceResult Base? I think so..
    class LayerBase(object):
        def __init__(
            self, name: str, performance_results: PvmPerformanceResultsBase
        ):
            self.performance_results = performance_results
            self.name = name

    class LayerResults(LayerBase):
        def __init__(
            self,
            performance_results: PvmAggregatedPerformanceResults,
            sub_layers: List["PositionAttributionResults.LayerBase"],
        ):
            super().__init__(performance_results.name, performance_results)
            self.sub_layers = sub_layers

        @cached_property
        def expanded(self) -> dict[str, PvmPerformanceResultsBase]:
            return PvmAggregatedPerformanceResults._expand(
                self.performance_results
            )

        def get_position_performance_concentration_at_layer(
            self, top=1, ascending=False, return_other=False
        ) -> tuple[
            PvmAggregatedPerformanceResults,
            PvmAggregatedPerformanceResults,
        ]:
            expanded = self.expanded

            pnl_list = []
            item_list = []
            name_list = []
            for i in expanded:
                pnl_list.append(expanded[i].pnl)
                item_list.append(expanded[i])
                name_list.append(i)
            df = pd.DataFrame({"pnl": pnl_list, "obj": item_list})
            df.sort_values(by="pnl", ascending=ascending, inplace=True)
            sorted = df.head(top)
            final = {}
            in_count = 1
            for i, s in sorted.iterrows():
                final[f"Top {in_count}"] = s["obj"]
                in_count = in_count + 1
            # now get other if return_other

            # if there is 100 objects, shape == 100
            other = None
            if return_other:
                other_df = df.iloc[top:]
                if other_df.shape[0] > 0:
                    other_final = {}
                    in_count = 1
                    for i, s in other_df.iterrows():
                        other_final[f"Top {in_count}"] = s["obj"]
                        in_count = in_count + 1
                    other = PvmAggregatedPerformanceResults(
                        f"Other [-{top}]",
                        other_final,
                        self.performance_results.aggregate_interval,
                    )
            return [
                PvmAggregatedPerformanceResults(
                    f"Top {top}",
                    final,
                    self.performance_results.aggregate_interval,
                ),
                other,
            ]

    @cached_property
    def merged_position_results(self) -> pd.DataFrame:
        final_cfs = []
        for i in self.investment_containers:
            merge_on = ["Position", "Asset", "Investment"]
            atts = ["Id", "Name"]
            final = []
            for m in merge_on:
                for a in atts:
                    f = f"{m}{a}"
                    if (
                        f in i.position_cashflows.columns
                        and f in i.position_dimn.columns
                    ):
                        final.append(f)
            merged = pd.merge(
                i.position_cashflows,
                i.position_dimn,
                on=final,
                how="left",
            )
            if "InvestmentName" not in merged.columns:
                merged["InvestmentName"] = i.name
            final_cfs.append(merged)
        final_cfs = pd.concat(final_cfs)
        final_cfs.reset_index(inplace=True, drop=True)
        return final_cfs

    def get_position_results(
        self,
        investment_name: str,
        aggregate_interval: AggregateInterval,
        position_id: object,
    ) -> PvmPerformanceResultsBase:
        target_inv = [
            x
            for x in self.investment_containers
            if x.name == investment_name
        ][0]
        results = target_inv.get_atom_level_performance_result_cache(
            aggregate_interval
        )
        this_position_results = results[position_id]
        return this_position_results

    def results(
        self,
        depth=0,
        parent_frame: pd.DataFrame = None,
        name: str = None,
    ) -> "LayerResults":
        is_base_case = False

        if depth >= len(self.attribute_by):
            if depth == len(self.attribute_by):
                attribution_item = f"{self.gross_atom.name}Id"
                is_base_case = True
            else:
                # you have run out of attribution items
                raise RuntimeError()
        else:
            attribution_item = self.attribute_by[depth]
        if parent_frame is None:
            parent_frame = self.merged_position_results
        if name is None:
            name = "Total"
        grouped = parent_frame.groupby(attribution_item)
        layer_cache: List[PositionAttributionResults.LayerBase] = []
        for n, g in grouped:
            # now we can get the cfs for in-group-items
            if is_base_case:
                investment_name = list(g["InvestmentName"].unique())
                if len(investment_name) == 1:
                    item: PvmPerformanceResultsBase = (
                        self.get_position_results(
                            investment_name[0],
                            self.aggregate_interval,
                            n,
                        )
                    )
                    layer = PositionAttributionResults.LayerBase(n, item)
                    layer_cache.append(layer)
            else:
                child_depth = depth + 1
                sub_item = self.results(
                    child_depth, g, name=f"{attribution_item} - {n}"
                )
                layer_cache.append(sub_item)
        dict_to_move = {}
        for i in [x for x in layer_cache]:
            dict_to_move[i.name] = i.performance_results
        return PositionAttributionResults.LayerResults(
            performance_results=PvmAggregatedPerformanceResults(
                name=name,
                components=dict_to_move,
                aggregate_interval=self.aggregate_interval,
            ),
            sub_layers=layer_cache,
        )
