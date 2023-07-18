from . import AggregateInterval, PvmPerformanceResultsBase
import pandas as pd
from typing import List
from gcm.inv.utils.pvm.investment_container import InvestmentContainerBase
from gcm.inv.dataprovider.entity_data.investment_manager.pvm.tr.gross_atom import (
    GrossAttributionAtom,
)
from functools import cached_property
from .report_layer_results import (
    ReportingLayerAggregatedResults,
    ReportingLayerBase,
)


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

    def evaluate_base(self, depth: int) -> tuple[bool, str]:
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
        return (is_base_case, attribution_item)

    def generate_base_layer(
        self, n: object, g: pd.DataFrame
    ) -> ReportingLayerBase:
        investment_names = list(g["InvestmentName"].unique())
        if len(investment_names) == 1:
            investment = [
                x
                for x in self.investment_containers
                if x.name in investment_names
            ]
            single_component: dict[str, PvmPerformanceResultsBase] = {
                n: self.get_position_results(
                    investment_names[0],
                    self.aggregate_interval,
                    n,
                )
            }
            layer = ReportingLayerBase(
                name=n,
                investment_obj=investment,
                components=single_component,
                aggregate_interval=self.aggregate_interval,
            )
            return layer
        else:
            raise RuntimeError()

    def results(
        self,
        depth=0,
        parent_frame: pd.DataFrame = None,
        name: str = None,
    ) -> ReportingLayerAggregatedResults:
        [is_base_case, attribution_item] = self.evaluate_base(depth)
        if parent_frame is None:
            parent_frame = self.merged_position_results
        if name is None:
            name = "Total"
        grouped = parent_frame.groupby(attribution_item)
        layer_cache: List[ReportingLayerBase] = []
        for n, g in grouped:
            # now we can get the cfs for in-group-items
            if is_base_case:
                layer = self.generate_base_layer(n, g)
                layer_cache.append(layer)
            else:
                child_depth = depth + 1
                sub_item = self.results(
                    child_depth, g, name=f"{attribution_item} - {n}"
                )
                layer_cache.append(sub_item)
        return ReportingLayerAggregatedResults(
            name=name,
            sub_layers=layer_cache,
            aggregate_interval=self.aggregate_interval,
        )
