from . import AggregateInterval, PvmPerformanceResultsBase
from .aggregated import PvmAggregatedPerformanceResults
import pandas as pd
from typing import List, Union
from gcm.inv.utils.pvm.investment_container import InvestmentContainerBase
from gcm.inv.dataprovider.entity_data.investment_manager.pvm.tr.gross_atom import (
    GrossAttributionAtom,
)
from functools import cached_property
from enum import Enum, auto
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
    def holding_period(self) -> float:
        as_of_date: dt.date = Scenario.get_attribute("as_of_date")
        exit_date = (
            as_of_date if self.exit_date is None else self.exit_date
        )
        hp = (exit_date - self.investment_date) / pd.Timedelta(
            "365.25 days"
        )
        return float(hp)


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
        return float(holding_period_tracker / cost_basis_tracker)

    def get_position_performance_concentration_at_layer(
        self, top=1, ascending=False, return_other=False
    ) -> tuple[
        "ReportingLayerAggregatedResults",
        "ReportingLayerAggregatedResults",
    ]:
        return ReportLayerUtils.get_concentration(
            full_expansion=self.full_expansion,
            aggregate_interval=self.aggregate_interval,
            top=top,
            ascending=ascending,
            return_other=return_other,
        )

    class DistributionType(Enum):
        AboveAtBelow_Cost = auto()

    def get_return_distributions_at_layer(
        self, distribution_type=DistributionType.AboveAtBelow_Cost
    ):
        if (
            distribution_type
            == ReportingLayerAggregatedResults.DistributionType.AboveAtBelow_Cost
        ):
            return ReportLayerUtils.get_return_distribution(
                full_expansion=self.full_expansion,
                aggregate_interval=self.aggregate_interval,
            )


class ReportLayerUtils:
    @staticmethod
    def aggregate_other(
        amount: int, df: pd.DataFrame, interval: AggregateInterval
    ) -> ReportingLayerAggregatedResults:
        other_df = df.iloc[amount:]
        if other_df.shape[0] > 0:
            tracker = []
            in_count = 1
            for i, s in other_df.iterrows():
                item: ReportingLayerBase = s["obj"]
                in_count = in_count + 1
                tracker.append(item)
            other = ReportingLayerAggregatedResults(
                "Other", sub_layers=tracker, aggregate_interval=interval
            )
            return other
        return None

    @staticmethod
    def _materialize_to_df(full_expansion: dict[str, ReportingLayerBase]):
        pnl_list = []
        item_list = []
        for i in full_expansion:
            pnl_list.append(full_expansion[i].pnl)
            item_list.append(full_expansion[i])
        df = pd.DataFrame({"pnl": pnl_list, "obj": item_list})
        return df

    @staticmethod
    def get_concentration(
        full_expansion: dict[str, ReportingLayerBase],
        aggregate_interval: AggregateInterval,
        top: int = 1,
        ascending=False,
        return_other=False,
    ) -> tuple[
        ReportingLayerAggregatedResults,
        ReportingLayerAggregatedResults,
    ]:
        df = ReportLayerUtils._materialize_to_df(
            full_expansion=full_expansion
        )
        df.sort_values(by="pnl", ascending=ascending, inplace=True)
        sorted = df.head(top)
        final = []
        for i, s in sorted.iterrows():
            final.append(s["obj"])
        # now get other if return_other
        other = None
        if return_other:
            other = ReportLayerUtils.aggregate_other(
                top, df, aggregate_interval
            )
        return [
            ReportingLayerAggregatedResults(
                f"Top {top}",
                sub_layers=final,
                aggregate_interval=aggregate_interval,
            ),
            other,
        ]

    @staticmethod
    def get_return_distribution(
        full_expansion: dict[str, PvmAggregatedPerformanceResults],
        aggregate_interval: AggregateInterval,
    ) -> ReportingLayerAggregatedResults:
        df = ReportLayerUtils._materialize_to_df(full_expansion)
        title = "PnL Bucket"

        def construct_pnl_bucket(series: pd.Series):
            if series.pnl == 0.0:
                return "At Cost"
            elif series.pnl > 0.0:
                return "Above Cost"
            elif series.pnl < 0.0:
                return "Below Cost"
            raise NotImplementedError()

        df[title] = df.apply(lambda x: construct_pnl_bucket(x), axis=1)
        cache = []
        for n, g in df.groupby(title):
            layers = []
            for i, s in g.iterrows():
                obj: ReportingLayerBase = s["obj"]
                layers.append(obj)
            item = ReportingLayerAggregatedResults(
                n,
                layers,
                aggregate_interval,
            )
            cache.append(item)
        return ReportingLayerAggregatedResults(
            "Return Distribution",
            cache,
            aggregate_interval,
        )
