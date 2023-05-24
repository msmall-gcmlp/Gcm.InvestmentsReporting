from ..data_handler.gross_atom import (
    GrossAttributionAtom,
)
from ..base_pvm_tr_report import (
    BasePvmTrackRecordReport,
)
from ......core.report_structure import (
    EntityDomainTypes,
)
import pandas as pd
from ......core.components.report_workbook_handler import (
    ReportWorksheet,
)
from typing import Union, List
from ......core.components.report_table import ReportTable
from ...pvm_performance_results.report_layer_results import (
    ReportingLayerBase,
    ReportingLayerAggregatedResults,
)


class PositionSummarySheet(object):
    def __init__(self, report: BasePvmTrackRecordReport):
        self.report = report

    title = "title"
    _percent_total_gain = "_percent_total_gain"
    report_measures = ReportingLayerBase.ReportLayerSpecificMeasure
    base_measures = ReportingLayerBase.Measures

    def construct_rendered_frame(
        self, input_df: pd.DataFrame, col_list: List[str]
    ):
        cls = self.__class__
        for s in col_list:
            # blank if doesn't exist:
            input_df = cls.assign_blank_if_not_present(input_df, s)
        input_df = input_df[col_list]
        return input_df

    @classmethod
    def assign_blank_if_not_present(
        cls, df: pd.DataFrame, name: str, default_val=""
    ):
        df[name] = default_val if name not in df.columns else df[name]
        return df

    standard_cols = [
        title,
        report_measures.investment_date.name,
        report_measures.exit_date.name,
        report_measures.holding_period.name,
        base_measures.cost.name,
        base_measures.unrealized_value.name,
        base_measures.total_value.name,
        base_measures.pnl.name,
        base_measures.moic.name,
        _percent_total_gain,
        base_measures.loss_ratio.name,
        base_measures.irr.name,
    ]

    def append_relevant_info(self, df: pd.DataFrame):
        cls = self.__class__
        df[cls._percent_total_gain] = (
            df[cls.base_measures.pnl.name] / self.total_gross.pnl
        )
        return df

    def create_item_df(
        self,
        sub_total: ReportingLayerAggregatedResults,
        col_list: List[str],
    ):
        cls = self.__class__
        if sub_total is not None:
            df = cls.assign_title(sub_total.to_df())
            df = self.append_relevant_info(df)
        else:
            df = pd.DataFrame()
        return self.construct_rendered_frame(df, col_list)

    @classmethod
    def _get_title(cls, x: pd.Series):
        count = getattr(
            x,
            cls.base_measures.full_expanded_performance_results_count.name,
        )
        name = getattr(x, "Name", "")
        return f"{name} ({count})"

    @classmethod
    def assign_title(cls, df: pd.DataFrame) -> pd.DataFrame:
        df[cls.title] = df.apply(lambda x: cls._get_title(x), axis=1)
        return df

    @property
    def total_gross(self):
        total_gross: ReportingLayerAggregatedResults = (
            self.report.total_positions_line_item
        )
        return total_gross

    @property
    def total_realized(self):
        total_gross: ReportingLayerAggregatedResults = (
            self.report.realized_reporting_layer
        )
        return total_gross

    def all_gross_investments_formatted(self) -> pd.DataFrame:
        df = self.create_item_df(
            self.total_gross, self.__class__.standard_cols
        )
        return df

    def total_realized_investments_formatted(self) -> pd.DataFrame:
        df = self.create_item_df(
            self.total_realized, self.__class__.standard_cols
        )
        return df

    def total_unrealized_investments_formatted(self) -> pd.DataFrame:
        unrealized: ReportingLayerAggregatedResults = (
            self.report.unrealized_reporting_layer
        )
        df = self.create_item_df(unrealized, self.__class__.standard_cols)
        return df

    def get_atom_reporting_name(self, k: Union[int, str]):
        assert k is not None
        id_get = f"{self.report.manager_handler.gross_atom.name}Id"
        name_get = "AssetName"
        df: pd.DataFrame = None
        if (
            self.report.manager_handler.gross_atom
            == GrossAttributionAtom.Position
        ):
            df = self.report.manager_handler.position_attrib
        elif (
            self.report.manager_handler.gross_atom
            == GrossAttributionAtom.Asset
        ):
            df = self.report.manager_handler.asset_attribs

        names = list(df[df[id_get] == k][name_get])
        names = list(set(names))
        return " & ".join(names)

    def position_breakout(
        self,
        r_type: BasePvmTrackRecordReport._KnownRealizationStatusBuckets,
        sort_by: str,
    ) -> pd.DataFrame:
        item: ReportingLayerAggregatedResults = (
            self.report.realized_reporting_layer
            if r_type
            == BasePvmTrackRecordReport._KnownRealizationStatusBuckets.REALIZED
            else self.report.unrealized_reporting_layer
        )
        if item is not None:
            cache = []
            expanded = item.full_expansion
            for k, v in expanded.items():
                df = self.create_item_df(v, self.__class__.standard_cols)
                df[self.__class__.title] = self.get_atom_reporting_name(k)
                cache.append(df)
            final = pd.concat(cache)
            final.reset_index(inplace=True, drop=True)
            final.sort_values(by=sort_by, inplace=True, ascending=False)
            return final
        else:
            return pd.DataFrame()

    def total_net_results_df(self):
        results = (
            self.report.pvm_perfomance_results.net_performance_results()
        )
        results = results.to_df()
        cls = self.__class__
        written = self.report.manager_handler.manager_name
        if self.report.__class__.level() == EntityDomainTypes.Investment:
            written = [
                x.name
                for x in self.report.pvm_perfomance_results.investments
            ]
            written = list(set(written))
            written = " & ".join(written)
        results = cls.assign_blank_if_not_present(
            results, cls.title, written
        )
        selected_cols = [
            cls.title,
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            cls.base_measures.tvpi.name,
            cls.base_measures.rvpi.name,
            cls.base_measures.dpi.name,
            cls.base_measures.irr.name,
        ]
        return self.construct_rendered_frame(results, selected_cols)

    def to_worksheet(self) -> ReportWorksheet:
        d_items = {
            "fund_net_results": self.total_net_results_df,
            "full_fund_total1": self.all_gross_investments_formatted,
            "realized_fund_total1": self.total_realized_investments_formatted,
            "unrealized_fund_total1": self.total_unrealized_investments_formatted,
        }
        to_render = []
        for k, v in d_items.items():
            df = v()
            i = ReportTable(k, df)
            to_render.append(i)

        # now breakout positions:
        resize_range = []
        for k in BasePvmTrackRecordReport._KnownRealizationStatusBuckets:
            sort = self.__class__.base_measures.pnl.name
            range_name = f"{k.name.lower()}_fund1"
            breakout = ReportTable(
                range_name, self.position_breakout(k, sort_by=sort)
            )
            to_render.append(breakout)
            resize_range.append(range_name)

        return ReportWorksheet(
            "Fund TR",
            ReportWorksheet.ReportWorkSheetRenderer(
                trim_region=resize_range
            ),
            to_render,
        )
