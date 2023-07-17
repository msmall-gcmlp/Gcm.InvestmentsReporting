from ...pvm_track_record.base_pvm_tr_report import (
    BasePvmTrackRecordReport,
    ReportingLayerAggregatedResults,
)
from .position_summary import (
    PositionSummarySheet,
    ReportWorksheet,
    ReportTable,
)
import pandas as pd
from gcm.Dao.DaoRunner import AzureDataLakeDao
from functools import cached_property
from ......core.components.report_workbook_handler import (
    ReportWorkBookHandler,
)


class PositionConcentration(PositionSummarySheet):
    top_deal_count = 5
    _percent_realized_gain = "percent realized gain"
    _percent_unrealized_gain = "percent unrealized gain"
    _percent_capital = "percent_in_group_capital"

    concentration_columns = [
        PositionSummarySheet.title,
        PositionSummarySheet.base_measures.irr.name,
        PositionSummarySheet.base_measures.moic.name,
        _percent_realized_gain,
        PositionSummarySheet._percent_total_gain,
    ]

    distribution_columns = [
        PositionSummarySheet.title,
        PositionSummarySheet.base_measures.full_expanded_performance_results_count.name,
        PositionSummarySheet.base_measures.moic.name,
        PositionSummarySheet.base_measures.irr.name,
        _percent_capital,
    ]

    # override
    def append_relevant_info(self, df: pd.DataFrame):
        df = super().append_relevant_info(df)
        cls = self.__class__
        if self.total_realized is not None:
            df[cls._percent_realized_gain] = (
                df[cls.base_measures.pnl.name] / self.total_realized.pnl
            )
        else:
            df[cls._percent_realized_gain] = 0.0
        return df

    # override
    @classmethod
    def get_standard_cols(cls):
        standard_cols = [
            PositionSummarySheet.title,
            PositionSummarySheet.report_measures.investment_date.name,
            PositionSummarySheet.report_measures.exit_date.name,
            PositionSummarySheet.base_measures.cost.name,
            PositionSummarySheet.base_measures.unrealized_value.name,
            PositionSummarySheet.base_measures.total_value.name,
            PositionSummarySheet.base_measures.pnl.name,
            PositionSummarySheet.base_measures.moic.name,
            PositionSummarySheet.base_measures.irr.name,
            cls._percent_realized_gain,
            PositionSummarySheet._percent_total_gain,
        ]
        return standard_cols

    # override
    @classmethod
    def _get_title(cls, x: pd.Series):
        name = getattr(x, "Name", "")
        if type(name) == str:
            if name.upper().startswith("TOP "):
                return name
        return super()._get_title(x)

    def __init__(self, report: BasePvmTrackRecordReport):
        super().__init__(report)

    @property
    def excel_template_location(self):
        return AzureDataLakeDao.BlobFileStructure(
            zone=AzureDataLakeDao.BlobFileStructure.Zone.raw,
            sources="investmentsreporting",
            entity="exceltemplates",
            path=["PvmTrackRecordPerformanceConcentration.xlsx"],
        )

    @cached_property
    def total_1_3_5_other(
        self,
    ) -> dict[object, ReportingLayerAggregatedResults]:
        return self.report.performance_details._1_3_5_objects(
            self.report.total_positions_line_item
        )

    @cached_property
    def realized_1_3_5_other(
        self,
    ) -> dict[object, ReportingLayerAggregatedResults]:
        item = self.report.performance_details._1_3_5_objects(
            self.report.realized_reporting_layer
        )
        return item

    @cached_property
    def unrealized_1_3_5_other(
        self,
    ) -> dict[object, ReportingLayerAggregatedResults]:
        item = self.report.performance_details._1_3_5_objects(
            self.report.unrealized_reporting_layer
        )
        return item

    @cached_property
    def total_performance_distribution(
        self,
    ) -> ReportingLayerAggregatedResults:
        return (
            self.report.performance_details.get_performance_distribution(
                self.report.total_positions_line_item
            )
        )

    @cached_property
    def realized_performance_distribution(
        self,
    ) -> ReportingLayerAggregatedResults:
        return (
            self.report.performance_details.get_performance_distribution(
                self.report.realized_reporting_layer
            )
        )

    @cached_property
    def unrealized_performance_distribution(
        self,
    ) -> ReportingLayerAggregatedResults:
        return (
            self.report.performance_details.get_performance_distribution(
                self.report.unrealized_reporting_layer
            )
        )

    def top_in_group_deals_df(
        self,
        deal_set: pd.DataFrame,
        set_1_3_5: dict[object, ReportingLayerAggregatedResults],
    ) -> pd.DataFrame:
        cls = self.__class__
        filtered_deal_set = deal_set.head(cls.top_deal_count)
        other_key = int(-1 * cls.top_deal_count)
        if other_key in set_1_3_5:
            other = set_1_3_5[other_key]
            other = self.create_item_df(
                other, self.__class__.get_standard_cols()
            )
            filtered_deal_set = pd.concat([filtered_deal_set, other])
        return self.construct_rendered_frame(
            filtered_deal_set, self.__class__.get_standard_cols()
        )

    @staticmethod
    def _format_total_buckets(df: pd.DataFrame) -> pd.DataFrame:
        if PositionConcentration._percent_realized_gain in df.columns:
            df[PositionConcentration._percent_realized_gain] = ""
        return df

    def _get_all_deals(self):
        sort_by = PositionSummarySheet.base_measures.pnl.name
        realized_df = self.position_breakout(
            BasePvmTrackRecordReport._KnownRealizationStatusBuckets.REALIZED,
            sort_by,
        )
        unrealized_df = self.position_breakout(
            BasePvmTrackRecordReport._KnownRealizationStatusBuckets.UNREALIZED,
            sort_by,
        )
        all_deals: pd.DataFrame = pd.concat([realized_df, unrealized_df])
        return all_deals

    def top_all_deals_df(self) -> pd.DataFrame:
        all_deals = self._get_all_deals()
        df = self.top_in_group_deals_df(all_deals, self.total_1_3_5_other)
        df = PositionConcentration._format_total_buckets(df)
        return df

    def top_realized_deals_df(self):
        sort_by = PositionSummarySheet.base_measures.pnl.name
        realized_df = self.position_breakout(
            BasePvmTrackRecordReport._KnownRealizationStatusBuckets.REALIZED,
            sort_by,
        )
        return self.top_in_group_deals_df(
            realized_df, self.realized_1_3_5_other
        )

    def top_unrealized_deals_df(self):
        sort_by = PositionSummarySheet.base_measures.pnl.name
        unrealized_df = self.position_breakout(
            BasePvmTrackRecordReport._KnownRealizationStatusBuckets.UNREALIZED,
            sort_by,
        )
        return self.top_in_group_deals_df(
            unrealized_df, self.unrealized_1_3_5_other
        )

    def generate_concentration_item(
        self, obj_1_3_5: dict[str, ReportingLayerAggregatedResults]
    ):
        target_cols = PositionConcentration.concentration_columns
        df_cache = []
        for i, v in obj_1_3_5.items():
            if v is not None:
                df = self.create_item_df(v, target_cols)
                df_cache.append(df)
        if len(df_cache) > 0:
            final = pd.concat(df_cache)
            final.reset_index(inplace=True, drop=True)
            final = self.construct_rendered_frame(final, target_cols)
            return final
        else:
            df = pd.DataFrame()
            return df

    def all_concen(self):
        total = self.generate_concentration_item(self.total_1_3_5_other)
        total = PositionConcentration._format_total_buckets(total)
        return total

    def all_concen_total(self):
        total = self.all_gross_investments_formatted()
        total = self.construct_rendered_frame(
            total, PositionConcentration.concentration_columns
        )
        total = PositionConcentration._format_total_buckets(total)
        return total

    def realized_concen(self):
        total = self.generate_concentration_item(self.realized_1_3_5_other)
        return total

    def unrealized_concen(self):
        total = self.generate_concentration_item(
            self.unrealized_1_3_5_other
        )
        return total

    def realized_concen_total(self):
        total = self.total_realized_investments_formatted()
        total = self.construct_rendered_frame(
            total, PositionConcentration.concentration_columns
        )
        return total

    def unrealized_concen_total(self):
        total = self.total_unrealized_investments_formatted()
        total = self.construct_rendered_frame(
            total, PositionConcentration.concentration_columns
        )
        return total

    def _generate_distribution_frame(
        self, top_line: ReportingLayerAggregatedResults, expand_down=True
    ):
        cls = self.__class__
        if top_line is not None:

            def _generate(
                item: ReportingLayerAggregatedResults,
            ) -> pd.DataFrame:
                df = self.create_item_df(item, cls.get_standard_cols())
                df[cls._percent_capital] = (
                    df[cls.base_measures.cost.name] / top_line.cost
                )
                df = self.construct_rendered_frame(
                    df, cls.distribution_columns
                )
                return df

            if expand_down:
                cache = []
                for k in top_line.sub_layers:
                    df = _generate(k)
                    cache.append(df)
                final = pd.concat(cache)
                final.reset_index(inplace=True, drop=True)
                return final
            else:
                df = _generate(top_line)
                return df
        return pd.DataFrame()

    def all_distrib(self):
        total = self.total_performance_distribution
        final = self._generate_distribution_frame(total)
        return final

    def all_distrib_total(self):
        total = self.total_performance_distribution
        final = self._generate_distribution_frame(total, expand_down=False)
        return final

    def realized_distrib(self):
        total = self.realized_performance_distribution
        final = self._generate_distribution_frame(total)
        return final

    def unrealized_distrib(self):
        total = self.unrealized_performance_distribution
        final = self._generate_distribution_frame(total)
        return final

    def realized_distrib_total(self):
        total = self.realized_performance_distribution
        final = self._generate_distribution_frame(total, expand_down=False)
        return final

    def unrealized_distrib_total(self):
        total = self.unrealized_performance_distribution
        final = self._generate_distribution_frame(total, expand_down=False)
        return final

    def to_worksheet(self) -> ReportWorksheet:
        d_items = {
            "top_deals": self.top_all_deals_df,
            "top_deals_total": self.all_gross_investments_formatted,
            "top_realized_deals": self.top_realized_deals_df,
            "top_realized_deals_total": self.total_realized_investments_formatted,
            "all_concen": self.all_concen,
            "all_concen_total": self.all_concen_total,
            "realized_concen": self.realized_concen,
            "realized_concen_total": self.realized_concen_total,
            "all_distrib": self.all_distrib,
            "all_distrib_total": self.all_distrib_total,
            "realized_distrib": self.realized_distrib,
            "realized_distrib_total": self.realized_distrib_total,
            ## unrealized
            "top_unrealized_deals": self.top_unrealized_deals_df,
            "top_unrealized_deals_total": self.total_unrealized_investments_formatted,
            "unrealized_concen": self.unrealized_concen,
            "unrealized_concen_total": self.unrealized_concen_total,
            "unrealized_distrib": self.unrealized_distrib,
            "unrealized_distrib_total": self.unrealized_distrib_total,
        }
        to_render = []
        for k, v in d_items.items():
            df = v()
            if k == "top_deals_total":
                PositionConcentration._format_total_buckets(df)

            i = ReportTable(k, df)
            to_render.append(i)
        return ReportWorksheet(
            "Performance Concentration",
            ReportWorksheet.ReportWorkSheetRenderer(),
            to_render,
        )

    def to_workbook(self) -> ReportWorkBookHandler:

        r_name = [self.__class__.__name__, self.report.level().name]
        r_name = "_".join(r_name)
        ws = self.to_worksheet()
        wb = ReportWorkBookHandler(
            r_name,
            self.excel_template_location,
            [ws],
        )
        return wb
