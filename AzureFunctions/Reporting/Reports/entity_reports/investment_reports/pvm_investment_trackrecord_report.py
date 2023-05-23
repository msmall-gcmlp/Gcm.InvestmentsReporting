from gcm.inv.utils.date.AggregateInterval import AggregateInterval

from ..utils.pvm_track_record.data_handler.gross_atom import (
    GrossAttributionAtom,
)
from ....core.report_structure import (
    ReportMeta,
)
from ..utils.pvm_track_record.base_pvm_tr_report import (
    BasePvmTrackRecordReport,
)
from gcm.Dao.DaoRunner import AzureDataLakeDao
from ....core.report_structure import (
    EntityDomainTypes,
    Standards as EntityDomainStandards,
)
import pandas as pd
from ...report_names import ReportNames
from ..utils.pvm_track_record.data_handler.investment_container import (
    InvestmentContainerBase,
)
from ..utils.pvm_track_record.data_handler.pvm_track_record_handler import (
    TrackRecordHandler,
)
from functools import cached_property
from ..utils.pvm_performance_results.attribution import (
    PvmTrackRecordAttribution,
)
from ....core.components.report_workbook_handler import (
    ReportWorksheet,
    ReportWorkBookHandler,
)
from ....core.components.report_table import ReportTable
from ..utils.pvm_performance_results.report_layer_results import (
    ReportingLayerBase,
    ReportingLayerAggregatedResults,
)


class PvmInvestmentTrackRecordReport(BasePvmTrackRecordReport):
    def __init__(
        self, report_meta: ReportMeta, investment_manager_name=None
    ):
        super().__init__(
            ReportNames.PvmInvestmentTrackRecordReport, report_meta
        )
        self.___investment_manager_name = investment_manager_name

    @property
    def manager_name(self) -> str:
        if self.___investment_manager_name is None:
            # time to do acrobatics....
            e = self.related_entities
            manager_data = e.get_entities_directly_related_by_name(
                EntityDomainTypes.InvestmentManager, None, False
            )
            managers = (
                manager_data[EntityDomainStandards.EntityName]
                .drop_duplicates()
                .to_list()
            )
            if len(managers) == 1:
                self.___investment_manager_name = managers[0]
            else:
                raise RuntimeError("More than one manager")
        return self.___investment_manager_name

    class InvestmentContainer(InvestmentContainerBase):
        def __init__(
            self, manager_handler: TrackRecordHandler, idw_pvm_tr_id: int
        ):
            super().__init__()
            self.manager_handler = manager_handler
            self.idw_pvm_tr_id = idw_pvm_tr_id

        def filter_by_inv_id(self, df: pd.DataFrame):
            return df[df["InvestmentId"] == self.idw_pvm_tr_id]

        @cached_property
        def name(self) -> str:
            return list(self.investment_dimn["InvestmentName"].unique())[0]

        @cached_property
        def investment_cashflows(self) -> pd.DataFrame:
            cfs = self.manager_handler.all_inv_cfs
            cfs = self.filter_by_inv_id(cfs)
            return cfs

        @cached_property
        def investment_dimn(self) -> pd.DataFrame:
            cached_property = self.manager_handler.investment_attrib
            cached_property = self.filter_by_inv_id(cached_property)
            return cached_property

        @cached_property
        def position_cashflows(self) -> pd.DataFrame:
            cfs = self.manager_handler.position_cf
            cfs = self.filter_by_inv_id(cfs)
            return cfs

        @cached_property
        def position_dimn(self) -> pd.DataFrame:
            position_dimn = self.manager_handler.position_attrib
            position_dimn = self.filter_by_inv_id(position_dimn)
            return position_dimn

        def get_atom_level_performance_result_cache(
            self, agg: AggregateInterval
        ):
            result_set = (
                self.manager_handler.gross_atom_level_performance_cache(
                    agg
                )
            )
            atom_id = f"{self.gross_atom.name}Id"
            set = list(self.position_dimn[atom_id].unique())
            items = {}
            for p in set:
                items[p] = result_set[p]
            return items

        @property
        def gross_atom(self) -> GrossAttributionAtom:
            return GrossAttributionAtom.Position

    @property
    def excel_template_location(self):
        return AzureDataLakeDao.BlobFileStructure(
            zone=AzureDataLakeDao.BlobFileStructure.Zone.raw,
            sources="investmentsreporting",
            entity="exceltemplates",
            path=["PvmInvestmentTrackRecordTemplate.xlsx"],
        )

    @cached_property
    def investment_handler(self) -> InvestmentContainer:
        return PvmInvestmentTrackRecordReport.InvestmentContainer(
            self.manager_handler, self.idw_pvm_tr_id
        )

    @classmethod
    def level(cls):
        return EntityDomainTypes.Investment

    @cached_property
    def pvm_perfomance_results(self) -> PvmTrackRecordAttribution:
        return PvmTrackRecordAttribution([self.investment_handler])

    def assign_components(self):
        formatted = FundSummaryTabFormatter(self)
        ws = formatted.to_worksheet()
        report_name = "_".join(
            [
                self.manager_name,
                self.investment_handler.name,
            ]
        )
        return [
            ReportWorkBookHandler(
                report_name, self.excel_template_location, [ws]
            )
        ]


class FundSummaryTabFormatter(object):
    def __init__(self, report: BasePvmTrackRecordReport):
        self.report = report

    title = "title"
    _percent_total_gain = "_percent_total_gain"
    report_measures = ReportingLayerBase.ReportLayerSpecificMeasure
    base_measures = ReportingLayerBase.Measures

    @classmethod
    def assign_blank_if_not_present(cls, df: pd.DataFrame, name: str):
        df[name] = "" if name not in df.columns else df[name]
        return df

    @classmethod
    def create_item_df(
        cls,
        sub_total: ReportingLayerAggregatedResults,
        total: ReportingLayerAggregatedResults,
    ):
        df = cls.assign_title(sub_total.to_df())
        ass_lam = cls.assign_blank_if_not_present
        for i in [e.name for e in cls.report_measures]:
            df = ass_lam(df, i)
        df[cls._percent_total_gain] = (
            df[cls.base_measures.pnl.name] / total.pnl
        )
        # formatted
        selected_cols = [
            cls.title,
            cls.report_measures.investment_date.name,
            cls.report_measures.exit_date.name,
            cls.report_measures.holding_period.name,
            cls.base_measures.cost.name,
            cls.base_measures.unrealized_value.name,
            cls.base_measures.total_value.name,
            cls.base_measures.pnl.name,
            cls.base_measures.moic.name,
            cls.base_measures.loss_ratio.name,
            cls._percent_total_gain,
            cls.base_measures.irr.name,
        ]
        df = df[selected_cols]
        return df

    @classmethod
    def assign_title(cls, df: pd.DataFrame) -> pd.DataFrame:
        def get_title(x: pd.Series):
            count = getattr(
                x,
                cls.base_measures.full_expanded_performance_results_count.name,
            )
            name = getattr(x, "Name", "")
            return f"{name} ({count})"

        df[cls.title] = df.apply(lambda x: get_title(x), axis=1)
        return df

    def all_gross_investments_formatted(self) -> pd.DataFrame:
        df = self.__class__.create_item_df(
            self.total_gross, self.total_gross
        )
        return df

    @property
    def total_gross(self):
        total_gross: ReportingLayerAggregatedResults = (
            self.report.total_positions_line_item
        )
        return total_gross

    def total_realized_investments_formatted(self) -> pd.DataFrame:
        realized: ReportingLayerAggregatedResults = (
            self.report.realized_reporting_layer
        )
        df = self.__class__.create_item_df(realized, self.total_gross)
        return df

    def total_unrealized_investments_formatted(self) -> pd.DataFrame:
        unrealized: ReportingLayerAggregatedResults = (
            self.report.unrealized_reporting_layer
        )
        df = self.__class__.create_item_df(unrealized, self.total_gross)
        return df

    def get_atom_reporting_name(self, k):
        assert k is not None
        id_get = f"{self.report.manager_handler.gross_atom.name}Id"
        name_get = 'AssetName'
        df: pd.DataFrame = None
        if self.report.manager_handler.gross_atom == GrossAttributionAtom.Position:
            df = self.report.manager_handler.position_attrib
        elif self.report.manager_handler.gross_atom == GrossAttributionAtom.Asset:
            df = self.report.manager_handler.asset_attribs
            
        names = list(df[df[id_get] == k][name_get])
        names = list(set(names))
        return " & ".join(names)

    def position_breakout(
        self,
        r_type: BasePvmTrackRecordReport._KnownRealizationStatusBuckets,
    ) -> pd.DataFrame:
        item: ReportingLayerAggregatedResults = (
            self.report.realized_reporting_layer
            if r_type
            == BasePvmTrackRecordReport._KnownRealizationStatusBuckets.REALIZED
            else self.report.unrealized_reporting_layer
        )
        cache = []
        expanded = item.full_expansion
        for k, v in expanded.items():
            df = self.__class__.create_item_df(v, self.total_gross)
            df[self.__class__.title] = self.get_atom_reporting_name(k)
            cache.append(df)
        final = pd.concat(cache)
        final.reset_index(inplace=True, drop=True)
        return final

    def to_worksheet(self) -> ReportWorksheet:
        top_line = ReportTable(
            "full_fund_total1", self.all_gross_investments_formatted()
        )
        realized_total = ReportTable(
            "realized_fund_total1",
            self.total_realized_investments_formatted(),
        )
        unrealized_total = ReportTable(
            "unrealized_fund_total1",
            self.total_unrealized_investments_formatted(),
        )
        to_render = [top_line, realized_total, unrealized_total]

        # now breakout positions:
        resize_range = []
        for k in BasePvmTrackRecordReport._KnownRealizationStatusBuckets:
            range_name = f"{k.name.lower()}_fund1"
            breakout = ReportTable(range_name, self.position_breakout(k))
            to_render.append(breakout)
            resize_range.append(range_name)

        return ReportWorksheet(
            "Fund TR",
            ReportWorksheet.ReportWorkSheetRenderer(
                trim_region=resize_range
            ),
            to_render,
        )
