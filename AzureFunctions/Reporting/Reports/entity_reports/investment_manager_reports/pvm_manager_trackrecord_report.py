from ..utils.pvm_track_record.base_pvm_tr_report import (
    BasePvmTrackRecordReport,
)
from ....core.components.report_workbook_handler import ReportWorksheet
from ....core.report_structure import (
    ReportMeta,
    EntityStandardNames,
)
from gcm.Dao.DaoRunner import AzureDataLakeDao
from ....core.report_structure import (
    EntityDomainTypes,
)
from ....core.components.report_workbook_handler import (
    ReportWorkBookHandler,
)
import pandas as pd
from ...report_names import ReportNames
import copy
from ..investment_reports.pvm_investment_trackrecord_report import (
    PvmInvestmentTrackRecordReport,
)
from functools import cached_property
from typing import List
from ..utils.pvm_performance_results.attribution import (
    PvmTrackRecordAttribution,
)
from ..utils.pvm_track_record.renderers.position_summary import (
    PositionSummarySheet,
    ReportTable,
)
from ..utils.pvm_track_record.renderers.position_concentration_1_3_5 import (
    PositionConcentration,
)
from enum import Enum, auto

# http://localhost:7071/orchestrators/ReportOrchestrator?as_of_date=2022-06-30&ReportName=PvmManagerTrackRecordReport&frequency=Once&save=True&aggregate_interval=ITD&EntityDomainTypes=InvestmentManager&EntityNames=[%22ExampleManagerName%22]


class PvmManagerTrackRecordReport(BasePvmTrackRecordReport):
    def __init__(self, report_meta: ReportMeta):
        super().__init__(
            ReportNames.PvmManagerTrackRecordReport, report_meta
        )

    @property
    def excel_template_location(self):
        return AzureDataLakeDao.BlobFileStructure(
            zone=AzureDataLakeDao.BlobFileStructure.Zone.raw,
            sources="investmentsreporting",
            entity="exceltemplates",
            path=["PvmManagerTrackRecordTemplate.xlsx"],
        )

    @cached_property
    def manager_name(self):
        manager_name: pd.DataFrame = self.report_meta.entity_info[
            [EntityStandardNames.EntityName]
        ].drop_duplicates()
        manager_name = "_".join(
            manager_name[EntityStandardNames.EntityName].to_list()
        )
        return manager_name

    @classmethod
    def level(cls):
        return EntityDomainTypes.InvestmentManager

    @property
    def child_type(self):
        return EntityDomainTypes.Investment

    @cached_property
    def children(self) -> pd.DataFrame:
        structure = self.manager_handler.manager_hierarchy_structure
        children = structure.get_entities_directly_related_by_name(
            self.child_type
        )
        return children

    @property
    def pvm_perfomance_results(self) -> PvmTrackRecordAttribution:
        investments = [
            self.children_reports[i].investment_handler
            for i in self.children_reports
        ]
        return PvmTrackRecordAttribution(investments)

    @cached_property
    def children_reports(
        self,
    ) -> dict[str, PvmInvestmentTrackRecordReport]:
        cach_dict = {}
        for g, n in self.children.groupby(EntityStandardNames.EntityName):
            meta = copy.deepcopy(self.report_meta)
            meta.entity_domain = self.child_type
            meta.entity_info = n
            this_report = PvmInvestmentTrackRecordReport(
                meta, self.manager_name
            )
            cach_dict[g] = this_report
        return cach_dict

    def assign_components(self) -> List[ReportWorkBookHandler]:
        manager_tr = ManagerTrackRecordTabRenderer(self)
        performance_concentration = PositionConcentration(self)
        m_ws = manager_tr.to_worksheet()
        report_name = self.manager_name
        final_list = [
            ReportWorkBookHandler(
                report_name,
                self.excel_template_location,
                [m_ws],
            )
        ]
        final_list = final_list + [performance_concentration.to_workbook()]
        for k, v in self.children_reports.items():
            components: List[ReportWorkBookHandler] = v.components
            final_list = final_list + components
        return final_list


class ManagerTrackRecordTabRenderer(PositionSummarySheet):
    # override
    _investment_name = "investment_name"
    _vintage = "vintage"
    _fund_size = "fund_size"

    @classmethod
    def get_standard_cols(cls):
        standard_cols = [
            cls._investment_name,
            cls._vintage,
            cls._fund_size,
            PositionSummarySheet.base_measures.full_expanded_performance_results_count.name,
            PositionSummarySheet.base_measures.cost.name,
            PositionSummarySheet.base_measures.realized_value.name,
            PositionSummarySheet.base_measures.unrealized_value.name,
            PositionSummarySheet.base_measures.total_value.name,
            PositionSummarySheet.base_measures.moic.name,
            PositionSummarySheet.base_measures.irr.name,
            PositionSummarySheet.base_measures.loss_ratio.name,
        ]
        return standard_cols

    subs_net = [
        PositionSummarySheet.base_measures.moic.name,
        PositionSummarySheet.base_measures.irr.name,
        PositionSummarySheet.base_measures.dpi.name,
    ]

    def __init__(self, report: PvmManagerTrackRecordReport):
        self.aggregated_position_summary = PositionSummarySheet(report)
        super().__init__(report)
        self.report = report

    @property
    def fund_position_summaries(self) -> dict[str, PositionSummarySheet]:
        final = {}
        for i in self.report.children_reports:
            item = self.report.children_reports[i]
            report = PositionSummarySheet(item)
            final[i] = report
        return final

    class FilterRStatus(Enum):
        all = auto()
        realized = auto()
        unrealized = auto()

    def _construct_single_df(
        self, k: str, v: PositionSummarySheet, f: FilterRStatus
    ) -> pd.DataFrame:
        cls = self.__class__
        target_cols = copy.deepcopy(cls.get_standard_cols())
        gross_df: pd.DataFrame = None
        if f == cls.FilterRStatus.all:
            gross_df = v.all_gross_investments_formatted(target_cols)
        elif f == cls.FilterRStatus.realized:
            gross_df = v.total_realized_investments_formatted(target_cols)
        elif f == cls.FilterRStatus.unrealized:
            gross_df = v.total_unrealized_investments_formatted(
                target_cols
            )
        assert gross_df is not None
        inv_name = cls._investment_name
        gross_df[inv_name] = k

        if f == cls.FilterRStatus.all:
            # add net returns
            net_fund = (
                v.report.pvm_perfomance_results.net_performance_results()
            )
            net_df = self.create_item_df(net_fund, cls.subs_net)
            net_cols = {}

            for i in cls.subs_net:
                net_cols[i] = f"net {i}"
            net_df.rename(inplace=True, columns=net_cols)
            net_df[inv_name] = k
            gross_df = pd.merge(gross_df, net_df, on=inv_name)
            target_cols = target_cols + [f"net {x}" for x in cls.subs_net]
        gross_df = self.construct_rendered_frame(gross_df, target_cols)
        return gross_df

    def _manager_fund_br_tr(self, f: FilterRStatus):
        df_cache = []
        for k, v in self.fund_position_summaries.items():
            df = self._construct_single_df(k, v, f)
            df_cache.append(df)
        final = pd.concat(df_cache)
        final.reset_index(inplace=True, drop=True)
        return final

    def full_manager_tr(self):
        return self._manager_fund_br_tr(
            ManagerTrackRecordTabRenderer.FilterRStatus.all
        )

    def full_manager_tr_total(self):
        return self._construct_single_df(
            "Total", self, ManagerTrackRecordTabRenderer.FilterRStatus.all
        )

    def unrealized_manager_tr(self):
        return self._manager_fund_br_tr(
            ManagerTrackRecordTabRenderer.FilterRStatus.unrealized
        )

    def unrealized_manager_tr_total(self):
        return self._construct_single_df(
            "Total Unrealized",
            self,
            ManagerTrackRecordTabRenderer.FilterRStatus.unrealized,
        )

    def realized_manager_tr(self):
        return self._manager_fund_br_tr(
            ManagerTrackRecordTabRenderer.FilterRStatus.realized
        )

    def realized_manager_tr_total(self):
        return self._construct_single_df(
            "Total Realized",
            self,
            ManagerTrackRecordTabRenderer.FilterRStatus.realized,
        )

    def to_worksheet(self) -> ReportWorksheet:
        d_items = {
            "full_manager_tr": self.full_manager_tr,
            "full_manager_tr_total": self.full_manager_tr_total,
            "realized_manager_tr": self.realized_manager_tr,
            "realized_manager_tr_total": self.realized_manager_tr_total,
            "unrealized_manager_tr": self.unrealized_manager_tr,
            "unrealized_manager_tr_total": self.unrealized_manager_tr_total,
        }
        to_render = []
        trim = []
        for k, v in d_items.items():
            df = v()
            i = ReportTable(k, df)
            if not k.endswith("_total"):
                trim.append(k)
            to_render.append(i)

        ws = ReportWorksheet(
            "Manager TR",
            ReportWorksheet.ReportWorkSheetRenderer(trim_region=trim),
            to_render,
        )
        return ws
