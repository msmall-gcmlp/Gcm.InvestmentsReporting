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
        total_gross = self.total_positions_line_item
        total_df = total_gross.performance_results.to_df()
        total_1_3_5_df = self.get_1_3_5_other_df(total_gross)
        realized = self.get_realation_status_positions("Realized")

        realized_expanded = realized.expanded
        if realized is not None:
            realized_df = realized.performance_results.to_df()
            realized_1_3_5_df = self.get_1_3_5_other_df(realized)
