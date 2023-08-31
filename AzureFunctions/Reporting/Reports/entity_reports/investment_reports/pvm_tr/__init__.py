from .....core.report_structure import ReportMeta
from ...xentity_reports.pvm_tr import BasePvmTrackRecordReport
from .....core.report_structure import (
    EntityDomainTypes,
)
from ....report_names import ReportNames
from gcm.inv.entityhierarchy.NodeHierarchy import (
    Standards as EntityStandardNames,
)
from typing import List
from functools import cached_property
from .....core.components.report_workbook_handler import (
    ReportWorkBookHandler,
)
from gcm.Dao.DaoRunner import AzureDataLakeDao
from gcm.inv.utils.pvm.standard_mappings import (
    ReportedRealizationStatus,
)
from ...xentity_reports.pvm_tr.render_breakout import (
    RenderRealizationStatusFundBreakout_NetGross,
)
from gcm.inv.utils.pvm.node import PvmNodeBase


class PvmInvestmentTrackRecordReport(BasePvmTrackRecordReport):
    def __init__(
        self, report_meta: ReportMeta, investment_manager_name: str = None
    ):
        super().__init__(
            ReportNames.PvmInvestmentTrackRecordReport, report_meta
        )
        self.___investment_manager_name = investment_manager_name

    @property
    def excel_template_location(self):
        return AzureDataLakeDao.BlobFileStructure(
            zone=AzureDataLakeDao.BlobFileStructure.Zone.raw,
            sources="investmentsreporting",
            entity="exceltemplates",
            path=["PvmInvestmentTrackRecordTemplate.xlsx"],
        )

    @cached_property
    def investments(self) -> List[str]:
        df = self.report_meta.entity_info
        entity_names = list(
            df[EntityStandardNames.EntityName].dropna().unique()
        )
        assert len(entity_names) == 1
        return entity_names

    def generate_position_breakout(self):
        asset_name = "AssetName"
        investment_name = "InvestmentName"
        gross_realized_status_breakout = self.position_node_provider.generate_evaluatable_node_hierarchy(
            [ReportedRealizationStatus, asset_name]
        )
        net = self.investment_node_provider.generate_evaluatable_node_hierarchy(
            [investment_name]
        )
        ref_items = self.position_node_provider.atomic_dimensions[
            [asset_name, "InvestmentDate", "ExitDate"]
        ]
        # because reported in mm
        ref_items = ref_items.rename(
            columns={asset_name: PvmNodeBase._DISPLAY_NAME}
        )
        i = RenderRealizationStatusFundBreakout_NetGross(
            gross_realization_status_breakout=gross_realized_status_breakout,
            net_breakout=net,
            name="Fund TR",
            dimn=ref_items,
            exclude_nulls=True,
            append_net_to_gross=False,
            extended_gross_table=True,
            enhance_display_name_with_count_and_drop_atomic_count=True,
        ).render()
        wb = ReportWorkBookHandler(
            "Summary",
            self.excel_template_location,
            report_sheets=[i],
            short_name=str(self.idw_pvm_tr_id),
        )
        return [wb]

    @cached_property
    def manager_name(self) -> str:
        if self.___investment_manager_name is None:
            # time to do acrobatics....
            e = self.related_entities
            manager_data = e.get_entities_directly_related_by_name(
                EntityDomainTypes.InvestmentManager, None, False
            )
            managers = (
                manager_data[EntityStandardNames.EntityName]
                .drop_duplicates()
                .to_list()
            )
            if len(managers) == 1:
                self.___investment_manager_name = managers[0]
            else:
                raise RuntimeError("More than one manager")
        return self.___investment_manager_name

    @classmethod
    def level(cls):
        return EntityDomainTypes.Investment

    def assign_components(self) -> List[ReportWorkBookHandler]:
        items = self.generate_position_breakout()
        return items
