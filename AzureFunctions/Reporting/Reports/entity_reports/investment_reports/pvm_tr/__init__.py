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
from gcm.inv.models.pvm.node_evaluation.evaluation_provider.from_.pvm_track_record import (
    PvmTrackRecordNodeProvider,
    PvmEvaluationProvider,
)
from gcm.Dao.DaoRunner import AzureDataLakeDao


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

    @property
    def attribution_items(self) -> List[str]:
        return [""]

    @cached_property
    def investments(self) -> List[str]:
        df = self.report_meta.entity_info
        entity_names = list(
            df[EntityStandardNames.EntityName].dropna().unique()
        )
        assert len(entity_names) == 1
        return entity_names

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
        items= self.investment_node_provider.generate_evaluatable_node_hierarchy(["InvestmentName"])
        assert items is not None
