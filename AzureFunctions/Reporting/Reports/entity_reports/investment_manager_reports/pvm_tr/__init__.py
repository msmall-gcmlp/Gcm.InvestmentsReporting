from ...xentity_reports.pvm_tr import (
    BasePvmTrackRecordReport,
)
from .....core.report_structure import (
    ReportMeta,
    EntityStandardNames,
)
from gcm.Dao.DaoRunner import AzureDataLakeDao
from .....core.report_structure import (
    EntityDomainTypes,
)
from .....core.components.report_workbook_handler import (
    ReportWorkBookHandler,
    ReportWorksheet,
)
import pandas as pd
from ....report_names import ReportNames
from functools import cached_property
from typing import List
from ...xentity_reports.pvm_tr.render_135 import (
    OneThreeFiveRenderer,
    TEMPLATE as Template_135,
)
from gcm.inv.utils.pvm.node import PvmNodeBase
from gcm.inv.models.pvm.underwriting_analytics.perf_1_3_5 import (
    generate_realized_unrealized_all_performance_breakout,
)
from ...xentity_reports.pvm_tr.render_attribution import (
    RenderAttribution,
    TEMPLATE as Template_Attribution,
)
from gcm.inv.utils.pvm.standard_mappings import (
    ReportedRealizationStatus,
)
from .render_manager_report import (
    RenderRealizationStatusFundBreakout_NetGross,
    TEMPLATE as SummaryTemplate,
)

# http://localhost:7071/orchestrators/ReportOrchestrator?as_of_date=2022-06-30&ReportName=PvmManagerTrackRecordReport&frequency=Once&save=True&aggregate_interval=ITD&EntityDomainTypes=InvestmentManager&EntityNames=[%22ExampleManagerName%22]


class PvmManagerTrackRecordReport(BasePvmTrackRecordReport):
    def __init__(
        self,
        report_meta: ReportMeta,
        report_name=ReportNames.PvmManagerTrackRecordReport,
    ):
        super().__init__(report_name, report_meta)

    @cached_property
    def investments(self) -> List[str]:
        c = self.children
        assert c is not None
        return list(
            c[EntityStandardNames.EntityName].dropna().drop_duplicates()
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

    def generate_135_tables(self) -> ReportWorksheet:
        realization_status_breakout = (
            generate_realized_unrealized_all_performance_breakout(
                self.position_node_provider
            )
        )
        dimns = self.position_node_provider.atomic_dimensions
        position_map = self.position_to_investment_breakout
        item = OneThreeFiveRenderer(
            breakout=realization_status_breakout,
            position_to_investment_mapping=position_map,
            position_dimn=dimns,
        )
        return item.render()

    # override
    @cached_property
    def attribution_items(self) -> List[str]:
        return self.position_node_provider.base_evaluation_items

    def generate_attribution_items(self) -> List[ReportWorkBookHandler]:
        wbs: List[ReportWorkBookHandler] = []
        for i in self.attribution_items:
            evaluated = self.position_node_provider.generate_evaluatable_node_hierarchy(
                [i]
            )
            rendered = RenderAttribution(evaluated).render()
            wb = ReportWorkBookHandler(
                i,
                Template_Attribution,
                report_sheets=[rendered],
                short_name=i,
            )
            wbs.append(wb)
        return wbs

    def generate_fund_breakout(self) -> ReportWorkBookHandler:
        # gross
        i_name = "InvestmentName"
        gross_realized_status_breakout = self.position_node_provider.generate_evaluatable_node_hierarchy(
            [ReportedRealizationStatus, i_name]
        )
        net = self.investment_node_provider.generate_evaluatable_node_hierarchy(
            [i_name]
        )
        ref_items = self.investment_node_provider.atomic_dimensions[
            [i_name, "Vintage", "Committed Capital"]
        ]
        # because reported in mm
        ref_items["Committed Capital"] = (
            ref_items["Committed Capital"] * 1_000_000.0
        )
        ref_items = ref_items.rename(
            columns={i_name: PvmNodeBase._DISPLAY_NAME}
        )
        ref_items.sort_values(by="Vintage", inplace=True, ascending=True)
        i = RenderRealizationStatusFundBreakout_NetGross(
            gross_realization_status_breakout=gross_realized_status_breakout,
            net_breakout=net,
            dimn=ref_items,
        ).render()
        wb = ReportWorkBookHandler(
            "Summary",
            SummaryTemplate,
            report_sheets=[i],
            short_name="Summary",
        )
        return wb

    def assign_components(self) -> List[ReportWorkBookHandler]:
        base = [self.generate_fund_breakout()]
        attribution = self.generate_attribution_items()
        one_three_five = [
            ReportWorkBookHandler(
                self.manager_name,
                Template_135,
                [self.generate_135_tables()],
            )
        ]
        final = one_three_five + base + attribution
        return final
