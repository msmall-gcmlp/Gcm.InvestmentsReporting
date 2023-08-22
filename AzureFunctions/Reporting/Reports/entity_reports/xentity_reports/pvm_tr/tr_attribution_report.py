from AzureFunctions.Reporting.Reports.report_names import ReportNames
from AzureFunctions.Reporting.core.report_structure import ReportMeta
from .....core.report_structure import ReportMeta
from ....report_names import ReportNames
from .....core.report_structure import ReportMeta
from . import BasePvmTrackRecordReport, EntityDomainTypes
from .render_attribution import (
    RenderAttribution,
    TEMPLATE as Template_Attribution,
)
from typing import List
from .....core.components.report_workbook_handler import (
    ReportWorkBookHandler,
)


class PvmTrAttributionReport(BasePvmTrackRecordReport):
    def __init__(
        self,
        report_meta: ReportMeta,
        manager_name: str,
        investments: List[str],
    ):
        super().__init__(report_meta, ReportNames.PvmTrAttributionReport)
        self.__manager_name = manager_name
        self.__investments = investments

    @property
    def manager_name(self) -> str:
        return self.__manager_name

    @property
    def investments(self) -> List[str]:
        return self.__investments

    @classmethod
    def level(cls):
        return EntityDomainTypes.Asset

    # override

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

    def excel_template_location(self):
        return Template_Attribution

    def assign_components(self) -> List[ReportWorkBookHandler]:
        return self.generate_attribution_items()
