from typing import List
from AzureFunctions.Reporting.core.components.report_workbook_handler import (
    ReportWorkBookHandler,
)
from ......Reports.report_names import ReportNames
from ......core.report_structure import ReportMeta, EntityStandardNames
from ....investment_manager_reports.pvm_tr import (
    PvmManagerTrackRecordReport,
)
import copy
from ....investment_reports.pvm_tr import PvmInvestmentTrackRecordReport
from functools import cached_property
from gcm.inv.dataprovider.entity_provider.controller import (
    get_entity_domain_provider,
)
import pandas as pd


class PvmManagerTrackRecordReportAggregation(PvmManagerTrackRecordReport):
    def __init__(self, report_meta: ReportMeta):
        super().__init__(
            report_meta,
            report_name=ReportNames.PvmManagerTrackRecordReportAggregation,
        )

    def investment_report_wbs(self) -> List[ReportWorkBookHandler]:
        wbs: List[ReportWorkBookHandler] = []
        domain = PvmInvestmentTrackRecordReport.level()
        this_entity_domain_provider = get_entity_domain_provider(domain)
        inv_data: pd.DataFrame = (
            this_entity_domain_provider.get_by_entity_names(
                self.investments
            )
        )
        for i in self.investments:
            this_report_meta = copy.deepcopy(self.report_meta)
            this_report_meta.entity_domain = domain
            this_inv_data = inv_data[
                inv_data[EntityStandardNames.EntityName].isin([i])
            ]
            this_report_meta.entity_info = this_inv_data
            r = PvmInvestmentTrackRecordReport(
                report_meta=this_report_meta,
                investment_manager_name=self.manager_name,
            )
            wb = r.assign_components()
            for w in wb:
                wbs.append(w)
        return wbs

    def assign_components(self) -> List[ReportWorkBookHandler]:
        investment_reports = self.investment_report_wbs()
        base_items = super().assign_components()

        return base_items + investment_reports
