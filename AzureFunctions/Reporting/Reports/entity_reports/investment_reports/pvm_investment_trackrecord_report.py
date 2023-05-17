from typing import List

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
from ....core.components.report_table import ReportTable
import pandas as pd
from ...report_names import ReportNames
from ..utils.pvm_track_record.data_handler.investment_container import (
    InvestmentContainerBase,
)
from ..utils.pvm_track_record.data_handler.pvm_track_record_handler import (
    TrackRecordHandler,
)
from functools import cached_property
from ....core.components.report_workbook_handler import (
    ReportWorkBookHandler,
)
from ....core.components.report_worksheet import ReportWorksheet
from ..utils.pvm_track_record.analytics.attribution import (
    PvmTrackRecordAttribution,
)
from ..utils.pvm_track_record.analytics.attribution import (
    get_perf_concentration_rpt_dict,
    generate_fund_rpt_dict,
)


class PvmInvestmentTrackRecordReport(BasePvmTrackRecordReport):
    def __init__(
        self, report_meta: ReportMeta, investment_manager_name=None
    ):
        super().__init__(
            ReportNames.PvmInvestmentTrackRecordReport, report_meta
        )
        self.___investment_manager_name = investment_manager_name

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
        def investment_cashflows(self) -> pd.DataFrame:
            cfs = self.manager_handler.all_net_cfs
            cfs = self.filter_by_inv_id(cfs)
            return cfs

        @cached_property
        def investment_dimn(self) -> pd.DataFrame:
            cached_property = self.manager_handler.investment_attrib
            cached_property = self.filter_by_inv_id(cached_property)
            return cached_property

        @cached_property
        def position_cashflows(self) -> pd.DataFrame:
            cfs = self.manager_handler.all_position_cfs
            cfs = self.filter_by_inv_id(cfs)
            return cfs

        @cached_property
        def position_dimn(self) -> pd.DataFrame:
            position_dimn = self.manager_handler.position_attrib
            position_dimn = self.filter_by_inv_id(position_dimn)
            return position_dimn

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

    @property
    def manager_name(self) -> str:
        if self.___investment_manager_name is None:
            # time to do acrobatics....
            e = self.related_entities
            manager_data = e.get_entities_directly_related_by_name(
                EntityDomainTypes.InvestmentManager, False
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

    @cached_property
    def pvm_perfomance_results(self) -> PvmTrackRecordAttribution:
        return PvmTrackRecordAttribution([self.investment_handler])

    def assign_components(self):
        tr_json = generate_fund_rpt_dict(
            df=self.investment_handler.position_dimn,
            cf=self.investment_handler.position_cashflows,
        )
        tr_tables: List[ReportTable] = []
        for k, v in tr_json.items():
            this_table = ReportTable(k, v)
            tr_tables.append(this_table)

        worksheets = [
            ReportWorksheet(
                "Fund TR",
                report_tables=tr_tables,
                render_params=ReportWorksheet.ReportWorkSheetRenderer(
                    trim_region=[x.component_name for x in tr_tables]
                ),
            )
        ]

        perf_concen_json = get_perf_concentration_rpt_dict(
            deal_attrib=self.investment_handler.position_dimn,
            deal_cf=self.investment_handler.position_cashflows,
        )

        perf_concen_tbls: List[ReportTable] = []
        for k, v in perf_concen_json.items():
            this_table = ReportTable(k, v)
            perf_concen_tbls.append(this_table)

        worksheets.append(
            ReportWorksheet(
                "Performance Concentration",
                report_tables=perf_concen_tbls,
                render_params=ReportWorksheet.ReportWorkSheetRenderer(
                    trim_region=[
                        x.component_name for x in perf_concen_tbls
                    ]
                ),
            )
        )

        return [
            ReportWorkBookHandler(
                f"{self.manager_name}_{self.idw_pvm_tr_id}_Report",
                template_location=self.excel_template_location,
                report_sheets=worksheets,
            )
        ]
