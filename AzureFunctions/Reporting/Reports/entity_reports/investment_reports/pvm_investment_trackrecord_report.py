from ....core.report_structure import (
    ReportMeta,
)
from ..utils.PvmTrackRecord.base_pvm_tr_report import (
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


class PvmInvestmentTrackRecordReport(BasePvmTrackRecordReport):
    def __init__(
        self, report_meta: ReportMeta, investment_manager_name=None
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

    def filter_by_inv_id(self, df: pd.DataFrame):
        return df[df["InvestmentId"] == self.idw_pvm_tr_id]

    @classmethod
    def level(cls):
        return EntityDomainTypes.Investment

    @property
    def net_cashflows(self) -> pd.DataFrame:
        __name = "__investment_cfs"
        if getattr(self, __name, None is None):
            cfs = self.manager_handler.all_net_cfs
            cfs = self.filter_by_inv_id(cfs)
            setattr(self, __name, cfs)
        return getattr(self, __name, None is None)

    @property
    def position_cashflows(self) -> pd.DataFrame:
        __name = "__pos_cfs"
        if getattr(self, __name, None is None):
            cfs = self.manager_handler.all_position_cfs
            cfs = self.filter_by_inv_id(cfs)
            setattr(self, __name, cfs)
        return getattr(self, __name, None is None)

    @property
    def manager_name(self):
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

    def assign_components(self):
        pos = self.position_cashflows
        cf = self.net_cashflows
        assert pos is not None and cf is not None
        tables = [
            ReportTable(
                "MyComponent",
                pd.DataFrame({"V1": [1.0, 2.0], "V2": [1.0, 2.0]}),
            ),
            ReportTable(
                "MyComponent_2",
                pd.DataFrame({"V1": [1.0, 2.0], "V2": [1.0, 2.0]}),
            ),
        ]
        return tables
