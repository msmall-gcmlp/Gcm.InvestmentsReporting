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
from gcm.inv.scenario import Scenario
import datetime as dt
from ..utils.PvmTrackRecord.get_positions import get_positions
from ..utils.PvmTrackRecord.get_cfs import get_cfs


class PvmInvestmentTrackRecordReport(BasePvmTrackRecordReport):
    def __init__(
        self, report_meta: ReportMeta, investment_manager_name=None
    ):
        super().__init__(
            ReportNames.PvmInvestmentTrackRecordReport, report_meta
        )
        self._investment_manager_name = investment_manager_name

    @property
    def excel_template_location(self):
        return AzureDataLakeDao.BlobFileStructure(
            zone=AzureDataLakeDao.BlobFileStructure.Zone.raw,
            sources="investmentsreporting",
            entity="exceltemplates",
            path=["PvmInvestmentTrackRecordTemplate.xlsx"],
        )

    @classmethod
    def level(cls):
        return EntityDomainTypes.InvestmentManager

    @property
    def net_cashflows(self) -> pd.DataFrame:
        __name = "__investment_cfs"
        if getattr(self, __name, None is None):
            cfs = None
            setattr(self, __name, cfs)
        return getattr(self, __name, None is None)

    @property
    def position_cashflows(self) -> pd.DataFrame:
        __name = "__pos_cfs"
        if getattr(self, __name, None is None):
            as_of_date: dt.date = Scenario.get_attribute("as_of_date")
            cfs = get_cfs(
                self.position_list["PositionId"].to_list(),
                as_of_date=as_of_date,
                cf_type="Position",
            )
            setattr(self, __name, cfs)
        return getattr(self, __name, None is None)

    @property
    def position_list(self) -> pd.DataFrame:
        __name = "__positions"
        if getattr(self, __name, None) is None:
            df = get_positions(
                self.manager_handler.manager_hierarchy_structure,
                self.report_meta.entity_info,
            )
            setattr(self, __name, df)
        return getattr(self, __name, None)

    @property
    def manager_name(self):
        if self._investment_manager_name is None:
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
                self._investment_manager_name = managers[0]
            else:
                raise RuntimeError("More than one manager")
        return self._investment_manager_name

    def assign_components(self):
        positions = self.position_list
        pos = self.position_cashflows
        cf = self.net_cashflows
        assert pos is not None and cf is not None
        assert positions is not None
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
