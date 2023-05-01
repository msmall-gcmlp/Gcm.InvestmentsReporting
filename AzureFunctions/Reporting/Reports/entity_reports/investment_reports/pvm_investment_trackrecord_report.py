from ....core.report_structure import (
    ReportMeta,
)
from ..PvmTrackRecord.base_pvm_tr_report import BasePvmTrackRecordReport
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
from .get_data_for_investments import get_positions


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

    def net_cashflows(self) -> pd.DataFrame:
        pass

    def position_cashflows(self) -> pd.DataFrame:
        pass

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
        as_of_date: dt.date = Scenario.get_attribute("as_of_date")
        assert as_of_date is not None
        positions = self.position_list
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
