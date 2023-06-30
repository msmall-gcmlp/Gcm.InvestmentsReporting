from .report_component_base import ReportComponentBase, ReportComponentType
from typing import List
from gcm.Dao.DaoRunner import AzureDataLakeDao
from .report_worksheet import ReportWorksheet


class ReportWorkBookHandler(ReportComponentBase):
    def __init__(
        self,
        component_name: str,
        template_location: AzureDataLakeDao.BlobFileStructure,
        report_sheets: List[ReportWorksheet] = [],
    ):
        super().__init__(component_name)
        self.template_location = template_location
        self.report_sheets = report_sheets

    @property
    def component_type(self) -> ReportComponentType:
        return ReportComponentType.ReportWorkBookHandler

    @staticmethod
    def from_dict(d: dict, **kwargs) -> "ReportWorkBookHandler":
        name = d["component_name"]
        sheets = [ReportWorksheet.from_dict(x) for x in d["report_sheets"]]
        location = d["template_location"]
        blob_loc = AzureDataLakeDao.BlobFileStructure(
            zone=AzureDataLakeDao.BlobFileStructure.Zone[location["zone"]],
            sources=location["sources"],
            entity=location["entity"],
            path=location["path"],
        )
        workbook = ReportWorkBookHandler(
            component_name=name,
            template_location=blob_loc,
            report_sheets=sheets,
        )
        return workbook

    def to_dict(self) -> dict:
        dict = {
            "component_type": self.component_type.name,
            "component_name": self.component_name,
            "report_sheets": [x.to_dict() for x in self.report_sheets],
            "template_location": {
                "zone": self.template_location.zone.name,
                "sources": self.template_location.sources,
                "entity": self.template_location.entity,
                "path": self.template_location.path,
            },
        }
        return dict