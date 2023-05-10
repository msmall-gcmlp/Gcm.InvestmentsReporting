from .report_component_base import (
    ReportComponentBase,
    ReportComponentType,
)
import json
from .report_table import ReportTable
from typing import List


class ReportWorksheet(ReportComponentBase):
    def __init__(
        self,
        # overriding constructor to make items clearer
        worksheet_name: str,
        render_params: "ReportWorkSheetRenderer",
        report_tables: List[ReportTable] = [],
    ):
        super().__init__(worksheet_name)
        if render_params is None:
            render_params = ReportWorksheet.ReportWorkSheetRenderer()
        self.render_params = render_params
        self.report_tables = report_tables

    @property
    def worksheet_name(self):
        return self.component_name

    class ReportWorkSheetRenderer(ReportComponentBase.RendererParams):
        def __init__(
            self,
            hide_columns: List[str] = None,
            print_region: str = None,
            trim_region: List[str] = None,
        ):
            self.hide_columns = hide_columns
            self.print_region = print_region
            self.trim_region = trim_region

    @property
    def component_type(self) -> ReportComponentType:
        return ReportComponentType.ReportWorksheet

    @staticmethod
    def from_dict(d: dict, **kwargs) -> "ReportWorksheet":
        tables = [ReportTable.from_dict(x) for x in d["report_tables"]]
        name = (d["component_name"],)
        r = ReportWorksheet.ReportWorkSheetRenderer.from_dict(
            json.loads(d["renderer"])
        )
        return ReportWorksheet(name, r, tables)

    def to_dict(self):
        final = {
            "component_type": self.component_type.name,
            "component_name": self.component_name,
            "renderer": self.render_params.to_json(),
            "report_tables": [x.to_dict() for x in self.report_tables],
        }
        return final
