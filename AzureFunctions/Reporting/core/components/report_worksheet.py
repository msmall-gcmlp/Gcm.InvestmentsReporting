from .report_component_base import (
    ReportComponentBase,
    ReportComponentType,
)
import json


class ReportWorksheet(ReportComponentBase):
    def __init__(
        self,
        # overriding constructor to make items clearer
        worksheet_name: str,
        render_params: "ReportWorkSheetRenderer",
    ):
        super().__init__(worksheet_name)
        if render_params is None:
            render_params = ReportWorksheet.ReportWorkSheetRenderer()
            self.render_params = render_params

    @property
    def worksheet_name(self):
        return self.component_name

    class ReportWorkSheetRenderer(ReportComponentBase.RendererParams):
        def __init__(self, hide_columns: dict):
            self.hide_columns = hide_columns

    @property
    def component_type(self) -> ReportComponentType:
        return ReportComponentType.ReportWorksheet

    @staticmethod
    def from_dict(d: dict, **kwargs) -> "ReportWorksheet":
        name = (d["component_name"],)
        r = ReportWorksheet.ReportWorkSheetRenderer.from_dict(
            json.loads(d["renderer"])
        )
        return ReportWorksheet(name, r)

    def to_dict(self):
        final = {
            "component_type": self.component_type.name,
            "component_name": self.component_name,
            "renderer": self.render_params.to_json(),
        }
        return final
