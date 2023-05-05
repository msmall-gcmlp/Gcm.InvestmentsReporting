from .report_component_base import (
    ReportComponentBase,
    ReportComponentType,
)
import pandas as pd
import json


class ReportTable(ReportComponentBase):
    def __init__(
        self,
        component_name: str,
        df: pd.DataFrame = None,
        render_params: "ReportTableRenderParams" = None,
    ):
        super().__init__(component_name)
        self.df = df
        if render_params is None:
            render_params = ReportTable.ReportTableRenderParams()
        self.render_params = render_params

    class ReportTableRenderParams(ReportComponentBase.RendererParams):
        def __init__(self, trim_range=False):
            self.trim_range = trim_range

    @property
    def component_type(self) -> ReportComponentType:
        return ReportComponentType.ReportTable

    @staticmethod
    def from_dict(d: dict, **kwargs) -> "ReportTable":
        name = d["component_name"]
        df = pd.read_json(d["df"])
        r: ReportTable.ReportTableRenderParams = None
        if "renderer" in d:
            r = ReportTable.ReportTableRenderParams.from_dict(
                json.loads(d["renderer"])
            )
        return ReportTable(name, df, r)

    def to_dict(self):
        final = {
            "component_type": self.component_type.name,
            "component_name": self.component_name,
            "renderer": self.render_params.to_json(),
            "df": self.df.to_json(),
        }
        return final
