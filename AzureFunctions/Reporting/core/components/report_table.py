from .report_component_base import ReportComponentBase, ReportComponentType
import pandas as pd


class ReportTable(ReportComponentBase):
    def __init__(self, component_name: str, df: pd.DataFrame = None):
        super().__init__(component_name)
        self.df = df

    @property
    def component_type(self) -> ReportComponentType:
        return ReportComponentType.ReportTable

    @staticmethod
    def from_dict(d: dict, **kwargs) -> "ReportTable":
        return ReportTable(d["component_name"], pd.read_json(d["df"]))

    def to_dict(self):
        return {
            "component_type": self.component_type.name,
            "component_name": self.component_name,
            "df": self.df.to_json(),
        }
