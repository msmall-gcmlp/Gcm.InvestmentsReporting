from .report_component_base import ReportComponentBase, ReportComponentType
from .report_table import ReportTable
from .report_workbook_handler import ReportWorkBookHandler


def get_component_by_type(t: ReportComponentType) -> ReportComponentBase:
    if t == ReportComponentType.ReportTable:
        return ReportTable
    if t == ReportComponentType.ReportWorkBookHandler:
        return ReportWorkBookHandler


def convert_component_from_dict(i: dict) -> ReportComponentBase:
    # get component type:
    component_type = ReportComponentType[i["component_type"]]
    # get component by type:
    component_class = get_component_by_type(component_type)
    component = component_class.from_dict(i)
    return component
