from gcm.inv.scenario import Scenario, DaoRunner
from gcm.Dao.DaoRunner import DaoSource
from Reporting.core.report_structure import ReportStructure
from typing import List
from Reporting.core.components.report_workbook_handler import (
    ReportWorkBookHandler,
)
from .standard_handler import (
    generate_workbook,
    Workbook,
    merge_files,
    print_excel_report,
)


def print(report_structure: ReportStructure, print_pdf: bool = True):
    dao: DaoRunner = Scenario.get_attribute("dao")
    assert report_structure is not None
    [params, source] = report_structure.save_params

    # TODO: Not sure why this happens with ReportingStorage
    f_s = "filesystem_name"
    if (
        source == DaoSource.ReportingStorage
        and type(params) == dict
        and f_s in params
    ):
        params[f_s] = params[f_s].strip("/")

    params = {
        key: value
        for key, value in params.items()
        if key
        in [
            "filesystem_name",
            "file_path",
            "retry",
            "metadata",
        ]
    }
    known_components: List[
        ReportWorkBookHandler
    ] = report_structure.components
    wbs: List[Workbook] = []
    for k in known_components:
        wb = generate_workbook(k)
        wbs.append(wb)
    final_wb: Workbook = merge_files(wbs) if len(wbs) > 1 else wbs[0]
    print_excel_report(
        final_wb,
        dao,
        source,
        params,
        Scenario.get_attribute("save"),
        print_pdf,
    )
    return params
