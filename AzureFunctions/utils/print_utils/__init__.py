from gcm.inv.scenario import Scenario, DaoRunner
from gcm.Dao.DaoRunner import DaoSource
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import (
    AzureDataLakeFile,
    TabularDataOutputTypes,
)
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
    print_pdf_report,
)


def _get_print_params(
    report_structure: ReportStructure, dao: DaoRunner = None
) -> tuple[dict, DaoSource]:
    dao: DaoRunner = (
        dao if dao is not None else Scenario.get_attribute("dao")
    )
    assert report_structure is not None
    [params, source] = report_structure.save_params

    # TODO: Not sure why this happens with ReportingStorage

    params = clean_params(params, source)
    return [params, source]


def clean_params(params: dict, source: DaoSource):
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
    return params


def print_excel_to_pdf(
    dao: DaoRunner,
    report_structure: ReportStructure,
    dl_params: dict,
    source: DaoSource = DaoSource.ReportingStorage,
) -> dict:
    cleaned_params = clean_params(dl_params, source)
    file: AzureDataLakeFile = dao.execute(
        params=cleaned_params,
        source=source,
        operation=lambda d, p: d.get_data(p),
    )
    wb: Workbook = file.to_tabular_data(
        TabularDataOutputTypes.ExcelWorkBook, dl_params
    )
    save_params = print_pdf_report(cleaned_params, source=source, wb=wb)
    return save_params


def print(
    report_structure: ReportStructure, print_pdf: bool = True
) -> dict:
    dao: DaoRunner = Scenario.get_attribute("dao")
    [params, source] = _get_print_params(report_structure, dao)
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
