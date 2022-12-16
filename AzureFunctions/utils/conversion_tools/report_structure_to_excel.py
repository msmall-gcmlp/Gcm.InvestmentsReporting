from .excel_io import ExcelIO
from openpyxl import Workbook
from openpyxl.writer.excel import save_virtual_workbook
from Reporting.core.components.report_table import ReportTable
from Reporting.core.report_structure import ReportStructure
from gcm.Dao.DaoRunner import DaoRunner, DaoSource
from .convert_excel_to_pdf import convert
import io
from typing import Tuple
from gcm.inv.scenario import Scenario


def print_report_to_template(
    wb: Workbook, struct: ReportStructure
) -> Workbook:
    for k in [x for x in struct.components if type(x) == ReportTable]:
        if k.component_name in wb.defined_names:
            address = list(wb.defined_names[k.component_name].destinations)
            for sheetname, cell_address in address:
                cell_address = cell_address.replace("$", "")
                # override wb:
                wb = ExcelIO.write_dataframe_to_xl(
                    wb, k.df, sheetname, cell_address
                )
    return wb


def save_report_structure_to_excel_return_virtual_book(
    template: Workbook,
    report_structure: ReportStructure,
    save: bool,
) -> Tuple[bytes, dict, DaoSource]:
    dao: DaoRunner = Scenario.get_attribute("dao")
    final_template = print_report_to_template(template, report_structure)
    b = save_virtual_workbook(final_template)
    [params, source] = report_structure.save_params()

    # TODO: Not sure why this happens with ReportingStorage

    if (
        source == DaoSource.ReportingStorage
        and type(params) == dict
        and "filesystem_name" in params
    ):
        params["filesystem_name"] = params["filesystem_name"].strip("/")

        if save:
            dao.execute(
                params=params,
                source=source,
                operation=lambda d, v: d.post_data(v, b),
            )
    return (b, params, source)


def save_pdf_from_workbook_and_structure(
    b: bytes,
    report_structure: ReportStructure,
    source: DaoSource,
    params: dict,
    save: bool,
):
    assert report_structure is not None
    if save:
        convert(
            io.BytesIO(b),
            base_params=params,
            source=source,
        )
