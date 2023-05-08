from gcm.inv.scenario import Scenario, DaoRunner
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import (
    AzureDataLakeFile,
    AzureDataLakeDao,
    TabularDataOutputTypes,
)
from openpyxl.worksheet.worksheet import Worksheet
from gcm.Dao.DaoRunner import DaoSource
from openpyxl import Workbook
from ...utils.conversion_tools.excel_io import ExcelIO
from ...Reporting.core.components.report_table import ReportTable
from ...Reporting.core.components.report_workbook_handler import (
    ReportWorkBookHandler,
)
from ...utils.conversion_tools.combine_excel import copy_sheet
from typing import List
from ...utils.conversion_tools.convert_excel_to_pdf import convert
import io
from ...Reporting.core.components.report_worksheet import ReportWorksheet


def get_template(
    blob_location: AzureDataLakeDao.BlobFileStructure,
) -> Workbook:
    assert blob_location is not None
    dao: DaoRunner = Scenario.get_attribute("dao")
    params = AzureDataLakeDao.create_blob_params(blob_location)
    file: AzureDataLakeFile = dao.execute(
        params=params,
        source=DaoSource.DataLake,
        operation=lambda d, p: d.get_data(p),
    )
    excel = file.to_tabular_data(
        TabularDataOutputTypes.ExcelWorkBook, params
    )
    return excel


def render_worksheet(wb: Workbook, sheet: ReportWorksheet):
    # to do: handle all the permutation combinations of a worksheet rendering
    # need to decide if we always need to wrap tables in a worksheet.

    this_sheet_name = sheet.worksheet_name
    assert this_sheet_name is not None

    # TODO: David to tackle this as well
    # check if rendering settings are not empty:
    if bool(sheet.render_params.hide_columns):
        raise NotImplementedError()
    if bool(sheet.render_params.print_region):
        raise NotImplementedError()

    # if any embedded tables in a worksheet
    if len(sheet.report_tables) > 0:
        for t in sheet.report_tables:
            wb = print_table_component(wb, t)
    return wb


def print_table_component(wb: Workbook, k: ReportTable) -> Workbook:
    if k.component_name in wb.defined_names:
        address = list(wb.defined_names[k.component_name].destinations)
        for sheetname, cell_address in address:
            cell_address = cell_address.replace("$", "")
            # override wb:
            wb = ExcelIO.write_dataframe_to_xl(
                wb, k.df, sheetname, cell_address
            )
    if k.render_params.trim_range:
        # TODO: David to complete
        # in this case, we want to trim the named range
        # as the number of rows in the DF of a given component
        # is lmore than the range defined in the named range region
        raise NotImplementedError()
    return wb


def merge_files(wb_list: List[Workbook]):
    merged = wb_list[0]
    wb_count = 0
    for k in wb_list:
        if wb_count > 0:
            ws_count = 0
            for s in k.sheetnames:
                source_sheet: Worksheet = k[s]
                target_sheet_name = f"{s}_{wb_count}_{ws_count}"
                merged.create_sheet(target_sheet_name)
                ws2: Worksheet = merged[target_sheet_name]
                copy_sheet(source_sheet, ws2)
                ws_count = ws_count + 1
        wb_count = wb_count + 1
    return merged


def generate_workbook(handler: ReportWorkBookHandler) -> Workbook:
    wb = get_template(handler.template_location)
    for table in handler.report_tables:
        wb = print_table_component(wb, table)
    if handler.report_sheets is not None:
        for ws in handler.report_sheets:
            wb = render_worksheet(wb, ws)
    return wb


def print_excel_report(
    wb: Workbook,
    dao: DaoRunner,
    source: DaoSource,
    params: dict,
    save: bool,
) -> dict:
    if save:
        wb_stream = io.BytesIO()
        wb.save(wb_stream)
        b = wb_stream.getvalue()
        dao.execute(
            params=params,
            source=source,
            operation=lambda d, v: d.post_data(v, b),
        )
        convert(
            io.BytesIO(b),
            base_params=params,
            source=source,
        )
    return params
