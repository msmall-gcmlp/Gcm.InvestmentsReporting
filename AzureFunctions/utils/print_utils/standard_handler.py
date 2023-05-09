from gcm.inv.scenario import Scenario, DaoRunner
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import (
    AzureDataLakeFile,
    AzureDataLakeDao,
    TabularDataOutputTypes,
)
from openpyxl.worksheet.worksheet import Worksheet
from gcm.Dao.DaoRunner import DaoSource
from openpyxl import Workbook
from ..conversion_tools.excel_io import ExcelIO
from Reporting.core.components.report_table import ReportTable
from Reporting.core.components.report_workbook_handler import (
    ReportWorkBookHandler,
)
from ..conversion_tools.combine_excel import copy_sheet
from typing import List
from ..conversion_tools.convert_excel_to_pdf import convert
import io
import re
from Reporting.core.components.report_worksheet import ReportWorksheet


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
    trim_rows: List[int] = []
    if sheet.report_tables is not None and len(sheet.report_tables) > 0:
        for t in sheet.report_tables:
            wb = print_table_component(wb, t)
            if sheet.render_params.trim_region is not None:
                if t.component_name in sheet.render_params.trim_region:
                    trim_rows = trim_rows + return_trim_rows(
                        sheet.worksheet_name, t, wb=wb
                    )
    if len(trim_rows) > 0:
        # sort descending so you delete from bottom to top
        # DONOT CHANGE
        trim_rows.sort(reverse=True)
        for r in trim_rows:
            wb[sheet.worksheet_name].delete_rows(r)
    return wb


def return_trim_rows(
    target_sheetname: str, k: ReportTable, wb: Workbook
) -> List[int]:
    # the the range in question must be a rectangle
    # (i.e. someone can't just ctrl-clicked a bunch of random cells)
    address = list(wb.defined_names[k.component_name].destinations)
    list_of_numbers = []
    rows_to_delete = []
    for sheetname, cell_address in address:
        assert target_sheetname == sheetname
        cell_address = cell_address.replace("$", "")
        for el in re.split("(\d+)", cell_address):
            try:
                list_of_numbers.append(int(el))
            except ValueError:
                pass
        # override wb:
    rectangle_check = list(dict.fromkeys(list_of_numbers))
    if len(rectangle_check) == 2:
        named_range_range = range(list_of_numbers[0], list_of_numbers[1])
        if len(named_range_range) != len(k.df):
            # no trimming to be done as length of dataframe is the same size as
            # named range region pass
            rows_to_delete.extend(
                list(
                    range(
                        list_of_numbers[0] + len(k.df),
                        list_of_numbers[1] + 1,
                    )
                )
            )
            return rows_to_delete
    else:
        return []


def _get_sheet_row_heights(sheet, default_height):
    import pandas as pd

    row_heights = pd.DataFrame()
    for i in range(0, sheet.max_row):
        row_heights = pd.concat(
            [
                row_heights,
                pd.DataFrame(
                    {
                        "row": [i + 1],
                        "height": [sheet.row_dimensions[i + 1].height],
                    }
                ),
            ]
        )
    return row_heights.fillna(default_height)


def print_table_component(wb: Workbook, k: ReportTable) -> Workbook:
    if k.component_name in wb.defined_names:
        address = list(wb.defined_names[k.component_name].destinations)
        for sheetname, cell_address in address:
            cell_address = cell_address.replace("$", "")
            # override wb:
            wb = ExcelIO.write_dataframe_to_xl(
                wb, k.df, sheetname, cell_address
            )
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
    print_pdf: bool = True,
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
