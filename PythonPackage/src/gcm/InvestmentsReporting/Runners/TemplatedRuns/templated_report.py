from ...Utils.excel_io import ExcelIO
from gcm.ProgramRunner.programrunner import Scenario
from gcm.Dao.DaoRunner import DaoRunner
from gcm.Dao.DaoSources import DaoSource
from gcm.Dao.daos.azure_datalake_dao import AzureDataLakeDao
from gcm.Dao.Utils.tabular_data_util_outputs import TabularDataOutputTypes
from openpyxl.writer.excel import save_virtual_workbook
import pandas as pd


def print_multiple_data_frame_to_template(
    input_data: dict,
    template_name: str,
    file_name: str,
    save: bool,
):
    excel_io = ExcelIO()
    runner = DaoRunner()
    test_loc = "raw/test/rqstest"
    location = f"{test_loc}/rqstest/ReportingTemplates/"
    params = AzureDataLakeDao.create_get_data_params(
        location,
        template_name,
        retry=False,
        return_as_stream=TabularDataOutputTypes.ExcelWorkBook,
    )

    wb = runner.execute(
        params=params,
        source=DaoSource.DataLake,
        operation=lambda dao, params: dao.get_data(params),
    )
    for sheet_name in input_data.keys():
        for cells in input_data[sheet_name].keys():
            data: pd.DataFrame = input_data[sheet_name][cells]
            wb = excel_io.write_dataframe_to_xl(
                wb, data, sheet_name, cells
            )
    date_string = Scenario.instance().rundate.strftime("%Y-%m-%d")
    output_name = f"{file_name}_{date_string}.xlsx"
    final_loc = f"{location}{output_name}"
    if save:
        params = AzureDataLakeDao.create_get_data_params(
            location, output_name, True
        )
        b = save_virtual_workbook(wb)
        runner.execute(
            params=params,
            source=DaoSource.DataLake,
            operation=lambda dao, params: dao.post_data(params, b),
        )
    return final_loc
