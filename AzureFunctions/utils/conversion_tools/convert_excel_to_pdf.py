from copy import deepcopy
import logging
from gcm.inv.utils.Java.java_client import java_client
from gcm.Dao.DaoRunner import AzureDataLakeDao, DaoSource
from gcm.Dao.daos.azure_datalake.azure_datalake_file import (
    AzureDataLakeFile,
)
from gcm.inv.scenario import Scenario
import io
import os
import pathlib

license__location = (
    "/".join(
        [
            "cleansed",
            "investmentsreporting",
            "controls",
        ]
    )
    + "/"
)

FONTS_RELATIVE_PATH = "assets/fonts"


def convert(file: io.BytesIO, base_params: dict, source: DaoSource):
    java_client.install_jre()
    java_client.start_jvm()

    params = deepcopy(base_params)
    params["file_path"] = params["file_path"].replace(".xlsx", ".pdf")
    assert params is not None
    dao = Scenario.get_attribute("dao")
    from asposecells.api import (
        Workbook,
        SaveFormat,
        LoadOptions,
        License,
        FontConfigs,
    )

    lic = License()
    try:

        license_content: AzureDataLakeFile = dao.execute(
            params=AzureDataLakeDao.create_get_data_params(
                license__location,
                "Aspose.Cells.PythonviaJava.lic",
                retry=False,
            ),
            source=DaoSource.DataLake,
            operation=lambda d, p: d.get_data(p),
        )
        lic.setLicense(license_content.content)
    except:
        logging.info("skipping licensing step")

    logging.info("Exporting PDF")
    loadOptions = LoadOptions()
    workbook = Workbook()
    wb = workbook.createWorkbookFromBytes(
        file.read(), loadOptions=loadOptions
    )
    wb.calculateFormula(True)
    FontConfigs.setFontFolder(get_fonts_path(), True)

    # DT uncomment for fixing merged cell pdf issue
    # t1 = wb.getWorksheets().get(1).getCells().getMergedCells()
    # for x in range(wb.getWorksheets().getCount()):
    #     if x <= 1:
    #         continue
    #     wb.getWorksheets().get(x).getCells().clearMergedCells()
    #     merged_cell_array = wb.getWorksheets().get(1).getCells().getMergedCells()
    #     for i in range(len(merged_cell_array)):
    #         start_row = merged_cell_array.get(i).StartRow
    #         end_row = merged_cell_array.get(i).EndRow
    #         start_col = merged_cell_array.get(i).StartColumn
    #         end_col = merged_cell_array.get(i).EndColumn
    #         wb.getWorksheets().get(x).getCells().merge(start_row+1, start_col, end_row-start_row+1, end_col-start_col+1)

    v = wb.saveToBytes(SaveFormat.PDF)
    dao.execute(
        params=params,
        source=source,
        operation=lambda d, p: d.post_data(p, v),
    )
    logging.info(f"Exported PDF: {str(params)}")
    return params


def get_fonts_path() -> str:
    full_path = pathlib.Path(__file__).parent.parent.resolve()
    fonts_folder_path = os.path.join(full_path, FONTS_RELATIVE_PATH)
    logging.info(f"Fonts directory: {fonts_folder_path}")

    return fonts_folder_path
