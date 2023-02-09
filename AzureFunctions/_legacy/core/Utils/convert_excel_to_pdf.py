from copy import deepcopy
import logging
from gcm.inv.utils.Java.java_client import java_client
from gcm.Dao.DaoRunner import DaoRunner, DaoSource, AzureDataLakeDao
from gcm.Dao.daos.azure_datalake.azure_datalake_file import (
    AzureDataLakeFile,
)
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


def convert(
    file: io.BytesIO,
    runner: DaoRunner,
    source: DaoSource,
    base_params: dict,
):
    java_client.install_jre()
    java_client.start_jvm()

    params = deepcopy(base_params)
    params["file_path"] = params["file_path"].replace(".xlsx", ".pdf")
    assert params is not None
    from asposecells.api import Workbook, SaveFormat, LoadOptions, License, FontConfigs

    lic = License()
    try:
        license_content: AzureDataLakeFile = runner.execute(
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
    wb = workbook.createWorkbookFromBytes(file.read(), loadOptions=loadOptions)

    FontConfigs.setFontFolder(get_fonts_path(), True)
    v = wb.saveToBytes(SaveFormat.PDF)
    runner.execute(
        params=params,
        source=source,
        operation=lambda d, p: d.post_data(p, v),
    )
    logging.info(f"Exported PDF: {str(params)}")


def get_fonts_path() -> str:
    full_path = pathlib.Path(__file__).parent.parent.parent.parent.resolve()
    fonts_folder_path = os.path.join(full_path, FONTS_RELATIVE_PATH)
    logging.info(f"Fonts directory: {fonts_folder_path}")

    return fonts_folder_path
