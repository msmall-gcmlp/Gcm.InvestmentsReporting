from copy import deepcopy
import logging
from gcm.inv.utils.Java.java_client import java_client
from gcm.Dao.DaoRunner import AzureDataLakeDao, DaoSource
from gcm.Dao.daos.azure_datalake.azure_datalake_file import (
    AzureDataLakeFile,
)
from gcm.inv.scenario import Scenario
import io

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


def convert(file: io.BytesIO, base_params: dict, source: DaoSource):
    java_client.install_jre()
    java_client.start_jvm()

    params = deepcopy(base_params)
    params["file_path"] = params["file_path"].replace(".xlsx", ".pdf")
    assert params is not None
    dao = Scenario.get_attribute("dao")
    from asposecells.api import Workbook, SaveFormat, LoadOptions, License

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

    v = wb.saveToBytes(SaveFormat.PDF)
    dao.execute(
        params=params,
        source=source,
        operation=lambda d, p: d.post_data(p, v),
    )
    logging.info("The PDF was saved to ")