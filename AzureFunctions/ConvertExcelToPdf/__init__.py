import logging
import os
import tempfile
import azure.functions as func
from java_client import java_client
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import (
    AzureDataLakeDao,
    AzureDataLakeFile,
)
from gcm.Dao.DaoRunner import DaoRunner, DaoSource, DaoRunnerConfigArgs


def main(req: func.HttpRequest) -> func.HttpResponse:
    # Aspose.Cells for Python is a wrapper around the Java library,
    # so we need to install Java and start the Java Virtual Machine before using the library
    java_client.install_jre()
    java_client.start_jvm()

    from asposecells.api import Workbook, SaveFormat, LoadOptions

    # This example creates a new Excel file, populates one cell, and saves the file to a temporary directory as a PDF
    # An Aspose.Cells license is needed to remove the watermark from the PDF
    logging.info("Exporting PDF")
    temp_dir = tempfile.TemporaryDirectory()
    file_path = os.path.join(temp_dir.name, "test_pdf.pdf")

    runner = DaoRunner(
        container_lambda=lambda b, i: b.config.from_dict(i),
        config_params={
            DaoRunnerConfigArgs.dao_global_envs.name: {
                DaoSource.ReportingStorage.name: {
                    "Environment": "dev",
                    "Subscription": "nonprd",
                }
            }
        },
    )
    file_path = "Performance Quality_Whale Rock_PFUND_Risk_2022-05-31.xlsx"
    params = AzureDataLakeDao.create_get_data_params(
        "performance/Risk", file_path
    )
    file: AzureDataLakeFile = runner.execute(
        params=params,
        source=DaoSource.ReportingStorage,
        operation=lambda dao, params: dao.get_data(params),
    )

    # file: AzureDataLakeFile = runner.execute(
    #     params=params,
    #     source=DaoSource.DataLake,
    #     operation=lambda dao, params: dao.get_data(params),
    # )
    loadOptions = LoadOptions()
    workbook = Workbook()
    wb1 = workbook.createWorkbookFromBytes(
        file.content, loadOptions=loadOptions
    )

    # workbook = Workbook()
    # workbook = Workbook("C:\\Users\\CMCNAMEE\\OneDrive -
    #  GCM Grosvenor\\Desktop\\test_multi_tab.xlsx")
    # workbook.getWorksheets().get(0).getCells().get("A1").setValue("Test")
    # TO SAVE in the Reporting hub
    wb1.save(file_path, SaveFormat.PDF)
    # wb1.save("C:\\Users\\agalstyan\\OneDrive
    #  - GCM Grosvenor\\Documents\\Market_Performance\\test2.pdf", SaveFormat.PDF)
    logging.info("The PDF was saved to " + file_path)

    return func.HttpResponse(status_code=200, body=str(file_path))
