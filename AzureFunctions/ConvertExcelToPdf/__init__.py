import logging
import os
import tempfile
import azure.functions as func
from java_client import java_client


def main(req: func.HttpRequest) -> func.HttpResponse:
    # Aspose.Cells for Python is a wrapper around the Java library,
    # so we need to install Java and start the Java Virtual Machine before using the library
    java_client.install_jre()
    java_client.start_jvm()

    from asposecells.api import Workbook, SaveFormat

    # This example creates a new Excel file, populates one cell, and saves the file to a temporary directory as a PDF
    # An Aspose.Cells license is needed to remove the watermark from the PDF
    logging.info('Exporting PDF')
    temp_dir = tempfile.TemporaryDirectory()
    file_path = os.path.join(temp_dir.name, "test_pdf.pdf")
    workbook = Workbook()
    workbook.getWorksheets().get(0).getCells().get("A1").setValue("Test")
    workbook.save(file_path, SaveFormat.PDF)
    logging.info('The PDF was saved to ' + file_path)

    return func.HttpResponse(status_code=200, body=str(file_path))
