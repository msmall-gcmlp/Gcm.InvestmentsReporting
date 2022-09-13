from copy import deepcopy
import logging
from gcm.inv.utils.Java.java_client import java_client
from gcm.Dao.DaoRunner import DaoRunner, DaoSource

import io


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
    from asposecells.api import Workbook, SaveFormat, LoadOptions

    logging.info("Exporting PDF")
    loadOptions = LoadOptions()
    workbook = Workbook()
    wb = workbook.createWorkbookFromBytes(
        file.read(), loadOptions=loadOptions
    )
    v = wb.saveToBytes(SaveFormat.PDF)
    runner.execute(
        params=params,
        source=source,
        operation=lambda d, p: d.post_data(p, v),
    )
    logging.info("The PDF was saved to ")
