from ...ReportStructure.report_structure import (
    ReportStructure,
    ReportTemplate,
)
from gcm.Dao.DaoRunner import DaoRunner
import datetime as dt


def print_multiple_data_frame_to_template(
    data: dict,
    date: dt.datetime,
    template_name: str,
    runner=DaoRunner,
    file_name=str,
):
    template = ReportTemplate(template_name, runner)
    structure = ReportStructure(
        file_name, data=data, asofdate=date, runner=runner
    )
    structure.load_template(template)
    structure.print_data_to_excel()
