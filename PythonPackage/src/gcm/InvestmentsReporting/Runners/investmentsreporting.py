from gcm.ProgramRunner.programrunner import ProgramRunner, Scenario
from ..ReportStructure.report_structure import (
    ReportStructure,
    ReportTemplate,
    template_location,
    base_output_location,
)
import pandas as pd
from gcm.Dao.DaoRunner import DaoRunner
import datetime as dt
import openpyxl


class InvestmentsReportRunner(ProgramRunner):
    def __init__(self, config_params=None, container_lambda=None):
        super().__init__(config_params, container_lambda)

    def base_container(self):
        return super().base_container()

    def run(self, **kwargs):
        data = kwargs["data"]
        final_data = {}
        for i in data:
            df = data[i]
            if type(df) == str:
                df = pd.read_json(df)
            elif type(df) == pd.DataFrame:
                df: pd.DataFrame = df
            final_data[i] = df
        runner: DaoRunner = kwargs["runner"]
        current_date: dt.datetime = (
            Scenario.current_scenario().get_attribute("asofdate")
        )
        if "report_name" in kwargs:

            report = ReportStructure(
                kwargs["report_name"], final_data, current_date, runner
            )
            if "template" in kwargs:
                template_dir = kwargs.get(
                    "template_dir", template_location
                )
                report.load_template(
                    ReportTemplate(
                        kwargs["template"],
                        runner,
                        template_location=template_dir,
                    )
                )
            if "_wb" in kwargs:
                _workbook: openpyxl.Workbook = kwargs["_wb"]
                report.load_workbook(_workbook)
            if "save" in kwargs:
                output_dir = kwargs.get("output_dir", base_output_location)
                report.print_report(output_dir=output_dir)
            return True
        else:
            raise RuntimeError("You must specify a report name")

    def global_post_conditions(self, **kwargs):
        return super().global_post_conditions(**kwargs)

    def global_preconditions(self, **kwargs):
        return super().global_preconditions(**kwargs)
