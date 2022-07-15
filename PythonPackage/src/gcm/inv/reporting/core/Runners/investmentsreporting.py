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
from ..ReportStructure.report_structure import (
    ReportingEntityTag,
    ReportingEntityTypes,
)
from gcm.Dao.DaoSources import DaoSource


class InvestmentsReportRunner(ProgramRunner):
    def __init__(self, config_params=None, container_lambda=None):
        super().__init__(config_params, container_lambda)

    def base_container(self):
        return super().base_container()

    def run(self, **kwargs):
        # we want to call via programrunner
        # so that we can
        # (1) ensure run in scenario (date)
        # (2) enable a single dao instance
        # (3) load items sequentially
        data = kwargs.get("data", None)
        final_data = None
        if data is not None:
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
            elif "raw_pdf_name" in kwargs:
                pdf_name = kwargs["raw_pdf_name"]
                pdf_location = kwargs["raw_pdf_location"]
                report.load_pdf(pdf_location, pdf_name)
            elif "_wb" in kwargs:
                _workbook: openpyxl.Workbook = kwargs["_wb"]
                report.load_workbook(_workbook)
            if "entity_name" in kwargs:
                entity_name: str = kwargs["entity_name"]
                entity_type: ReportingEntityTypes = kwargs["entity_type"]
                # must be a list of ints
                entity_ids = kwargs.get("entity_ids", None)
                entity_display_name = kwargs.get(
                    "entity_display_name", entity_name
                )
                reporting_entity = ReportingEntityTag(
                    entity_type,
                    entity_name,
                    entity_display_name,
                    entity_ids,
                )
                report.load_reporting_entity(reporting_entity)
            if "save" in kwargs:
                output_dir = kwargs.get("output_dir", base_output_location)
                output_source = kwargs.get(
                    "report_output_source", DaoSource.ReportingStorage
                )
                report.print_report(
                    output_source=output_source,
                    output_dir=output_dir,
                    save=kwargs["save"]
                )
            return True
        else:
            raise RuntimeError("You must specify a report name")

    def global_post_conditions(self, **kwargs):        return super().global_post_conditions(**kwargs)

    def global_preconditions(self, **kwargs):
        return super().global_preconditions(**kwargs)
