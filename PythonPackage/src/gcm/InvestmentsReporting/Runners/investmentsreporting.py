from gcm.ProgramRunner.programrunner import ProgramRunner, Scenario
import pandas as pd
from .TemplatedRuns.templated_report import (
    print_multiple_data_frame_to_template,
)


class InvestmentsReportRunner(ProgramRunner):
    def __init__(self, config_params=None, container_lambda=None):
        super().__init__(config_params, container_lambda)

    def base_container(self):
        return super().base_container()

    def print_single_data_frame_simple(
        self,
        input_data: pd.DataFrame,
        location: str,
        file_name: str,
        save: bool,
    ):
        date_string = Scenario.instance().rundate.strftime("%Y-%m-%d")
        final_loc = f"{location}{file_name}_{date_string}.xlsx"
        if save:
            input_data.to_excel(final_loc)
        return final_loc

    def run(self, **kwargs):
        input_data = kwargs["input_data"]
        print_type: str = kwargs["print_type"]
        save: bool = kwargs["save"]
        if type(input_data) is pd.DataFrame and print_type == "simple":
            location: str = kwargs["location"]
            file_name: str = kwargs["file_name"]
            loc = self.print_single_data_frame_simple(
                input_data=input_data,
                location=location,
                file_name=file_name,
                save=save,
            )
            return loc
        elif type(input_data) is dict and print_type == "templated":
            template_name = kwargs["template_name"]
            file_name: str = kwargs["file_name"]
            loc = print_multiple_data_frame_to_template(
                data=input_data,
                template_name=template_name,
                file_name=file_name,
                date=Scenario.current_scenario().asofdate,
                runner=kwargs["runner"],
            )
            return loc
        return "done"

    def global_post_conditions(self, **kwargs):
        return super().global_post_conditions(**kwargs)

    def global_preconditions(self, **kwargs):
        return super().global_preconditions(**kwargs)
