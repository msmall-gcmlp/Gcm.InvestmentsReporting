from gcm.ProgramRunner.programrunner import ProgramRunner
import pandas as pd
import datetime as dt


class InvestemtnsReportingRunner(ProgramRunner):
    @property
    def base_container(self):
        pass

    def print_single_data_frame(
        self,
        asofdate: dt.datetime,
        input_data: pd.DataFrame,
        location: str,
        file_name: str,
        save: bool,
    ):
        date_string = asofdate.strftime("%Y-%m-%d")
        final_loc = f"{location}{file_name}_{date_string}.xlsx"
        if save:
            input_data.to_excel(final_loc)
        return final_loc

    def run(self, **kwargs):
        asofdate: dt.datetime = kwargs["asofdate"]
        input_data = kwargs["input_data"]
        print_type: str = kwargs["print_type"]
        save: bool = kwargs["save"]
        if type(input_data) is pd.DataFrame and print_type == "simple":
            location: str = kwargs["location"]
            file_name: str = kwargs["file_name"]
            loc = self.print_single_data_frame(
                asofdate=asofdate,
                input_data=input_data,
                location=location,
                file_name=file_name,
                save=save,
            )
            return loc
        return "done"

    def global_post_conditions(self):
        return super().global_post_conditions()

    def global_preconditions(self):
        return super().global_preconditions()
