from gcm.ProgramRunner.programrunner import ProgramRunner


class InvestemtnsReportingRunner(ProgramRunner):
    @property
    def base_container(self):
        pass

    def run(self, **kwargs):
        return "Running Report"

    def global_post_conditions(self):
        return super().global_post_conditions()

    def global_preconditions(self):
        return super().global_preconditions()
