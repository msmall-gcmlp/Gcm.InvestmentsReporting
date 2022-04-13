from gcm.ProgramRunner.programrunner import ProgramRunner
from gcm.Dao.DaoRunner import DaoRunner


class ReportingRunnerBase(ProgramRunner):
    def __init__(
        self, runner: DaoRunner, config_params=None, container_lambda=None
    ):
        self._runner = runner
        super().__init__(
            config_params=config_params, container_lambda=container_lambda
        )

    def global_preconditions(self, **kwargs):
        return super().global_preconditions(**kwargs)

    def global_post_conditions(self, **kwargs):
        return super().global_post_conditions(**kwargs)

    def base_container(self):
        return super().base_container()

    def _executing_in_scenario(self, **kwargs):
        pass
