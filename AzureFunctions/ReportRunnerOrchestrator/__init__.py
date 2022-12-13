import azure.durable_functions as df
from gcm.inv.utils.azure.durable_functions.base_orchestrator import (
    BaseOrchestrator,
)
from ..utils.reporting_parsed_args import (
    ReportingParsedArgs,
)
from gcm.inv.utils.azure.durable_functions.parg_serialization import (
    serialize_pargs,
)


class ReportRunnerOrchestrator(BaseOrchestrator):
    def __init__(self):
        super().__init__()

    @property
    def parg_type(self):
        return ReportingParsedArgs

    def orchestrate(self, context: df.DurableOrchestrationContext):
        # get report type
        report_location = yield context.call_activity(
            "ReportConstructorActivity",
            serialize_pargs(self.pargs, self._d),
        )
        return report_location


main = df.Orchestrator.create(ReportRunnerOrchestrator.main)
