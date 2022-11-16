import azure.durable_functions as df
from gcm.inv.utils.azure.durable_functions.base_orchestrator import (
    BaseOrchestrator,
)
from .legacy_report_orch_parsed_args import (
    LegacyReportingOrchParsedArgs,
    LegacyOrchestrations,
)
from .orchestrators.BaselReportOrchestrator import (
    orchestrator_function as base_orchestrator_function,
)


class LegacyReportOrchestrator(BaseOrchestrator):
    def __init__(self):
        super().__init__()

    @property
    def parg_type(self):
        return LegacyReportingOrchParsedArgs

    def orchestrate(self, context: df.DurableOrchestrationContext):
        d = {}
        legacy_orchestrator: LegacyOrchestrations = (
            self.pargs.LegacyOrchestrations
        )
        if (
            legacy_orchestrator
            == LegacyOrchestrations.BaselReportOrchestrator
        ):
            d = base_orchestrator_function(context=context)


main = df.Orchestrator.create(LegacyReportOrchestrator.main)
