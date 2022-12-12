import azure.durable_functions as df
from gcm.inv.utils.azure.durable_functions.base_orchestrator import (
    BaseOrchestrator,
)
from .legacy_report_orch_parsed_args import (
    LegacyReportingOrchParsedArgs,
    LegacyOrchestrations,
)
from .orchestrators.BaselReportOrchestrator import (
    orchestrator_function as basel_orchestrator_function,
)
from .orchestrators.BbaReportOrchestrator import (
    orchestrator_function as bba_orchestrator_function,
)
from .orchestrators.EofLiquidityStressReportOrchestrator import (
    orchestrator_function as eof_liq_stress_orchestrator_function,
)
from .orchestrators.EofRbaReportOrchestrator import (
    orchestrator_function as eof_rba_orchestrator_function,
)
from .orchestrators.HkmaMarketPerformanceReportOrchestrator import (
    orchestrator_function as hkma_mkt_orchestrator_function,
)
from .orchestrators.PerformanceScreenerReportOrchestrator import (
    orchestrator_function as perf_screener_orchestrator_function,
)
from .orchestrators.MarketPerformanceReportOrchestrator import (
    orchestrator_function as market_performance_orchestrator_function,
)
from .orchestrators.legacy_tasks import LegacyTasks
from gcm.inv.utils.azure.durable_functions.parg_serialization import (
    serialize_pargs,
)


class LegacyReportOrchestrator(BaseOrchestrator):
    def __init__(self):
        super().__init__()

    @property
    def parg_type(self):
        return LegacyReportingOrchParsedArgs

    def execute_legacy_tasks(
        self, tasks: LegacyTasks, context: df.DurableOrchestrationContext
    ):
        for i in tasks.activity_sets:
            activity_set_qeue = []
            for j in i.activity_params:
                client_inputs: dict = (
                    LegacyReportingOrchParsedArgs.parse_client_inputs(
                        context
                    )
                )
                client_inputs: dict = client_inputs | j.to_dict()
                activity_set_qeue.append(
                    context.call_activity(
                        "legacy_azure_activity",
                        serialize_pargs(self.pargs, client_inputs),
                    )
                )
            outputs = yield context.task_all(activity_set_qeue)
            assert outputs is not None
        return "Done"

    def orchestrate(self, context: df.DurableOrchestrationContext):
        legacy_orchestrator: LegacyOrchestrations = (
            self.pargs.LegacyOrchestrations
        )
        orchestrator_map = {
            LegacyOrchestrations.BaselReportOrchestrator: basel_orchestrator_function,
            LegacyOrchestrations.BbaReportOrchestrator: bba_orchestrator_function,
            LegacyOrchestrations.EofLiquidityStressReportOrchestrator: eof_liq_stress_orchestrator_function,
            LegacyOrchestrations.EofRbaReportOrchestrator: eof_rba_orchestrator_function,
            LegacyOrchestrations.MarketPerformanceReportOrchestrator: market_performance_orchestrator_function,
            LegacyOrchestrations.HkmaMarketPerformanceReportOrchestrator: hkma_mkt_orchestrator_function,
            LegacyOrchestrations.PerformanceScreenerReportOrchestrator: perf_screener_orchestrator_function,
        }
        d: LegacyTasks = orchestrator_map[legacy_orchestrator](context)
        assert d is not None
        return self.execute_legacy_tasks(d, context)


main = df.Orchestrator.create(LegacyReportOrchestrator.main)
