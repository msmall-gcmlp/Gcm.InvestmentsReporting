import azure.durable_functions as df
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
from .orchestrators.SingleNameEquityExposureOrchestrator import (
    orchestrator_function as singlename_equityexposure_orchestrator_function,
)
from gcm.inv.utils.azure.legacy_conversion.legacy_orchestration import (
    LegacyOrchestrator,
)


class LegacyReportOrchestrator(LegacyOrchestrator):
    def __init__(self):
        super().__init__()

    @property
    def orchestrator_map(self):
        return {
            LegacyOrchestrations.BaselReportOrchestrator: basel_orchestrator_function,
            LegacyOrchestrations.BbaReportOrchestrator: bba_orchestrator_function,
            LegacyOrchestrations.EofLiquidityStressReportOrchestrator: eof_liq_stress_orchestrator_function,
            LegacyOrchestrations.EofRbaReportOrchestrator: eof_rba_orchestrator_function,
            LegacyOrchestrations.MarketPerformanceReportOrchestrator: market_performance_orchestrator_function,
            LegacyOrchestrations.HkmaMarketPerformanceReportOrchestrator: hkma_mkt_orchestrator_function,
            LegacyOrchestrations.PerformanceScreenerReportOrchestrator: perf_screener_orchestrator_function,
            LegacyOrchestrations.SingleNameEquityExposureOrchestrator: singlename_equityexposure_orchestrator_function
        }

    @property
    def parg_type(self):
        return LegacyReportingOrchParsedArgs


main = df.Orchestrator.create(LegacyReportOrchestrator.main)
