from gcm.inv.utils.azure.legacy_conversion.legacy_pargs_base import (
    LegacyParsedArgs,
)
from .legacy_orchestrations import LegacyOrchestrations


class LegacyReportingOrchParsedArgs(LegacyParsedArgs):
    def __init__(self):
        super().__init__()

    PargType = LegacyParsedArgs.LegacyPargType.Legacy_Orchestrator
    EnumType = LegacyOrchestrations
