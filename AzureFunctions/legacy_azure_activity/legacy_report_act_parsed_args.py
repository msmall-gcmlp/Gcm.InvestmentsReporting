from ..legacy_azure_orchestrator.legacy_report_orch_parsed_args import (
    LegacyReportingOrchParsedArgs,
)


class LegacyReportingActivityParsedArgs(LegacyReportingOrchParsedArgs):
    def __init__(self):
        super().__init__()

    @classmethod
    def from_dict(cls, d: dict):
        pargs = super().from_dict(d)
        return pargs
