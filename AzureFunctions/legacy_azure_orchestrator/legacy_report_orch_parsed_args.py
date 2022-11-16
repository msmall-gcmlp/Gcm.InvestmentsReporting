from gcm.inv.entityhierarchy.az_func.entity_extract_activity_base import (
    EntityParsedArgs,
)
from .legacy_orchestrations import LegacyOrchestrations
import azure.functions as func


class LegacyReportingOrchParsedArgs(EntityParsedArgs):
    def __init__(self):
        super().__init__()

    @classmethod
    def from_http(cls, req: func.HttpRequest):
        d = req.params
        d["LegacyOrchestrations"] = req.route_params["functionName"]
        pargs = LegacyReportingOrchParsedArgs.from_dict(d)
        return pargs

    @classmethod
    def from_dict(cls, d: dict):
        pargs = super().from_dict(d)
        if "LegacyOrchestrations" in d:
            if d["LegacyOrchestrations"] in [
                LegacyOrchestrations(x).name
                for x in LegacyOrchestrations.list()
            ]:
                pargs.LegacyOrchestrations = LegacyOrchestrations[
                    d["LegacyOrchestrations"]
                ]
        else:
            raise NotImplementedError()
        return pargs
